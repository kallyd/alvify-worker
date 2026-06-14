"""
Queue abstraction layer — decouples the poll mechanism from main.py.

Today the worker polls for jobs by calling:
    GET /internal/workers/jobs/poll
which internally does a BLPOP on a Redis list (server-side blocking).

This module wraps that call behind a QueueClient interface so the
polling strategy can be swapped (e.g. to Redis Streams with consumer
groups) without touching main.py.

Migration path to Redis Streams
--------------------------------
1. Implement a RedisStreamsQueueClient that reads from a Stream instead
   of BLPOP.
2. Flip the factory in make_queue_client() based on an env var.
3. Deploy — no changes needed in main.py.

Interface contract:
    client.poll(session)  -> Optional[dict]
        Blocks up to POLL_TIMEOUT seconds.
        Returns a job dict on success, None if no job available.
        Never raises; errors are logged and return None.

    client.ack(job_id)  -> None (no-op today; needed for Streams)
"""
from __future__ import annotations

import logging
import os
from typing import Optional, Protocol

import aiohttp

logger = logging.getLogger(__name__)

_POLL_TIMEOUT_S: int = int(os.environ.get("POLL_TIMEOUT", "30"))


# ── Interface (Protocol) ──────────────────────────────────────────────────────

class QueueClient(Protocol):
    """Minimal interface all queue implementations must satisfy."""

    async def poll(self, session: aiohttp.ClientSession) -> Optional[dict]:
        """Block up to POLL_TIMEOUT seconds; return job dict or None."""
        ...

    async def ack(self, job_id: str, session: aiohttp.ClientSession) -> None:
        """Acknowledge successful processing (no-op for current backend)."""
        ...


# ── API-poll implementation (current / default) ───────────────────────────────

class ApiPollQueueClient:
    """
    Polls the Alvify API endpoint for the next available job.

    The server performs a Redis BLPOP internally and returns 204 if
    no job becomes available within the timeout.  This keeps the
    HTTP connection open for up to POLL_TIMEOUT seconds, avoiding
    busy-wait on the worker side.
    """

    def __init__(
        self,
        poll_url: str,
        headers_factory,  # callable() -> dict[str, str]
    ) -> None:
        self._poll_url = poll_url
        self._headers_factory = headers_factory

    async def poll(self, session: aiohttp.ClientSession) -> Optional[dict]:
        """Return the next job dict, or None if nothing is available."""
        try:
            async with session.get(
                self._poll_url,
                headers=self._headers_factory(),
                timeout=aiohttp.ClientTimeout(total=_POLL_TIMEOUT_S + 5),
            ) as resp:
                if resp.status == 204:
                    return None  # No job available before timeout
                if resp.status in (502, 503, 504):
                    # API is restarting or unavailable — backoff to avoid log spam
                    logger.warning(
                        "QueueClient poll returned HTTP %d — backing off 5s", resp.status
                    )
                    await asyncio.sleep(5)
                    return None
                if resp.status != 200:
                    logger.warning(
                        "QueueClient poll returned HTTP %d", resp.status
                    )
                    return None
                job = await resp.json()
                if not job or not job.get("job_id"):
                    return None
                return job
        except asyncio.TimeoutError:
            # Expected for long-poll: the server held the connection open.
            return None
        except (aiohttp.ClientConnectorError, ConnectionRefusedError, OSError):
            # API is down — backoff
            logger.warning("QueueClient poll connection failed — backing off 5s")
            await asyncio.sleep(5)
            return None
        except Exception as exc:
            logger.warning("QueueClient poll error: %s", exc)
            return None

    async def ack(self, job_id: str, session: aiohttp.ClientSession) -> None:
        # No-op: the API marks the job as "running" when it dequeues it.
        pass


# ── Future: Redis Streams implementation (stub) ───────────────────────────────

class RedisStreamsQueueClient:
    """
    Placeholder for future Redis Streams implementation.

    When activated (set QUEUE_BACKEND=streams env var), this client will:
      - XREADGROUP with consumer groups for at-least-once delivery
      - XACK after successful processing (ack() method is meaningful)
      - Automatic retries for unacknowledged messages
      - Dead-letter stream for permanently failed jobs

    Implementation note: fill in methods below when migrating.
    """

    async def poll(self, session: aiohttp.ClientSession) -> Optional[dict]:
        raise NotImplementedError(
            "RedisStreamsQueueClient is not yet implemented. "
            "Set QUEUE_BACKEND=api (default) to use the current implementation."
        )

    async def ack(self, job_id: str, session: aiohttp.ClientSession) -> None:
        raise NotImplementedError


# ── Factory ───────────────────────────────────────────────────────────────────

def make_queue_client(poll_url: str, headers_factory) -> QueueClient:
    """
    Return the appropriate QueueClient based on QUEUE_BACKEND env var.

    QUEUE_BACKEND=api      (default) — current API long-poll
    QUEUE_BACKEND=streams  (future)  — Redis Streams consumer groups
    """
    backend = os.environ.get("QUEUE_BACKEND", "api").lower()
    if backend == "streams":
        logger.warning("QUEUE_BACKEND=streams is not yet implemented — falling back to api")
    return ApiPollQueueClient(poll_url, headers_factory)


# ── needed for ApiPollQueueClient ─────────────────────────────────────────────
import asyncio  # noqa: E402  (must be after class definitions)
