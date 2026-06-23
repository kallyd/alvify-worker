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


# ── WebSocket implementation (push-based dispatch) ────────────────────────────

class WebSocketQueueClient:
    """
    Receives jobs via WebSocket push from backend.

    Instead of HTTP long-polling, this client maintains a persistent
    WebSocket connection to /internal/workers/ws. The backend pushes
    jobs instantly when they are enqueued.

    Falls back gracefully: if the WS connection drops, poll() returns None
    so the caller can switch to ApiPollQueueClient.
    """

    def __init__(self, ws_url: str, api_key: str, worker_id: str) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        self._worker_id = worker_id
        self._queue: asyncio.Queue = asyncio.Queue()
        self._ws = None
        self._connected = False
        self._receiver_task: Optional[asyncio.Task] = None

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> bool:
        """
        Establish WS connection and start receiver loop.
        Returns True on success, False on failure.
        """
        try:
            import websockets
            import urllib.parse

            # Build URL with auth query params
            sep = "&" if "?" in self._ws_url else "?"
            url = (
                f"{self._ws_url}{sep}"
                f"token={urllib.parse.quote(self._api_key)}"
                f"&worker_id={urllib.parse.quote(self._worker_id)}"
            )

            self._ws = await websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            )

            # Send ready message
            import json
            await self._ws.send(json.dumps({
                "type": "ready",
                "worker_id": self._worker_id,
            }))

            self._connected = True
            self._receiver_task = asyncio.create_task(self._receive_loop())
            logger.info("WebSocketQueueClient connected to %s", self._ws_url)
            return True

        except Exception as exc:
            logger.warning("WebSocketQueueClient connect failed: %s", exc)
            self._connected = False
            return False

    async def _receive_loop(self) -> None:
        """Background task: receive messages from WS and route them."""
        import json

        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                msg_type = msg.get("type")

                if msg_type == "job":
                    payload = msg.get("payload")
                    if payload:
                        await self._queue.put(payload)
                        # Send ack immediately
                        job_id = payload.get("job_id", "")
                        await self._ws.send(json.dumps({
                            "type": "ack",
                            "job_id": job_id,
                        }))
                elif msg_type == "ping":
                    # Respond to application-level ping
                    await self._ws.send(json.dumps({"type": "pong"}))

        except Exception as exc:
            logger.warning("WebSocketQueueClient receiver stopped: %s", exc)
        finally:
            self._connected = False

    async def poll(self, session: aiohttp.ClientSession = None) -> Optional[dict]:
        """
        Compatible with QueueClient protocol. Blocks until job received.

        Returns None if not connected (signals caller to fallback).
        """
        if not self._connected:
            return None

        try:
            # Wait up to POLL_TIMEOUT for a job from the WS stream
            job = await asyncio.wait_for(
                self._queue.get(), timeout=_POLL_TIMEOUT_S
            )
            return job
        except asyncio.TimeoutError:
            return None
        except Exception as exc:
            logger.warning("WebSocketQueueClient poll error: %s", exc)
            return None

    async def ack(self, job_id: str, session: aiohttp.ClientSession = None) -> None:
        # Ack is sent immediately in _receive_loop when job arrives.
        pass

    async def close(self) -> None:
        """Close WS connection and cancel receiver task."""
        self._connected = False
        if self._receiver_task and not self._receiver_task.done():
            self._receiver_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        logger.info("WebSocketQueueClient closed")


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

def _derive_ws_url(poll_url: str) -> str:
    """
    Derive the WebSocket dispatch URL from the existing API poll URL.

    API_BASE_URL typically looks like: http://host:port/internal/workers/jobs/poll
    WS URL should be: ws://host:port/internal/workers/ws
    """
    # Check for explicit WS_DISPATCH_URL first
    explicit = os.environ.get("WS_DISPATCH_URL")
    if explicit:
        return explicit

    # Derive from poll_url: strip /jobs/poll, replace scheme
    base = poll_url
    if base.endswith("/jobs/poll"):
        base = base[: -len("/jobs/poll")]
    elif base.endswith("/poll"):
        base = base[: -len("/poll")]

    base = base.replace("https://", "wss://").replace("http://", "ws://")
    return f"{base}/ws"


def make_queue_client(poll_url: str, headers_factory) -> QueueClient:
    """
    Return the appropriate QueueClient based on QUEUE_BACKEND env var.

    QUEUE_BACKEND=api      (default) — current API long-poll
    QUEUE_BACKEND=ws       — WebSocket push-based dispatch
    QUEUE_BACKEND=auto     — Try WS first, return WS client (caller handles fallback)
    QUEUE_BACKEND=streams  (future)  — Redis Streams consumer groups
    """
    backend = os.environ.get("QUEUE_BACKEND", "api").lower()

    if backend in ("ws", "auto"):
        ws_url = _derive_ws_url(poll_url)
        # Extract API key and worker ID from headers factory
        headers = headers_factory()
        api_key = headers.get("Authorization", "").replace("Bearer ", "")
        worker_id = headers.get("X-Worker-ID", "")
        return WebSocketQueueClient(ws_url, api_key, worker_id)

    if backend == "streams":
        logger.warning("QUEUE_BACKEND=streams is not yet implemented — falling back to api")

    return ApiPollQueueClient(poll_url, headers_factory)


# ── needed for ApiPollQueueClient ─────────────────────────────────────────────
import asyncio  # noqa: E402  (must be after class definitions)
