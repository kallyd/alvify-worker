"""Test pause/cancel polling in worker."""
import asyncio
import sys
import os

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

# Ensure worker directory is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestCheckJobStatus:
    """Tests for check_job_status function."""

    @pytest.mark.asyncio
    async def test_running_returns_running(self):
        from main import check_job_status

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"status": "running"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        result = await check_job_status(
            mock_session, "job-1", "http://api", "key", "w1"
        )
        assert result == "running"

    @pytest.mark.asyncio
    async def test_paused_returns_paused(self):
        from main import check_job_status

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"status": "paused"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        result = await check_job_status(
            mock_session, "job-1", "http://api", "key", "w1"
        )
        assert result == "paused"

    @pytest.mark.asyncio
    async def test_cancelled_returns_cancelled(self):
        from main import check_job_status

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"status": "cancelled"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        result = await check_job_status(
            mock_session, "job-1", "http://api", "key", "w1"
        )
        assert result == "cancelled"

    @pytest.mark.asyncio
    async def test_network_error_returns_running(self):
        """On network error, assume running (fail-open)."""
        from main import check_job_status

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=Exception("connection refused"))

        result = await check_job_status(
            mock_session, "job-1", "http://api", "key", "w1"
        )
        assert result == "running"

    @pytest.mark.asyncio
    async def test_non_200_returns_running(self):
        """On non-200 response, assume running (fail-open)."""
        from main import check_job_status

        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        result = await check_job_status(
            mock_session, "job-1", "http://api", "key", "w1"
        )
        assert result == "running"

    @pytest.mark.asyncio
    async def test_calls_correct_url(self):
        """Verify the function calls the correct endpoint."""
        from main import check_job_status

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"status": "running"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        await check_job_status(
            mock_session, "job-123", "http://localhost:8080/api", "mykey", "worker-1"
        )

        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert call_args[0][0] == "http://localhost:8080/api/internal/workers/jobs/job-123/status"
        assert call_args[1]["headers"]["Authorization"] == "Bearer mykey"
        assert call_args[1]["headers"]["X-Worker-ID"] == "worker-1"

    @pytest.mark.asyncio
    async def test_missing_status_field_returns_running(self):
        """If API returns JSON without 'status' field, default to running."""
        from main import check_job_status

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        result = await check_job_status(
            mock_session, "job-1", "http://api", "key", "w1"
        )
        assert result == "running"
