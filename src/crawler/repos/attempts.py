from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.models.attempts import CrawlAttempt


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class AttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def start_attempt(self, *, crawl_url_id: UUID, attempt_number: int) -> CrawlAttempt:
        attempt = CrawlAttempt(
            crawl_url_id=crawl_url_id,
            attempt_number=attempt_number,
            started_at=_utcnow(),
            result_status="in_progress",
            http_status_code=None,
            retry_after_seconds=None,
            response_headers=None,
            error_detail=None,
        )
        self._session.add(attempt)
        await self._session.flush()
        return attempt

    async def finish_attempt(
        self,
        *,
        attempt: CrawlAttempt,
        result_status: str,
        http_status_code: int | None,
        retry_after_seconds: int | None,
        response_headers: dict[str, Any] | None,
        error_detail: str | None,
    ) -> None:
        attempt.finished_at = _utcnow()
        attempt.result_status = result_status
        attempt.http_status_code = http_status_code
        attempt.retry_after_seconds = retry_after_seconds
        attempt.response_headers = response_headers
        attempt.error_detail = error_detail
        await self._session.flush()
