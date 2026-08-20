from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.models.attempts import CrawlAttempt


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class AttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def start_attempt(self, *, crawl_url_id: UUID) -> CrawlAttempt:
        attempt_number = (await self._next_attempt_number(crawl_url_id=crawl_url_id)) + 1
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
        attempt_id: UUID,
        result_status: str,
        http_status_code: int | None,
        retry_after_seconds: int | None,
        response_headers: dict[str, Any] | None,
        error_detail: str | None,
    ) -> None:
        await self._session.execute(
            update(CrawlAttempt)
            .where(CrawlAttempt.id == attempt_id)
            .values(
                finished_at=_utcnow(),
                result_status=result_status,
                http_status_code=http_status_code,
                retry_after_seconds=retry_after_seconds,
                response_headers=response_headers,
                error_detail=error_detail,
            )
        )
        await self._session.flush()

    async def mark_abandoned_attempts(
        self,
        *,
        crawl_url_ids: list[UUID],
        now: datetime,
    ) -> int:
        if not crawl_url_ids:
            return 0
        result = await self._session.execute(
            update(CrawlAttempt)
            .where(
                CrawlAttempt.crawl_url_id.in_(crawl_url_ids),
                CrawlAttempt.finished_at.is_(None),
            )
            .values(
                finished_at=now,
                result_status="abandoned",
                error_detail="lease expired",
            )
            .returning(CrawlAttempt.id)
        )
        await self._session.flush()
        return len(result.scalars().all())

    async def _next_attempt_number(self, *, crawl_url_id: UUID) -> int:
        next_attempt_number = await self._session.scalar(
            select(func.coalesce(func.max(CrawlAttempt.attempt_number), 0)).where(
                CrawlAttempt.crawl_url_id == crawl_url_id
            )
        )
        return int(next_attempt_number or 0)
