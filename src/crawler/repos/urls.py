from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.domain.url_policy import normalize_url


ACTIVE_URL_STATUSES = (UrlStatus.CLAIMED, UrlStatus.FETCHING, UrlStatus.PROCESSING)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class UrlRepository:
    def __init__(self, session: AsyncSession, *, lease_duration_seconds: int = 60) -> None:
        self._session = session
        self._lease_duration_seconds = lease_duration_seconds

    async def seed_url(
        self,
        *,
        job_id: UUID,
        seed_url: str,
        discovered_from_url_id: UUID | None = None,
    ) -> CrawlUrl:
        normalized_url = normalize_url(seed_url)
        stmt = (
            insert(CrawlUrl)
            .values(
                job_id=job_id,
                normalized_url=normalized_url,
                url_hash=hashlib.sha256(normalized_url.encode()).hexdigest(),
                discovered_from_url_id=discovered_from_url_id,
                status=UrlStatus.QUEUED,
            )
            .on_conflict_do_nothing(
                index_elements=(CrawlUrl.job_id, CrawlUrl.normalized_url)
            )
            .returning(CrawlUrl.id)
        )
        url_id = (await self._session.execute(stmt)).scalar_one_or_none()
        if url_id is None:
            url_id = await self._session.scalar(
                select(CrawlUrl.id).where(
                    CrawlUrl.job_id == job_id,
                    CrawlUrl.normalized_url == normalized_url,
                )
            )
        if url_id is None:
            raise RuntimeError("failed to create or retrieve crawl URL")
        url = await self._session.get(CrawlUrl, url_id)
        if url is None:
            raise RuntimeError("created crawl URL could not be loaded")
        return url

    async def claim_runnable_urls(self, *, worker_id: str, limit: int) -> list[CrawlUrl]:
        if limit <= 0:
            return []

        now = _utcnow()
        stmt = (
            select(CrawlUrl)
            .join(CrawlJob)
            .where(CrawlJob.status == JobStatus.RUNNING)
            .where(CrawlUrl.status.in_((UrlStatus.QUEUED, UrlStatus.RETRY_WAIT)))
            .where(
                or_(
                    CrawlUrl.next_eligible_at.is_(None),
                    CrawlUrl.next_eligible_at <= now,
                )
            )
            .order_by(CrawlUrl.next_eligible_at, CrawlUrl.id)
            .with_for_update(of=CrawlUrl, skip_locked=True)
            .limit(limit)
        )
        rows = list((await self._session.execute(stmt)).scalars().all())
        for row in rows:
            row.status = UrlStatus.CLAIMED
            row.claimed_by = worker_id
            row.claimed_at = now
            row.last_heartbeat_at = now
            row.lease_expires_at = now + timedelta(seconds=self._lease_duration_seconds)
        await self._session.flush()
        return rows

    async def heartbeat_url(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        lease_duration_seconds: int,
        now: datetime,
    ) -> bool:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status.in_(ACTIVE_URL_STATUSES),
            )
            .values(
                last_heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=lease_duration_seconds),
            )
            .returning(CrawlUrl.id)
        )
        await self._session.flush()
        return result.scalar_one_or_none() is not None

    async def get_claimed_url(self, *, url_id: UUID, worker_id: str) -> CrawlUrl | None:
        return await self._session.scalar(
            select(CrawlUrl).where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status == UrlStatus.CLAIMED,
            )
        )

    async def mark_fetching(self, *, url_id: UUID, worker_id: str) -> CrawlUrl | None:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status == UrlStatus.CLAIMED,
                CrawlUrl.job_id.in_(
                    select(CrawlJob.id).where(CrawlJob.status == JobStatus.RUNNING)
                ),
            )
            .values(status=UrlStatus.FETCHING, started_at=_utcnow())
            .returning(CrawlUrl)
        )
        await self._session.flush()
        return result.scalar_one_or_none()

    async def mark_processing(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        content_type: str,
        http_status_code: int,
    ) -> bool:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status == UrlStatus.FETCHING,
                CrawlUrl.job_id.in_(
                    select(CrawlJob.id).where(CrawlJob.status == JobStatus.RUNNING)
                ),
            )
            .values(
                status=UrlStatus.PROCESSING,
                content_type=content_type,
                http_status_code=http_status_code,
            )
            .returning(CrawlUrl.id)
        )
        await self._session.flush()
        return result.scalar_one_or_none() is not None

    async def mark_done(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        content_artifact_id: UUID,
    ) -> bool:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status == UrlStatus.PROCESSING,
                CrawlUrl.job_id.in_(
                    select(CrawlJob.id).where(CrawlJob.status == JobStatus.RUNNING)
                ),
            )
            .values(
                status=UrlStatus.DONE,
                content_artifact_id=content_artifact_id,
                fetch_attempts=CrawlUrl.fetch_attempts + 1,
                finished_at=_utcnow(),
                claimed_by=None,
                claimed_at=None,
                lease_expires_at=None,
                last_heartbeat_at=None,
                error_code=None,
                error_detail=None,
            )
            .returning(CrawlUrl.id)
        )
        await self._session.flush()
        return result.scalar_one_or_none() is not None

    async def exists_in_job(self, job_id: UUID, url: str) -> bool:
        normalized_url = normalize_url(url)
        return (
            await self._session.scalar(
                select(CrawlUrl.id).where(
                    CrawlUrl.job_id == job_id,
                    CrawlUrl.normalized_url == normalized_url,
                )
            )
        ) is not None

    async def release_expired_leases(self, *, now: datetime) -> int:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.status.in_(ACTIVE_URL_STATUSES),
                CrawlUrl.lease_expires_at.is_not(None),
                CrawlUrl.lease_expires_at <= now,
            )
            .values(
                status=UrlStatus.QUEUED,
                next_eligible_at=now,
                claimed_by=None,
                claimed_at=None,
                lease_expires_at=None,
                last_heartbeat_at=None,
            )
            .returning(CrawlUrl.id)
        )
        await self._session.flush()
        return len(result.scalars().all())

    async def mark_retry_wait(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        next_eligible_at: datetime,
        error_code: str,
        error_detail: str,
        http_status_code: int | None = None,
    ) -> bool:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status.in_(ACTIVE_URL_STATUSES),
            )
            .values(
                status=UrlStatus.RETRY_WAIT,
                fetch_attempts=CrawlUrl.fetch_attempts + 1,
                next_eligible_at=next_eligible_at,
                http_status_code=http_status_code,
                claimed_by=None,
                claimed_at=None,
                lease_expires_at=None,
                last_heartbeat_at=None,
                error_code=error_code,
                error_detail=error_detail,
            )
            .returning(CrawlUrl.id)
        )
        await self._session.flush()
        return result.scalar_one_or_none() is not None

    async def mark_failed_permanent(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        error_code: str,
        error_detail: str,
        http_status_code: int | None,
    ) -> bool:
        result = await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.id == url_id,
                CrawlUrl.claimed_by == worker_id,
                CrawlUrl.status.in_(ACTIVE_URL_STATUSES),
            )
            .values(
                status=UrlStatus.FAILED_PERMANENT,
                fetch_attempts=CrawlUrl.fetch_attempts + 1,
                http_status_code=http_status_code,
                finished_at=_utcnow(),
                claimed_by=None,
                claimed_at=None,
                lease_expires_at=None,
                last_heartbeat_at=None,
                error_code=error_code,
                error_detail=error_detail,
            )
            .returning(CrawlUrl.id)
        )
        await self._session.flush()
        return result.scalar_one_or_none() is not None
