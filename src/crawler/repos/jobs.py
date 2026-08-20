from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import exists, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.domain.url_policy import normalize_url


ACTIVE_URL_STATUSES = (UrlStatus.CLAIMED, UrlStatus.FETCHING, UrlStatus.PROCESSING)
TERMINAL_URL_STATUSES = (
    UrlStatus.DONE,
    UrlStatus.FAILED_PERMANENT,
    UrlStatus.CANCELED,
    UrlStatus.SKIPPED_CHILD_SPAWNED,
)


@dataclass(frozen=True)
class LifecycleAdvanceResult:
    paused_jobs: int
    canceled_jobs: int
    completed_jobs: int


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_job(
        self,
        *,
        seed_url: str,
        config: dict[str, Any],
        parent_job_id: UUID | None = None,
    ) -> CrawlJob:
        normalized_seed_url = normalize_url(seed_url)
        seed_hostname = urlsplit(normalized_seed_url).hostname
        if not seed_hostname:
            raise ValueError("seed_url must include a hostname")

        job = CrawlJob(
            seed_url=normalized_seed_url,
            seed_hostname=seed_hostname,
            parent_job_id=parent_job_id,
            status=JobStatus.PENDING,
            config=config,
        )
        self._session.add(job)
        await self._session.flush()
        return job

    async def request_pause(self, job_id: UUID) -> None:
        await self._session.execute(
            update(CrawlJob)
            .where(CrawlJob.id == job_id, CrawlJob.status == JobStatus.RUNNING)
            .values(status=JobStatus.PAUSING, pause_requested_at=_utcnow())
        )
        await self._session.flush()

    async def request_resume(self, job_id: UUID) -> None:
        await self._session.execute(
            update(CrawlJob)
            .where(CrawlJob.id == job_id, CrawlJob.status == JobStatus.PAUSED)
            .values(status=JobStatus.RUNNING, pause_requested_at=None)
        )
        await self._session.flush()

    async def request_cancel(self, job_id: UUID, cascade_children: bool = True) -> None:
        job_ids = self._job_ids_to_cancel(job_id, cascade_children)
        now = _utcnow()

        await self._session.execute(
            update(CrawlJob)
            .where(
                CrawlJob.id.in_(job_ids),
                CrawlJob.status.in_(
                    (JobStatus.RUNNING, JobStatus.PAUSED, JobStatus.PAUSING)
                ),
            )
            .values(status=JobStatus.CANCELING, cancel_requested_at=now)
        )
        await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.job_id.in_(job_ids),
                CrawlUrl.status.not_in(ACTIVE_URL_STATUSES + TERMINAL_URL_STATUSES),
            )
            .values(
                status=UrlStatus.CANCELED,
                claimed_by=None,
                claimed_at=None,
                lease_expires_at=None,
                last_heartbeat_at=None,
            )
        )
        await self._session.flush()

    async def mark_completed_if_drained(self, job_id: UUID) -> bool:
        result = await self._session.execute(
            self._complete_drained_jobs_stmt(now=_utcnow()).where(CrawlJob.id == job_id)
        )
        await self._session.flush()
        return result.scalar_one_or_none() is not None

    async def advance_lifecycle_states(self, *, now: datetime) -> LifecycleAdvanceResult:
        active_urls_exist = self._active_urls_exist()
        paused = await self._session.execute(
            update(CrawlJob)
            .where(CrawlJob.status == JobStatus.PAUSING, ~active_urls_exist)
            .values(status=JobStatus.PAUSED)
            .returning(CrawlJob.id)
        )
        canceled = await self._session.execute(
            update(CrawlJob)
            .where(CrawlJob.status == JobStatus.CANCELING, ~active_urls_exist)
            .values(status=JobStatus.CANCELED, finished_at=now)
            .returning(CrawlJob.id)
        )
        completed = await self._session.execute(self._complete_drained_jobs_stmt(now=now))
        await self._session.flush()

        return LifecycleAdvanceResult(
            paused_jobs=len(paused.scalars().all()),
            canceled_jobs=len(canceled.scalars().all()),
            completed_jobs=len(completed.scalars().all()),
        )

    def _job_ids_to_cancel(self, job_id: UUID, cascade_children: bool):
        if not cascade_children:
            return select(CrawlJob.id).where(CrawlJob.id == job_id)

        job_tree = select(CrawlJob.id).where(CrawlJob.id == job_id).cte(recursive=True)
        child = aliased(CrawlJob)
        job_tree = job_tree.union_all(
            select(child.id).where(child.parent_job_id == job_tree.c.id)
        )
        return select(job_tree.c.id)

    @staticmethod
    def _active_urls_exist():
        return (
            exists(
                select(1).where(
                    CrawlUrl.job_id == CrawlJob.id,
                    CrawlUrl.status.in_(ACTIVE_URL_STATUSES),
                )
            )
            .correlate(CrawlJob)
        )

    @staticmethod
    def _complete_drained_jobs_stmt(*, now: datetime):
        unresolved_urls_exist = (
            exists(
                select(1).where(
                    CrawlUrl.job_id == CrawlJob.id,
                    CrawlUrl.status.not_in(TERMINAL_URL_STATUSES),
                )
            )
            .correlate(CrawlJob)
        )
        return (
            update(CrawlJob)
            .where(CrawlJob.status == JobStatus.RUNNING, ~unresolved_urls_exist)
            .values(status=JobStatus.COMPLETED, finished_at=now)
            .returning(CrawlJob.id)
        )
