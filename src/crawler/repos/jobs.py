from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import exists, or_, select, text, update
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

    async def get(self, job_id: UUID) -> CrawlJob | None:
        return await self._session.get(CrawlJob, job_id)

    async def is_running(self, job_id: UUID) -> bool:
        return (
            await self._session.scalar(
                select(CrawlJob.id).where(
                    CrawlJob.id == job_id,
                    CrawlJob.status == JobStatus.RUNNING,
                )
            )
        ) is not None

    async def lock_running_job(self, job_id: UUID) -> CrawlJob | None:
        return await self._session.scalar(
            select(CrawlJob)
            .where(
                CrawlJob.id == job_id,
                CrawlJob.status == JobStatus.RUNNING,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )

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

    async def get_or_create_child_job(
        self,
        *,
        parent_job_id: UUID,
        seed_url: str,
        seed_hostname: str,
        inherited_config: dict[str, Any],
    ) -> UUID:
        normalized_seed_url = normalize_url(seed_url)
        if urlsplit(normalized_seed_url).hostname != seed_hostname:
            raise ValueError("child seed URL must use the parent seed hostname")

        child_job_id = await self._find_child_job_id(parent_job_id, normalized_seed_url)
        if child_job_id is not None:
            return child_job_id

        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": f"child-job:{parent_job_id}:{normalized_seed_url}"},
        )
        child_job_id = await self._find_child_job_id(parent_job_id, normalized_seed_url)
        if child_job_id is not None:
            return child_job_id

        child = await self.create_job(
            seed_url=normalized_seed_url,
            config=deepcopy(inherited_config),
            parent_job_id=parent_job_id,
        )
        child.status = JobStatus.RUNNING
        child.started_at = _utcnow()
        await self._session.flush()
        return child.id

    async def find_child_job(self, parent_job_id: UUID, seed_url: str) -> UUID | None:
        return await self._find_child_job_id(parent_job_id, normalize_url(seed_url))

    async def _find_child_job_id(
        self, parent_job_id: UUID, normalized_seed_url: str
    ) -> UUID | None:
        return await self._session.scalar(
            select(CrawlJob.id).where(
                CrawlJob.parent_job_id == parent_job_id,
                CrawlJob.seed_url == normalized_seed_url,
            )
        )

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
        canceling_job_ids = select(CrawlJob.id).where(CrawlJob.status == JobStatus.CANCELING)
        await self._session.execute(
            update(CrawlUrl)
            .where(
                CrawlUrl.job_id.in_(canceling_job_ids),
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
        active_urls_exist = self._active_urls_exist()
        unresolved_urls_exist = self._unresolved_urls_exist()
        paused = await self._session.execute(
            update(CrawlJob)
            .where(CrawlJob.status == JobStatus.PAUSING, ~active_urls_exist)
            .values(status=JobStatus.PAUSED)
            .returning(CrawlJob.id)
        )
        canceled = await self._session.execute(
            update(CrawlJob)
            .where(CrawlJob.status == JobStatus.CANCELING, ~unresolved_urls_exist)
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

    async def list_jobs_with_runnable_work(self, *, now: datetime) -> list[UUID]:
        statement = (
            select(CrawlUrl.job_id)
            .join(CrawlJob)
            .where(CrawlJob.status == JobStatus.RUNNING)
            .where(CrawlUrl.status.in_((UrlStatus.QUEUED, UrlStatus.RETRY_WAIT)))
            .where(
                or_(
                    CrawlUrl.next_eligible_at.is_(None),
                    CrawlUrl.next_eligible_at <= now,
                )
            )
            .distinct()
            .order_by(CrawlUrl.job_id)
        )
        return list((await self._session.scalars(statement)).all())

    async def list_resumable_jobs(self, *, now: datetime) -> list[UUID]:
        return await self.list_jobs_with_runnable_work(now=now)

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
        unresolved_urls_exist = JobRepository._unresolved_urls_exist()
        return (
            update(CrawlJob)
            .where(CrawlJob.status == JobStatus.RUNNING, ~unresolved_urls_exist)
            .values(status=JobStatus.COMPLETED, finished_at=now)
            .returning(CrawlJob.id)
        )

    @staticmethod
    def _unresolved_urls_exist():
        return (
            exists(
                select(1).where(
                    CrawlUrl.job_id == CrawlJob.id,
                    CrawlUrl.status.not_in(TERMINAL_URL_STATUSES),
                )
            )
            .correlate(CrawlJob)
        )
