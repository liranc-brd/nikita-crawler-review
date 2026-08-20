from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from crawler.repos.jobs import LifecycleAdvanceResult


class JobSchedulerRepository(Protocol):
    async def advance_lifecycle_states(self, *, now: datetime) -> LifecycleAdvanceResult: ...

    async def list_jobs_with_runnable_work(self, *, now: datetime) -> list[UUID]: ...


class UrlSchedulerRepository(Protocol):
    async def release_expired_leases(self, *, now: datetime) -> int: ...


class JobWakeupPublisher(Protocol):
    async def publish_job_wakeup(self, job_id: UUID) -> None: ...


@dataclass(frozen=True)
class ReconcileResult:
    expired_leases_released: int
    republished_jobs: int
    paused_jobs: int
    canceled_jobs: int
    completed_jobs: int


class SchedulerService:
    def __init__(
        self,
        jobs: JobSchedulerRepository,
        urls: UrlSchedulerRepository,
        publisher: JobWakeupPublisher,
    ) -> None:
        self._jobs = jobs
        self._urls = urls
        self._publisher = publisher

    async def reconcile_once(self, *, now: datetime) -> ReconcileResult:
        expired = await self._urls.release_expired_leases(now=now)
        lifecycle = await self._jobs.advance_lifecycle_states(now=now)
        runnable_job_ids = await self._jobs.list_jobs_with_runnable_work(now=now)
        for job_id in runnable_job_ids:
            await self._publisher.publish_job_wakeup(job_id)
        return ReconcileResult(
            expired_leases_released=expired,
            republished_jobs=len(runnable_job_ids),
            paused_jobs=lifecycle.paused_jobs,
            canceled_jobs=lifecycle.canceled_jobs,
            completed_jobs=lifecycle.completed_jobs,
        )
