from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

import pytest

from crawler.services.scheduler import SchedulerService
from crawler.services.scheduler import ReconcileResult
from crawler.workers import scheduler_worker


FROZEN_NOW = datetime(2026, 8, 20, 12, 0, 0)
JOB_ID = UUID("b1bd0c23-6b57-4f9e-aac0-b6ae76960773")


@dataclass
class FakeRepositories:
    jobs: "FakeJobRepository"
    urls: "FakeUrlRepository"


class FakeJobRepository:
    def __init__(
        self,
        *,
        runnable_job_ids: list[UUID],
        lifecycle: SimpleNamespace,
        events: list[str],
    ) -> None:
        self._runnable_job_ids = runnable_job_ids
        self._lifecycle = lifecycle
        self._events = events

    async def advance_lifecycle_states(self, *, now: datetime) -> SimpleNamespace:
        assert now == FROZEN_NOW
        self._events.append("advance_lifecycle_states")
        return self._lifecycle

    async def list_jobs_with_runnable_work(self, *, now: datetime) -> list[UUID]:
        assert now == FROZEN_NOW
        self._events.append("list_jobs_with_runnable_work")
        return self._runnable_job_ids


class FakeUrlRepository:
    def __init__(self, *, released: int, events: list[str]) -> None:
        self._released = released
        self._events = events

    async def release_expired_leases(self, *, now: datetime) -> int:
        assert now == FROZEN_NOW
        self._events.append("release_expired_leases")
        return self._released


class FakePublisher:
    def __init__(self, events: list[str]) -> None:
        self.published: list[dict[str, str]] = []
        self._events = events

    async def publish_job_wakeup(self, job_id: UUID) -> None:
        self._events.append("publish_job_wakeup")
        self.published.append({"job_id": str(job_id)})


@pytest.fixture
def fake_repos() -> FakeRepositories:
    events: list[str] = []
    return FakeRepositories(
        jobs=FakeJobRepository(
            runnable_job_ids=[JOB_ID],
            lifecycle=SimpleNamespace(paused_jobs=0, canceled_jobs=0, completed_jobs=0),
            events=events,
        ),
        urls=FakeUrlRepository(released=0, events=events),
    )


@pytest.fixture
def fake_publisher(fake_repos: FakeRepositories) -> FakePublisher:
    return FakePublisher(fake_repos.jobs._events)


@pytest.mark.anyio
async def test_scheduler_republishes_runnable_work_when_queue_message_was_lost(
    fake_repos: FakeRepositories,
    fake_publisher: FakePublisher,
) -> None:
    await SchedulerService(
        fake_repos.jobs, fake_repos.urls, fake_publisher
    ).reconcile_once(now=FROZEN_NOW)

    assert fake_publisher.published == [{"job_id": str(JOB_ID)}]


@pytest.mark.anyio
async def test_scheduler_releases_expired_leases_before_requeue(
    fake_repos: FakeRepositories,
    fake_publisher: FakePublisher,
) -> None:
    fake_repos.urls._released = 1

    result = await SchedulerService(
        fake_repos.jobs, fake_repos.urls, fake_publisher
    ).reconcile_once(now=FROZEN_NOW)

    assert result.expired_leases_released == 1
    assert fake_repos.jobs._events == [
        "release_expired_leases",
        "advance_lifecycle_states",
        "list_jobs_with_runnable_work",
        "publish_job_wakeup",
    ]


@pytest.mark.anyio
async def test_scheduler_advances_pausing_canceling_and_completed_jobs(
    fake_repos: FakeRepositories,
    fake_publisher: FakePublisher,
) -> None:
    fake_repos.jobs._lifecycle = SimpleNamespace(
        paused_jobs=1,
        canceled_jobs=1,
        completed_jobs=1,
    )

    result = await SchedulerService(
        fake_repos.jobs, fake_repos.urls, fake_publisher
    ).reconcile_once(now=FROZEN_NOW)

    assert result.paused_jobs == 1
    assert result.canceled_jobs == 1
    assert result.completed_jobs == 1


@pytest.mark.anyio
async def test_scheduler_worker_reconcile_once_returns_reconciliation_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = ReconcileResult(
        expired_leases_released=1,
        republished_jobs=2,
        paused_jobs=3,
        canceled_jobs=4,
        completed_jobs=5,
    )
    engine = SimpleNamespace(disposed=False)

    async def dispose() -> None:
        engine.disposed = True

    engine.dispose = dispose

    class FakeSession:
        async def __aenter__(self) -> "FakeSession":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        def begin(self) -> "FakeSession":
            return self

    class FakeSessionFactory:
        kw = {"bind": engine}

        def __call__(self) -> FakeSession:
            return FakeSession()

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = False

        async def channel(self) -> object:
            return object()

        async def close(self) -> None:
            self.closed = True

    connection = FakeConnection()

    class FakeSchedulerService:
        def __init__(self, *args: object) -> None:
            pass

        async def reconcile_once(self, *, now: datetime) -> ReconcileResult:
            assert now == FROZEN_NOW
            return expected

    async def connect_robust(_: str) -> FakeConnection:
        return connection

    settings = SimpleNamespace(rabbitmq_url="amqp://test", lease_duration_seconds=60)
    monkeypatch.setattr(scheduler_worker, "Settings", lambda: settings)
    monkeypatch.setattr(scheduler_worker, "async_session_factory", lambda _: FakeSessionFactory())
    monkeypatch.setattr(scheduler_worker.aio_pika, "connect_robust", connect_robust)
    monkeypatch.setattr(scheduler_worker, "SchedulerService", FakeSchedulerService)

    result = await scheduler_worker.reconcile_once(FROZEN_NOW)

    assert result is expected
    assert connection.closed is True
    assert engine.disposed is True
