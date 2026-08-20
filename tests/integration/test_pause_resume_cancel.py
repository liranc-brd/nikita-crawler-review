from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler.config import Settings
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.session import async_session_factory
from crawler.repos.discoveries import DiscoveryRepository
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    factory = async_session_factory(Settings())
    engine = factory.kw["bind"]
    yield factory
    await engine.dispose()


@pytest.fixture
async def async_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session
        await session.rollback()
        await session.execute(
            delete(CrawlJob).where(CrawlJob.seed_hostname.like("task4-%.example.com"))
        )
        await session.commit()


async def _create_running_job(async_session: AsyncSession):
    jobs = JobRepository(async_session)
    hostname = f"task4-{uuid4().hex}.example.com"
    job = await jobs.create_job(
        seed_url=f"https://{hostname}",
        config={"max_attempts": 3, "child_rules": []},
    )
    job.status = JobStatus.RUNNING
    await async_session.commit()
    return job


@pytest.mark.anyio
async def test_request_pause_advances_a_drained_job_to_paused(async_session: AsyncSession) -> None:
    job = await _create_running_job(async_session)
    repo = JobRepository(async_session)
    frozen_now = datetime(2026, 8, 20, 12, 0, 0)

    await repo.request_pause(job.id)
    result = await repo.advance_lifecycle_states(now=frozen_now)
    await async_session.refresh(job)

    assert job.status is JobStatus.PAUSED
    assert job.pause_requested_at is not None
    assert result.paused_jobs == 1


@pytest.mark.anyio
async def test_request_resume_returns_paused_job_to_running(async_session: AsyncSession) -> None:
    job = await _create_running_job(async_session)
    job.status = JobStatus.PAUSED
    job.pause_requested_at = datetime(2026, 8, 20, 11, 0, 0)
    await async_session.commit()

    await JobRepository(async_session).request_resume(job.id)
    await async_session.refresh(job)

    assert job.status is JobStatus.RUNNING
    assert job.pause_requested_at is None


@pytest.mark.anyio
async def test_request_cancel_cascades_to_children_and_cancels_runnable_urls(
    async_session: AsyncSession,
) -> None:
    jobs = JobRepository(async_session)
    urls = UrlRepository(async_session)
    parent = await _create_running_job(async_session)
    child = await jobs.create_job(
        seed_url=f"{parent.seed_url}child",
        config={"max_attempts": 3, "child_rules": []},
        parent_job_id=parent.id,
    )
    child.status = JobStatus.RUNNING
    queued = await urls.seed_url(job_id=parent.id, seed_url=parent.seed_url)
    await async_session.commit()

    await jobs.request_cancel(parent.id)
    result = await jobs.advance_lifecycle_states(now=datetime(2026, 8, 20, 12, 0, 1))
    await async_session.refresh(parent)
    await async_session.refresh(child)
    await async_session.refresh(queued)

    assert parent.status is JobStatus.CANCELED
    assert child.status is JobStatus.CANCELED
    assert queued.status is UrlStatus.CANCELED
    assert result.canceled_jobs == 2


@pytest.mark.anyio
async def test_cancel_finalizes_a_url_recovered_after_cancel_request(
    async_session: AsyncSession,
) -> None:
    job = await _create_running_job(async_session)
    url = await UrlRepository(async_session).seed_url(job_id=job.id, seed_url=job.seed_url)
    frozen_now = datetime(2026, 8, 20, 12, 0, 0)
    url.status = UrlStatus.CLAIMED
    url.claimed_by = "dead-worker"
    url.lease_expires_at = frozen_now - timedelta(seconds=1)
    await async_session.commit()

    jobs = JobRepository(async_session)
    await jobs.request_cancel(job.id)
    assert await UrlRepository(async_session).release_expired_leases(now=frozen_now) == 1
    result = await jobs.advance_lifecycle_states(now=frozen_now)
    await async_session.refresh(job)
    await async_session.refresh(url)

    assert result.canceled_jobs == 1
    assert job.status is JobStatus.CANCELED
    assert url.status is UrlStatus.CANCELED


@pytest.mark.anyio
async def test_mark_completed_if_drained_waits_for_runnable_urls(async_session: AsyncSession) -> None:
    job = await _create_running_job(async_session)
    url = await UrlRepository(async_session).seed_url(job_id=job.id, seed_url=job.seed_url)
    await async_session.commit()
    repo = JobRepository(async_session)

    blocked = await repo.mark_completed_if_drained(job.id)
    url.status = UrlStatus.DONE
    await async_session.flush()
    completed = await repo.mark_completed_if_drained(job.id)
    await async_session.refresh(job)

    assert blocked is False
    assert completed is True
    assert job.status is JobStatus.COMPLETED
    assert job.finished_at is not None


@pytest.mark.anyio
async def test_record_discovery_persists_external_links_without_target_url(
    async_session: AsyncSession,
) -> None:
    job = await _create_running_job(async_session)
    source = await UrlRepository(async_session).seed_url(job_id=job.id, seed_url=job.seed_url)
    await async_session.commit()

    discovery = await DiscoveryRepository(async_session).record_discovery(
        job_id=job.id,
        source_url_id=source.id,
        target_url="https://external.example.org/page#section",
        is_same_hostname=False,
    )

    assert discovery.target_normalized_url == "https://external.example.org/page"
    assert discovery.target_url_id is None
    assert discovery.is_same_hostname is False
