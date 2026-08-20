from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler.config import Settings
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.db.session import async_session_factory
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


async def _create_running_job_with_seed(async_session: AsyncSession) -> CrawlUrl:
    jobs = JobRepository(async_session)
    urls = UrlRepository(async_session)
    hostname = f"task4-{uuid4().hex}.example.com"
    job = await jobs.create_job(
        seed_url=f"https://{hostname}",
        config={"max_attempts": 3, "child_rules": []},
    )
    job.status = JobStatus.RUNNING
    url = await urls.seed_url(job_id=job.id, seed_url=job.seed_url)
    await async_session.commit()
    return url


@pytest.mark.anyio
async def test_create_job_normalizes_seed_url_and_seed_url_is_idempotent(
    async_session: AsyncSession,
) -> None:
    jobs = JobRepository(async_session)
    urls = UrlRepository(async_session)

    job = await jobs.create_job(
        seed_url="HTTPS://Example.COM#ignored",
        config={"max_attempts": 3, "child_rules": []},
    )
    first = await urls.seed_url(job_id=job.id, seed_url=job.seed_url)
    second = await urls.seed_url(job_id=job.id, seed_url=job.seed_url)

    assert job.seed_url == "https://example.com/"
    assert job.seed_hostname == "example.com"
    assert first.id == second.id
    assert first.status is UrlStatus.QUEUED


@pytest.mark.anyio
async def test_create_job_uses_hostname_without_port(async_session: AsyncSession) -> None:
    job = await JobRepository(async_session).create_job(
        seed_url="https://example.com:8443/docs",
        config={"max_attempts": 3, "child_rules": []},
    )

    assert job.seed_hostname == "example.com"


@pytest.mark.anyio
async def test_claim_runnable_urls_allows_only_one_active_worker(
    async_session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _create_running_job_with_seed(async_session)

    async with session_factory() as worker_a_session:
        async with session_factory() as worker_b_session:
            worker_a_repo = UrlRepository(worker_a_session)
            worker_b_repo = UrlRepository(worker_b_session)
            first = await worker_a_repo.claim_runnable_urls(worker_id="worker-a", limit=1)
            second = await worker_b_repo.claim_runnable_urls(worker_id="worker-b", limit=1)

    assert len(first) == 1
    assert first[0].claimed_by == "worker-a"
    assert second == []


@pytest.mark.anyio
async def test_claim_runnable_urls_allows_workers_to_claim_distinct_urls_in_one_job(
    async_session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_url = await _create_running_job_with_seed(async_session)
    second_url = await UrlRepository(async_session).seed_url(
        job_id=first_url.job_id,
        seed_url=f"{first_url.normalized_url}second",
    )
    await async_session.commit()

    async with session_factory() as worker_a_session:
        async with session_factory() as worker_b_session:
            first = await UrlRepository(worker_a_session).claim_runnable_urls(
                worker_id="worker-a",
                limit=1,
            )
            second = await UrlRepository(worker_b_session).claim_runnable_urls(
                worker_id="worker-b",
                limit=1,
            )

    assert len(first) == 1
    assert len(second) == 1
    assert {first[0].id, second[0].id} == {first_url.id, second_url.id}


@pytest.mark.anyio
async def test_claim_runnable_urls_skips_not_yet_eligible_retry_work(
    async_session: AsyncSession,
) -> None:
    url = await _create_running_job_with_seed(async_session)
    url.status = UrlStatus.RETRY_WAIT
    url.next_eligible_at = datetime.now() + timedelta(hours=1)
    await async_session.commit()

    claimed = await UrlRepository(async_session).claim_runnable_urls(worker_id="worker-a", limit=1)

    assert claimed == []


@pytest.mark.anyio
async def test_release_expired_leases_requeues_abandoned_work(async_session: AsyncSession) -> None:
    url = await _create_running_job_with_seed(async_session)
    frozen_now = datetime(2026, 8, 20, 12, 0, 0)
    url.status = UrlStatus.CLAIMED
    url.claimed_by = "dead-worker"
    url.claimed_at = frozen_now - timedelta(minutes=2)
    url.last_heartbeat_at = frozen_now - timedelta(minutes=2)
    url.lease_expires_at = frozen_now - timedelta(seconds=1)
    await async_session.commit()

    released = await UrlRepository(async_session).release_expired_leases(now=frozen_now)
    await async_session.refresh(url)

    assert released == 1
    assert url.status is UrlStatus.QUEUED
    assert url.claimed_by is None
    assert url.lease_expires_at is None


@pytest.mark.anyio
async def test_heartbeat_extends_only_current_workers_lease(async_session: AsyncSession) -> None:
    url = await _create_running_job_with_seed(async_session)
    frozen_now = datetime(2026, 8, 20, 12, 0, 0)
    url.status = UrlStatus.CLAIMED
    url.claimed_by = "worker-a"
    url.lease_expires_at = frozen_now + timedelta(seconds=5)
    await async_session.commit()

    repo = UrlRepository(async_session)
    updated = await repo.heartbeat_url(
        url_id=url.id,
        worker_id="worker-a",
        lease_duration_seconds=30,
        now=frozen_now,
    )
    rejected = await repo.heartbeat_url(
        url_id=url.id,
        worker_id="worker-b",
        lease_duration_seconds=30,
        now=frozen_now + timedelta(seconds=1),
    )
    await async_session.refresh(url)

    assert updated is True
    assert rejected is False
    assert url.last_heartbeat_at == frozen_now
    assert url.lease_expires_at == frozen_now + timedelta(seconds=30)


@pytest.mark.anyio
async def test_mark_retry_wait_releases_the_claim_and_records_error(async_session: AsyncSession) -> None:
    url = await _create_running_job_with_seed(async_session)
    url.status = UrlStatus.CLAIMED
    url.claimed_by = "worker-a"
    next_eligible_at = datetime(2026, 8, 20, 12, 1, 0)
    await async_session.commit()

    updated = await UrlRepository(async_session).mark_retry_wait(
        url_id=url.id,
        worker_id="worker-a",
        next_eligible_at=next_eligible_at,
        error_code="rate_limited",
        error_detail="retry after",
    )
    await async_session.refresh(url)

    assert updated is True
    assert url.status is UrlStatus.RETRY_WAIT
    assert url.fetch_attempts == 1
    assert url.next_eligible_at == next_eligible_at
    assert url.claimed_by is None
    assert url.error_code == "rate_limited"
    assert url.error_detail == "retry after"


@pytest.mark.anyio
async def test_mark_retry_wait_rejects_a_stale_worker_after_reclaim(
    async_session: AsyncSession,
) -> None:
    url = await _create_running_job_with_seed(async_session)
    frozen_now = datetime(2026, 8, 20, 12, 0, 0)
    url.status = UrlStatus.CLAIMED
    url.claimed_by = "worker-a"
    url.lease_expires_at = frozen_now - timedelta(seconds=1)
    await async_session.commit()

    repo = UrlRepository(async_session)
    assert await repo.release_expired_leases(now=frozen_now) == 1
    reclaimed = await repo.claim_runnable_urls(worker_id="worker-b", limit=1)
    updated = await repo.mark_retry_wait(
        url_id=url.id,
        worker_id="worker-a",
        next_eligible_at=frozen_now + timedelta(minutes=1),
        error_code="stale_failure",
        error_detail="worker-a finished after its lease expired",
    )
    await async_session.refresh(url)

    assert len(reclaimed) == 1
    assert updated is False
    assert url.status is UrlStatus.CLAIMED
    assert url.claimed_by == "worker-b"
    assert url.fetch_attempts == 0


@pytest.mark.anyio
async def test_seed_url_persists_normalized_url_hash(async_session: AsyncSession) -> None:
    jobs = JobRepository(async_session)
    urls = UrlRepository(async_session)
    job = await jobs.create_job(
        seed_url="https://example.com",
        config={"max_attempts": 3, "child_rules": []},
    )

    seed = await urls.seed_url(job_id=job.id, seed_url="https://EXAMPLE.com/docs#section")
    saved = await async_session.scalar(select(CrawlUrl).where(CrawlUrl.id == seed.id))

    assert saved is not None
    assert saved.normalized_url == "https://example.com/docs"
    assert len(saved.url_hash) == 64
