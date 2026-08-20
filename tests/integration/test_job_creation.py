from collections.abc import AsyncIterator

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.config import Settings
from crawler.db.models.artifacts import ContentArtifact
from crawler.db.models.discoveries import DiscoveredLink
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.db.session import async_session_factory


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def async_session(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncSession]:
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler"
    )
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    session_factory = async_session_factory(Settings())
    engine = session_factory.kw["bind"]

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.anyio
async def test_job_schema_persists_seed_job(async_session) -> None:
    job = CrawlJob(
        seed_url="https://example.com",
        seed_hostname="example.com",
        status=JobStatus.PENDING,
        config={"max_attempts": 3, "child_rules": []},
    )
    async_session.add(job)
    await async_session.commit()
    await async_session.refresh(job)

    assert job.id is not None
    assert job.seed_hostname == "example.com"

    await async_session.delete(job)
    await async_session.commit()


async def _create_job(async_session: AsyncSession, hostname: str) -> CrawlJob:
    job = CrawlJob(
        seed_url=f"https://{hostname}",
        seed_hostname=hostname,
        status=JobStatus.PENDING,
        config={"max_attempts": 3, "child_rules": []},
    )
    async_session.add(job)
    await async_session.flush()
    return job


async def _create_url(async_session: AsyncSession, job: CrawlJob) -> CrawlUrl:
    crawl_url = CrawlUrl(
        job_id=job.id,
        normalized_url=job.seed_url,
        url_hash=job.seed_hostname,
        status=UrlStatus.DISCOVERED,
    )
    async_session.add(crawl_url)
    await async_session.flush()
    return crawl_url


@pytest.mark.anyio
async def test_content_artifact_requires_matching_crawl_url_job(async_session: AsyncSession) -> None:
    first_job = await _create_job(async_session, "first.example.com")
    second_job = await _create_job(async_session, "second.example.com")
    second_url = await _create_url(async_session, second_job)
    await async_session.commit()

    artifact = ContentArtifact(
        job_id=first_job.id,
        crawl_url_id=second_url.id,
        content_type="text/html",
        storage_path="output/html/page.html",
        filename="page.html",
        content_hash="content-hash",
    )
    async_session.add(artifact)

    try:
        with pytest.raises(IntegrityError):
            await async_session.commit()
    finally:
        await async_session.rollback()
        await async_session.delete(first_job)
        await async_session.delete(second_job)
        await async_session.commit()


@pytest.mark.anyio
async def test_discovered_link_requires_matching_source_url_job(async_session: AsyncSession) -> None:
    first_job = await _create_job(async_session, "first.example.com")
    second_job = await _create_job(async_session, "second.example.com")
    second_url = await _create_url(async_session, second_job)
    await async_session.commit()

    discovered_link = DiscoveredLink(
        job_id=first_job.id,
        source_url_id=second_url.id,
        target_normalized_url="https://second.example.com/target",
        is_same_hostname=False,
    )
    async_session.add(discovered_link)

    try:
        with pytest.raises(IntegrityError):
            await async_session.commit()
    finally:
        await async_session.rollback()
        await async_session.delete(first_job)
        await async_session.delete(second_job)
        await async_session.commit()


@pytest.mark.anyio
async def test_discovered_link_requires_matching_target_url_job(async_session: AsyncSession) -> None:
    first_job = await _create_job(async_session, "first.example.com")
    second_job = await _create_job(async_session, "second.example.com")
    first_url = await _create_url(async_session, first_job)
    second_url = await _create_url(async_session, second_job)
    await async_session.commit()

    discovered_link = DiscoveredLink(
        job_id=first_job.id,
        source_url_id=first_url.id,
        target_normalized_url="https://second.example.com/target",
        target_url_id=second_url.id,
        is_same_hostname=False,
    )
    async_session.add(discovered_link)

    try:
        with pytest.raises(IntegrityError):
            await async_session.commit()
    finally:
        await async_session.rollback()
        await async_session.delete(first_job)
        await async_session.delete(second_job)
        await async_session.commit()
