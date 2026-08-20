from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.config import Settings
from crawler.db.models.attempts import CrawlAttempt
from crawler.db.models.artifacts import ContentArtifact
from crawler.db.models.discoveries import DiscoveredLink
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.db.session import async_session_factory
from crawler.main import create_app
from crawler.repos.discoveries import DiscoveryRepository
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def async_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app()),
        base_url="http://testserver",
    ) as client:
        yield client


async def _delete_job(job_id: UUID) -> None:
    session_factory = async_session_factory(Settings())
    engine = session_factory.kw["bind"]
    try:
        async with session_factory() as session:
            job = await session.get(CrawlJob, job_id)
            if job is not None:
                await session.delete(job)
                await session.commit()
    finally:
        await engine.dispose()


@pytest.fixture
async def async_session() -> AsyncIterator[AsyncSession]:
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


@pytest.mark.anyio
async def test_post_crawls_creates_seed_job_and_seed_url(
    async_client: httpx.AsyncClient,
    async_session: AsyncSession,
) -> None:
    response = await async_client.post(
        "/crawls",
        json={"seed_url": "https://example.com", "child_rules": []},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["status"] == "running"
    assert payload["started_at"] is not None

    job_id = UUID(payload["id"])
    try:
        job = await async_session.get(CrawlJob, job_id)
        seed_url = await async_session.scalar(select(CrawlUrl).where(CrawlUrl.job_id == job_id))

        assert job is not None
        assert job.status is JobStatus.RUNNING
        assert job.started_at is not None
        assert seed_url is not None
        assert seed_url.status is UrlStatus.QUEUED
    finally:
        await _delete_job(job_id)


@pytest.mark.anyio
async def test_crawl_inspection_endpoints_project_persisted_state(
    async_client: httpx.AsyncClient,
    async_session: AsyncSession,
) -> None:
    create_response = await async_client.post(
        "/crawls",
        json={"seed_url": "https://example.com", "child_rules": []},
    )
    assert create_response.status_code == 201
    job_id = UUID(create_response.json()["id"])
    child_job_id: UUID | None = None

    try:
        seed_url = await async_session.scalar(
            select(CrawlUrl).where(CrawlUrl.job_id == job_id)
        )
        assert seed_url is not None
        second_url = await UrlRepository(async_session).seed_url(
            job_id=job_id,
            seed_url="https://example.com/docs",
        )
        child = await JobRepository(async_session).create_job(
            seed_url="https://example.com/child",
            config={"max_attempts": 3, "child_rules": []},
            parent_job_id=job_id,
        )
        child_job_id = child.id
        async_session.add(
            CrawlAttempt(
                crawl_url_id=seed_url.id,
                attempt_number=1,
                started_at=datetime.now(UTC).replace(tzinfo=None),
                result_status="success",
                http_status_code=200,
            )
        )
        await DiscoveryRepository(async_session).record_discovery(
            job_id=job_id,
            source_url_id=seed_url.id,
            target_url=second_url.normalized_url,
            target_url_id=second_url.id,
            is_same_hostname=True,
        )
        await async_session.commit()

        status_response = await async_client.get(f"/crawls/{job_id}")
        urls_response = await async_client.get(f"/crawls/{job_id}/urls")
        attempts_response = await async_client.get(f"/crawls/{job_id}/attempts")
        children_response = await async_client.get(f"/crawls/{job_id}/children")
        parent_response = await async_client.get(f"/crawls/{child_job_id}/parent")
        discoveries_response = await async_client.get(f"/crawls/{job_id}/discoveries")

        assert status_response.status_code == 200
        assert status_response.json()["progress"] == {
            "total_discovered": 2,
            "runnable": 2,
            "in_progress": 0,
            "retry_waiting": 0,
            "done": 0,
            "permanently_failed": 0,
            "canceled": 0,
            "child_jobs_spawned": 1,
        }
        assert urls_response.status_code == 200
        assert {row["id"] for row in urls_response.json()} == {
            str(seed_url.id),
            str(second_url.id),
        }
        assert attempts_response.status_code == 200
        assert attempts_response.json()[0]["crawl_url_id"] == str(seed_url.id)
        assert children_response.status_code == 200
        assert children_response.json()[0]["id"] == str(child_job_id)
        assert parent_response.status_code == 200
        assert parent_response.json()["id"] == str(job_id)
        assert discoveries_response.status_code == 200
        assert discoveries_response.json()[0]["target_url_id"] == str(second_url.id)
    finally:
        if child_job_id is not None:
            child = await async_session.get(CrawlJob, child_job_id)
            if child is not None:
                await async_session.delete(child)
        job = await async_session.get(CrawlJob, job_id)
        if job is not None:
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
