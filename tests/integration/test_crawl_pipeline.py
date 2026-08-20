from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler.config import Settings
from crawler.db.models.artifacts import ContentArtifact
from crawler.db.models.attempts import CrawlAttempt
from crawler.db.models.discoveries import DiscoveredLink
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.metadata import ContentMetadata
from crawler.db.models.urls import CrawlUrl
from crawler.db.session import async_session_factory
from crawler.repos.discoveries import DiscoveryRepository
from crawler.repos.attempts import AttemptRepository
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository
from crawler.services.fetch_client import FetchClient
from crawler.services.orchestrator import CrawlOrchestrator
from crawler.services.processors.registry import ProcessorRegistry
from crawler.services.storage import ArtifactStorage


@dataclass
class CrawlTestContainer:
    async_session: AsyncSession
    http_client: httpx.AsyncClient
    orchestrator: CrawlOrchestrator
    job_repo: JobRepository
    url_repo: UrlRepository
    discovery_repo: DiscoveryRepository
    parent_job_id: UUID
    seed_url_id: UUID
    fetch_response: dict[str, Any]
    fetch_call_count: dict[str, int]
    after_fetch: list[Callable[[], Awaitable[None]]]


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
async def test_database_lock(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    engine = session_factory.kw["bind"]
    async with engine.connect() as connection:
        await connection.execute(text("SELECT pg_advisory_lock(2026082006)"))
        yield
        await connection.execute(text("SELECT pg_advisory_unlock(2026082006)"))


@pytest.fixture
async def async_session(
    session_factory: async_sessionmaker[AsyncSession],
    test_database_lock: None,
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        session.info["task6_job_ids"] = set()
        yield session
        await session.rollback()
        job_ids: set[UUID] = session.info["task6_job_ids"]
        if job_ids:
            await session.execute(delete(CrawlJob).where(CrawlJob.id.in_(job_ids)))
            await session.commit()


@pytest.fixture
async def app_container(
    async_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> AsyncIterator[CrawlTestContainer]:
    monkeypatch.chdir(tmp_path)
    jobs = JobRepository(async_session)
    urls = UrlRepository(async_session)
    discoveries = DiscoveryRepository(async_session)
    hostname = f"task6-{uuid4().hex}.example.com"
    job = await jobs.create_job(
        seed_url=f"https://{hostname}",
        config={"max_attempts": 3, "child_rules": []},
    )
    job.status = JobStatus.RUNNING
    seed = await urls.seed_url(job_id=job.id, seed_url=job.seed_url)
    seed.status = UrlStatus.CLAIMED
    seed.claimed_by = "worker-a"
    async_session.info["task6_job_ids"].add(job.id)
    await async_session.commit()

    fetch_response: dict[str, Any] = {
        "status_code": 200,
        "headers": {"Content-Type": "text/html"},
        "body": (
            "<html><head><title>Example title</title></head><body>"
            "<a href='/about'>About</a>"
            "<a href='/products/42'>Product</a>"
            "<a href='https://external.example.org/page'>External</a>"
            "</body></html>"
        ),
    }
    fetch_call_count = {"value": 0}
    after_fetch: list[Callable[[], Awaitable[None]]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        fetch_call_count["value"] += 1
        assert request.url.params == httpx.QueryParams({"url": seed.normalized_url})
        for action in after_fetch:
            await action()
        return httpx.Response(200, json=fetch_response)

    async with httpx.AsyncClient(
        base_url="http://mock-api.mock.com",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        yield CrawlTestContainer(
            async_session=async_session,
            http_client=http_client,
            orchestrator=CrawlOrchestrator(
                fetch_client=FetchClient(http_client),
                storage=ArtifactStorage(session=async_session),
                processors=ProcessorRegistry(),
                jobs=jobs,
                urls=urls,
                discoveries=discoveries,
                attempts=AttemptRepository(async_session),
            ),
            job_repo=jobs,
            url_repo=urls,
            discovery_repo=discoveries,
            parent_job_id=job.id,
            seed_url_id=seed.id,
            fetch_response=fetch_response,
            fetch_call_count=fetch_call_count,
            after_fetch=after_fetch,
        )


@pytest.mark.anyio
async def test_process_html_url_persists_artifact_metadata_and_discoveries(
    app_container: CrawlTestContainer,
) -> None:
    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    saved = await app_container.async_session.scalar(
        select(ContentArtifact).where(ContentArtifact.crawl_url_id == app_container.seed_url_id)
    )
    assert saved is not None
    metadata = await app_container.async_session.scalar(
        select(ContentMetadata).where(ContentMetadata.artifact_id == saved.id)
    )

    assert saved.content_type == "text/html"
    assert saved.storage_path.startswith("output/html/")
    assert metadata is not None
    assert metadata.metadata_json["title"] == "Example title"
    attempts = list(
        (
            await app_container.async_session.scalars(
                select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
            )
        ).all()
    )
    assert len(attempts) == 1
    assert attempts[0].result_status == "success"
    assert attempts[0].http_status_code == 200
    assert attempts[0].finished_at is not None
    assert await app_container.url_repo.exists_in_job(
        app_container.parent_job_id,
        f"https://{(await app_container.async_session.get(CrawlJob, app_container.parent_job_id)).seed_hostname}/about",
    )


@pytest.mark.anyio
async def test_external_links_are_recorded_but_not_enqueued(
    app_container: CrawlTestContainer,
) -> None:
    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    discovery = await app_container.discovery_repo.get_by_target(
        "https://external.example.org/page"
    )

    assert discovery is not None
    assert discovery.is_same_hostname is False
    assert await app_container.url_repo.exists_in_job(
        app_container.parent_job_id,
        "https://external.example.org/page",
    ) is False


@pytest.mark.anyio
async def test_not_found_response_marks_url_failed_permanent(
    app_container: CrawlTestContainer,
) -> None:
    app_container.fetch_response.update(
        {"status_code": 404, "headers": {"Content-Type": "text/html"}, "body": ""}
    )

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.FAILED_PERMANENT
    assert crawl_url.http_status_code == 404
    assert crawl_url.error_code == "not_found"
    assert crawl_url.claimed_by is None
    attempt = await app_container.async_session.scalar(
        select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
    )
    assert attempt is not None
    assert attempt.result_status == UrlStatus.FAILED_PERMANENT.value
    assert attempt.http_status_code == 404
    assert attempt.finished_at is not None


@pytest.mark.anyio
async def test_blocked_response_marks_url_failed_permanent(
    app_container: CrawlTestContainer,
) -> None:
    app_container.fetch_response.update(
        {"status_code": 403, "headers": {"Content-Type": "text/html"}, "body": ""}
    )

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.FAILED_PERMANENT
    assert crawl_url.http_status_code == 403
    assert crawl_url.error_code == "blocked"


@pytest.mark.anyio
async def test_rate_limited_response_enters_retry_wait(
    app_container: CrawlTestContainer,
) -> None:
    parent_job = await app_container.async_session.get(CrawlJob, app_container.parent_job_id)
    assert parent_job is not None
    parent_job.config = {
        "max_attempts": 3,
        "base_backoff_seconds": 5,
        "max_backoff_seconds": 60,
        "respect_retry_after": True,
        "child_rules": [],
    }
    app_container.fetch_response.update(
        {
            "status_code": 429,
            "headers": {"Content-Type": "text/html", "Retry-After": "45"},
            "body": "",
        }
    )
    await app_container.async_session.commit()

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.RETRY_WAIT
    assert crawl_url.http_status_code == 429
    assert crawl_url.error_code == "rate_limited"
    assert crawl_url.next_eligible_at is not None
    assert crawl_url.claimed_by is None
    attempt = await app_container.async_session.scalar(
        select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
    )
    assert attempt is not None
    assert attempt.result_status == UrlStatus.RETRY_WAIT.value
    assert attempt.http_status_code == 429
    assert attempt.retry_after_seconds == 45
    assert attempt.response_headers == {
        "Content-Type": "text/html",
        "Retry-After": "45",
    }


@pytest.mark.anyio
async def test_server_error_exhausting_retry_limit_marks_url_failed_permanent(
    app_container: CrawlTestContainer,
) -> None:
    parent_job = await app_container.async_session.get(CrawlJob, app_container.parent_job_id)
    assert parent_job is not None
    parent_job.config = {
        "max_attempts": 1,
        "base_backoff_seconds": 5,
        "max_backoff_seconds": 60,
        "child_rules": [],
    }
    app_container.fetch_response.update(
        {"status_code": 500, "headers": {"Content-Type": "text/html"}, "body": ""}
    )
    await app_container.async_session.commit()

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.FAILED_PERMANENT
    assert crawl_url.http_status_code == 500
    assert crawl_url.error_code == "retry_exhausted"


@pytest.mark.anyio
async def test_processing_error_enters_retry_wait(
    app_container: CrawlTestContainer,
) -> None:
    app_container.fetch_response.update(
        {
            "status_code": 200,
            "headers": {"Content-Type": "application/json"},
            "body": "{}",
        }
    )

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.RETRY_WAIT
    assert crawl_url.http_status_code == 200
    assert crawl_url.error_code == "processing_error"
    assert crawl_url.claimed_by is None
    attempt = await app_container.async_session.scalar(
        select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
    )
    assert attempt is not None
    assert attempt.result_status == UrlStatus.RETRY_WAIT.value
    assert attempt.error_detail is not None


@pytest.mark.anyio
async def test_fetch_exception_persists_retry_attempt(
    app_container: CrawlTestContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def raise_connect_error(*args, **kwargs) -> httpx.Response:
        raise httpx.ConnectError("fetch unavailable")

    monkeypatch.setattr(app_container.http_client, "get", raise_connect_error)

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    attempt = await app_container.async_session.scalar(
        select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
    )
    assert attempt is not None
    assert attempt.result_status == UrlStatus.RETRY_WAIT.value
    assert attempt.http_status_code is None
    assert attempt.error_detail == "fetch unavailable"


@pytest.mark.anyio
@pytest.mark.parametrize("job_status", [JobStatus.PAUSED, JobStatus.CANCELED])
async def test_non_running_job_does_not_process_claimed_url(
    app_container: CrawlTestContainer,
    job_status: JobStatus,
) -> None:
    parent_job = await app_container.async_session.get(CrawlJob, app_container.parent_job_id)
    assert parent_job is not None
    parent_job.status = job_status
    await app_container.async_session.commit()

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    artifact = await app_container.async_session.scalar(
        select(ContentArtifact).where(ContentArtifact.crawl_url_id == app_container.seed_url_id)
    )
    attempt = await app_container.async_session.scalar(
        select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
    )

    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.CLAIMED
    assert app_container.fetch_call_count["value"] == 0
    assert artifact is None
    assert attempt is None


@pytest.mark.anyio
async def test_stopped_job_after_fetch_does_not_persist_or_discover(
    app_container: CrawlTestContainer,
) -> None:
    async def pause_job_after_fetch() -> None:
        parent_job = await app_container.async_session.get(
            CrawlJob, app_container.parent_job_id
        )
        assert parent_job is not None
        parent_job.status = JobStatus.PAUSED
        await app_container.async_session.flush()

    app_container.after_fetch.append(pause_job_after_fetch)

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    crawl_url = await app_container.async_session.get(CrawlUrl, app_container.seed_url_id)
    artifact = await app_container.async_session.scalar(
        select(ContentArtifact).where(ContentArtifact.crawl_url_id == app_container.seed_url_id)
    )
    attempt = await app_container.async_session.scalar(
        select(CrawlAttempt).where(CrawlAttempt.crawl_url_id == app_container.seed_url_id)
    )
    metadata = await app_container.async_session.scalar(select(ContentMetadata))
    discoveries = list((await app_container.async_session.scalars(select(DiscoveredLink))).all())

    assert crawl_url is not None
    assert crawl_url.status is UrlStatus.FETCHING
    assert artifact is None
    assert attempt is not None
    assert attempt.result_status == "in_progress"
    assert attempt.finished_at is None
    assert metadata is None
    assert discoveries == []
