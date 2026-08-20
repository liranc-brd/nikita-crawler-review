# Production Site Crawler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade crawler service that accepts crawl jobs, persists crawl state in PostgreSQL, uses RabbitMQ only as a wake-up mechanism, processes supported content types, and exposes operator controls and inspection APIs.

**Architecture:** The system is a FastAPI control plane plus two worker roles: a crawler worker and a scheduler/control worker. PostgreSQL is the source of truth for jobs, URLs, attempts, discoveries, artifacts, and metadata; RabbitMQ is advisory delivery only, so all correctness-critical state transitions happen transactionally in the database.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2.x, Alembic, asyncpg, aio-pika, httpx, BeautifulSoup4, Pillow, pypdf, pymediainfo, pytest, pytest-asyncio, testcontainers-python

**Spec:** `docs/superpowers/specs/2026-08-20-production-site-crawler-design.md`

## Global Constraints

- Accept a seed URL and crawl content reachable from it within the allowed boundary
- Persist durable crawl state so jobs can be resumed after process or machine failure
- Guarantee each normalized URL has at most one active worker at a time per crawl job, even under concurrency
- Download, process, and persist HTML, images, videos, and PDFs
- Expose API endpoints for lifecycle control and inspection
- Keep content-type processing extensible so adding a new type does not require rewriting the crawler core
- All content retrieval goes through the external fetch API: `GET http://mock-api.mock.com/fetch?url=<encoded_url>`
- The crawler must use response headers deliberately rather than inferring content type from URL structure
- The crawler must persist raw downloaded content into `output/html/`, `output/images/`, `output/videos/`, and `output/pdfs/`
- The default crawl boundary is strict to the exact seed hostname
- External hostnames must never be enqueued or followed, but they must still be recorded in `discovered_links` for audit
- Child jobs are created only for configured path prefixes or regex-like patterns
- If a discovered URL spawns a child job, the parent job should not also process that URL locally
- PostgreSQL is the source of truth for crawl state and RabbitMQ only as a delivery and wake-up mechanism
- The scheduler must also treat PostgreSQL as the recovery source if RabbitMQ delivery fails

---

## Planned File Structure

**Project and runtime**

- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `docker-compose.yml`
- Create: `README.md`

**Application package**

- Create: `src/crawler/__init__.py`
- Create: `src/crawler/config.py`
- Create: `src/crawler/logging.py`
- Create: `src/crawler/main.py`
- Create: `src/crawler/db/base.py`
- Create: `src/crawler/db/session.py`
- Create: `src/crawler/db/models/enums.py`
- Create: `src/crawler/db/models/jobs.py`
- Create: `src/crawler/db/models/urls.py`
- Create: `src/crawler/db/models/attempts.py`
- Create: `src/crawler/db/models/discoveries.py`
- Create: `src/crawler/db/models/artifacts.py`
- Create: `src/crawler/db/models/metadata.py`
- Create: `src/crawler/domain/types.py`
- Create: `src/crawler/domain/url_policy.py`
- Create: `src/crawler/domain/retry_policy.py`
- Create: `src/crawler/repos/jobs.py`
- Create: `src/crawler/repos/urls.py`
- Create: `src/crawler/repos/discoveries.py`
- Create: `src/crawler/services/fetch_client.py`
- Create: `src/crawler/services/storage.py`
- Create: `src/crawler/services/processors/base.py`
- Create: `src/crawler/services/processors/html.py`
- Create: `src/crawler/services/processors/images.py`
- Create: `src/crawler/services/processors/videos.py`
- Create: `src/crawler/services/processors/pdfs.py`
- Create: `src/crawler/services/processors/registry.py`
- Create: `src/crawler/services/orchestrator.py`
- Create: `src/crawler/services/scheduler.py`
- Create: `src/crawler/messaging/rabbitmq.py`
- Create: `src/crawler/api/schemas.py`
- Create: `src/crawler/api/dependencies.py`
- Create: `src/crawler/api/routes/crawls.py`
- Create: `src/crawler/workers/crawler_worker.py`
- Create: `src/crawler/workers/scheduler_worker.py`

**Migrations**

- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/20260820_0001_create_crawler_schema.py`

**Tests**

- Create: `tests/conftest.py`
- Create: `tests/unit/test_url_policy.py`
- Create: `tests/unit/test_retry_policy.py`
- Create: `tests/unit/test_processors.py`
- Create: `tests/unit/test_scheduler.py`
- Create: `tests/integration/test_job_creation.py`
- Create: `tests/integration/test_claiming_and_leases.py`
- Create: `tests/integration/test_crawl_pipeline.py`
- Create: `tests/integration/test_pause_resume_cancel.py`
- Create: `tests/integration/test_child_jobs.py`
- Create: `tests/integration/test_rabbitmq_recovery.py`

## Task 1: Bootstrap The Project Runtime

**Files:**
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `docker-compose.yml`
- Create: `README.md`
- Create: `src/crawler/__init__.py`
- Create: `src/crawler/config.py`
- Create: `src/crawler/logging.py`
- Create: `src/crawler/main.py`
- Test: `tests/conftest.py`

**Interfaces:**
- Consumes: none
- Produces: `class Settings(BaseSettings)`, `def create_app() -> FastAPI`, `def configure_logging() -> None`

- [ ] **Step 1: Write the failing test**

```python
from crawler.config import Settings
from crawler.main import create_app


def test_create_app_exposes_health_route() -> None:
    app = create_app()
    routes = {route.path for route in app.routes}
    assert "/health" in routes


def test_settings_reads_required_environment_variables(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    settings = Settings()
    assert str(settings.database_url).startswith("postgresql+asyncpg://")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/conftest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'crawler'`

- [ ] **Step 3: Write minimal implementation**

```python
from fastapi import FastAPI
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    rabbitmq_url: str


def create_app() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/conftest.py -v`
Expected: PASS

- [ ] **Step 5: Refactor runtime wiring and add base project files**

```toml
[project]
name = "crawler"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "fastapi",
  "uvicorn[standard]",
  "pydantic>=2.8",
  "pydantic-settings",
  "sqlalchemy>=2.0",
  "asyncpg",
  "alembic",
  "aio-pika",
  "httpx",
  "beautifulsoup4",
  "pillow",
  "pypdf",
  "pymediainfo",
]
```

- [ ] **Step 6: Run focused tests and config validation**

Run: `venv/bin/pytest tests/conftest.py -v`
Expected: PASS

Run: `venv/bin/python -c "from crawler.main import create_app; print(create_app().title)"`
Expected: prints the FastAPI app title without import errors

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .env.example docker-compose.yml README.md src/crawler/__init__.py src/crawler/config.py src/crawler/logging.py src/crawler/main.py tests/conftest.py
git commit -m "chore: bootstrap crawler service runtime"
```

## Task 2: Create The Database Schema And Migration

**Files:**
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/20260820_0001_create_crawler_schema.py`
- Create: `src/crawler/db/base.py`
- Create: `src/crawler/db/session.py`
- Create: `src/crawler/db/models/enums.py`
- Create: `src/crawler/db/models/jobs.py`
- Create: `src/crawler/db/models/urls.py`
- Create: `src/crawler/db/models/attempts.py`
- Create: `src/crawler/db/models/discoveries.py`
- Create: `src/crawler/db/models/artifacts.py`
- Create: `src/crawler/db/models/metadata.py`
- Test: `tests/integration/test_job_creation.py`

**Interfaces:**
- Consumes: `class Settings(BaseSettings)`
- Produces: `Base`, `async_session_factory`, ORM models `CrawlJob`, `CrawlUrl`, `CrawlAttempt`, `DiscoveredLink`, `ContentArtifact`, `ContentMetadata`, enums `JobStatus`, `UrlStatus`

- [ ] **Step 1: Write the failing integration test**

```python
async def test_job_schema_persists_seed_job(async_session) -> None:
    from crawler.db.models.jobs import CrawlJob, JobStatus

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/integration/test_job_creation.py::test_job_schema_persists_seed_job -v`
Expected: FAIL with missing DB session, models, or tables

- [ ] **Step 3: Write minimal schema and migration**

```python
class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    CANCELING = "canceling"
    CANCELED = "canceled"
    COMPLETED = "completed"
    FAILED = "failed"


class CrawlJob(Base):
    __tablename__ = "crawl_jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    seed_url: Mapped[str]
    seed_hostname: Mapped[str]
    status: Mapped[JobStatus]
    config: Mapped[dict[str, Any]]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/integration/test_job_creation.py::test_job_schema_persists_seed_job -v`
Expected: PASS

- [ ] **Step 5: Expand the schema to match the spec**

```python
class CrawlUrl(Base):
    __tablename__ = "crawl_urls"
    __table_args__ = (UniqueConstraint("job_id", "normalized_url", name="uq_crawl_urls_job_id_normalized_url"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    normalized_url: Mapped[str]
    url_hash: Mapped[str]
    status: Mapped[UrlStatus]
    next_eligible_at: Mapped[datetime | None]
    claimed_by: Mapped[str | None]
    claimed_at: Mapped[datetime | None]
    lease_expires_at: Mapped[datetime | None]
    last_heartbeat_at: Mapped[datetime | None]
```

- [ ] **Step 6: Add artifact and metadata persistence tables**

```python
class ContentArtifact(Base):
    __tablename__ = "content_artifacts"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    crawl_url_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_urls.id", ondelete="CASCADE"), unique=True)
    content_type: Mapped[str]
    storage_path: Mapped[str]
    content_hash: Mapped[str]


class ContentMetadata(Base):
    __tablename__ = "content_metadata"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    artifact_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("content_artifacts.id", ondelete="CASCADE"))
    metadata_type: Mapped[str]
    metadata_json: Mapped[dict[str, Any]]
```

- [ ] **Step 7: Run migrations and broader DB tests**

Run: `venv/bin/alembic upgrade head`
Expected: schema applies successfully against the configured PostgreSQL database

Run: `venv/bin/pytest tests/integration/test_job_creation.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add alembic.ini alembic src/crawler/db tests/integration/test_job_creation.py
git commit -m "feat: add crawler database schema and migration"
```

## Task 3: Implement URL Normalization, Hostname Policy, And Child-Rule Matching

**Files:**
- Create: `src/crawler/domain/types.py`
- Create: `src/crawler/domain/url_policy.py`
- Create: `src/crawler/domain/retry_policy.py`
- Test: `tests/unit/test_url_policy.py`
- Test: `tests/unit/test_retry_policy.py`

**Interfaces:**
- Consumes: `JobStatus`, `UrlStatus`
- Produces: `def normalize_url(raw_url: str, base_url: str | None = None) -> str`, `def is_same_hostname(seed_hostname: str, candidate_url: str) -> bool`, `def should_spawn_child(url: str, child_rules: list[dict[str, str]]) -> bool`, `def compute_next_retry_at(...) -> datetime`

- [ ] **Step 1: Write the failing unit tests**

```python
def test_normalize_url_resolves_relative_links() -> None:
    assert normalize_url("/docs?page=1", "https://example.com/base") == "https://example.com/docs?page=1"


def test_is_same_hostname_is_strict() -> None:
    assert is_same_hostname("example.com", "https://example.com/about") is True
    assert is_same_hostname("example.com", "https://www.example.com/about") is False


def test_should_spawn_child_matches_prefix_rule() -> None:
    rules = [{"kind": "path_prefix", "value": "/products"}]
    assert should_spawn_child("https://example.com/products/42", rules) is True
    assert should_spawn_child("https://example.com/blog/42", rules) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/unit/test_url_policy.py tests/unit/test_retry_policy.py -v`
Expected: FAIL with missing functions or wrong normalization behavior

- [ ] **Step 3: Write minimal implementation**

```python
def normalize_url(raw_url: str, base_url: str | None = None) -> str:
    resolved = urllib.parse.urljoin(base_url, raw_url) if base_url else raw_url
    parsed = urllib.parse.urlsplit(resolved)
    return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "/", parsed.query, ""))


def is_same_hostname(seed_hostname: str, candidate_url: str) -> bool:
    return urllib.parse.urlsplit(candidate_url).hostname == seed_hostname
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/unit/test_url_policy.py tests/unit/test_retry_policy.py -v`
Expected: PASS

- [ ] **Step 5: Refine edge-case handling and retry math**

```python
def compute_next_retry_at(*, now: datetime, attempt_number: int, base_backoff_seconds: int, max_backoff_seconds: int, retry_after_seconds: int | None) -> datetime:
    if retry_after_seconds is not None:
        return now + timedelta(seconds=retry_after_seconds)
    backoff = min(max_backoff_seconds, base_backoff_seconds * (2 ** max(0, attempt_number - 1)))
    return now + timedelta(seconds=backoff)
```

- [ ] **Step 6: Run focused and broader tests**

Run: `venv/bin/pytest tests/unit/test_url_policy.py tests/unit/test_retry_policy.py -v`
Expected: PASS

Run: `venv/bin/pytest tests/integration/test_job_creation.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/crawler/domain tests/unit/test_url_policy.py tests/unit/test_retry_policy.py
git commit -m "feat: add URL policy and retry policy utilities"
```

## Task 4: Build Repository And Transactional Crawl-State Operations

**Files:**
- Create: `src/crawler/repos/jobs.py`
- Create: `src/crawler/repos/urls.py`
- Create: `src/crawler/repos/discoveries.py`
- Test: `tests/integration/test_claiming_and_leases.py`
- Test: `tests/integration/test_pause_resume_cancel.py`
- Modify: `src/crawler/db/session.py`

**Interfaces:**
- Consumes: `CrawlJob`, `CrawlUrl`, `DiscoveredLink`, `normalize_url`, `compute_next_retry_at`
- Produces: `class JobRepository`, `class UrlRepository`, `class DiscoveryRepository`, methods `create_job(...)`, `seed_url(...)`, `claim_runnable_urls(...)`, `heartbeat_url(...)`, `release_expired_leases(...)`, `mark_retry_wait(...)`, `request_pause(...)`, `request_resume(...)`, `request_cancel(...)`, `advance_lifecycle_states(...)`, `mark_completed_if_drained(...)`

- [ ] **Step 1: Write the failing integration tests**

```python
async def test_claim_runnable_urls_allows_only_one_active_worker(async_session) -> None:
    async with separate_session_factory() as worker_a_session:
        async with separate_session_factory() as worker_b_session:
            worker_a_repo = UrlRepository(worker_a_session)
            worker_b_repo = UrlRepository(worker_b_session)
            first = await worker_a_repo.claim_runnable_urls(worker_id="worker-a", limit=1)
            second = await worker_b_repo.claim_runnable_urls(worker_id="worker-b", limit=1)
    assert len(first) == 1
    assert second == []


async def test_release_expired_leases_requeues_abandoned_work(async_session) -> None:
    released = await url_repo.release_expired_leases(now=frozen_now)
    assert released == 1


async def test_heartbeat_extends_only_current_workers_lease(async_session) -> None:
    updated = await url_repo.heartbeat_url(
        url_id=claimed_url_id,
        worker_id="worker-a",
        lease_duration_seconds=30,
        now=frozen_now,
    )
    assert updated is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/integration/test_claiming_and_leases.py tests/integration/test_pause_resume_cancel.py -v`
Expected: FAIL with missing repositories, missing separate-session locking behavior, or missing heartbeat semantics

- [ ] **Step 3: Write minimal implementation**

```python
class UrlRepository:
    async def claim_runnable_urls(self, *, worker_id: str, limit: int) -> list[CrawlUrl]:
        stmt = (
            select(CrawlUrl)
            .join(CrawlJob)
            .where(CrawlJob.status == JobStatus.RUNNING)
            .where(CrawlUrl.status.in_([UrlStatus.QUEUED, UrlStatus.RETRY_WAIT]))
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
        rows = list((await self._session.execute(stmt)).scalars())
        for row in rows:
            row.status = UrlStatus.CLAIMED
            row.claimed_by = worker_id
            row.claimed_at = utcnow()
            row.last_heartbeat_at = utcnow()
            row.lease_expires_at = utcnow() + timedelta(seconds=self._lease_duration_seconds)
        return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/integration/test_claiming_and_leases.py::test_claim_runnable_urls_allows_only_one_active_worker -v`
Expected: PASS

- [ ] **Step 5: Complete pause, resume, cancel, and lease-heartbeat operations**

```python
class JobRepository:
    async def request_pause(self, job_id: UUID) -> None: ...
    async def request_resume(self, job_id: UUID) -> None: ...
    async def request_cancel(self, job_id: UUID, cascade_children: bool = True) -> None: ...
    async def mark_completed_if_drained(self, job_id: UUID) -> bool: ...
    async def advance_lifecycle_states(self, *, now: datetime) -> LifecycleAdvanceResult: ...


class UrlRepository:
    async def heartbeat_url(self, *, url_id: UUID, worker_id: str, lease_duration_seconds: int, now: datetime) -> bool: ...
    async def mark_retry_wait(self, *, url_id: UUID, next_eligible_at: datetime, error_code: str, error_detail: str) -> None: ...
    async def release_expired_leases(self, *, now: datetime) -> int: ...
```

- [ ] **Step 6: Run repository integration tests**

Run: `venv/bin/pytest tests/integration/test_claiming_and_leases.py tests/integration/test_pause_resume_cancel.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/crawler/repos src/crawler/db/session.py tests/integration/test_claiming_and_leases.py tests/integration/test_pause_resume_cancel.py
git commit -m "feat: add transactional crawl state repositories"
```

## Task 5: Implement The Fetch Client, Artifact Storage, And Content Processors

**Files:**
- Create: `src/crawler/services/fetch_client.py`
- Create: `src/crawler/services/storage.py`
- Create: `src/crawler/services/processors/base.py`
- Create: `src/crawler/services/processors/html.py`
- Create: `src/crawler/services/processors/images.py`
- Create: `src/crawler/services/processors/videos.py`
- Create: `src/crawler/services/processors/pdfs.py`
- Create: `src/crawler/services/processors/registry.py`
- Test: `tests/unit/test_processors.py`

**Interfaces:**
- Consumes: `normalize_url`, `is_same_hostname`, `should_spawn_child`
- Produces: `class FetchClient`, `class ArtifactStorage`, `class ContentProcessor(Protocol)`, `class ProcessorRegistry`, `async def fetch(url: str) -> FetchResponse`, `def extract_metadata(body: bytes, headers: dict[str, str]) -> dict[str, Any]`, `async def persist_artifact(...) -> ContentArtifact`, `async def persist_metadata(...) -> ContentMetadata`

- [ ] **Step 1: Write the failing processor tests**

```python
def test_html_processor_extracts_title_and_link_count() -> None:
    body = b"<html><head><title>Hello</title></head><body><a href='/a'>A</a><a href='https://other.com'>B</a></body></html>"
    result = HtmlProcessor().extract_metadata(body, {"Content-Type": "text/html"})
    assert result["title"] == "Hello"
    assert result["discovered_link_count"] == 2


def test_image_processor_extracts_dimensions(tmp_path) -> None:
    result = ImageProcessor().extract_metadata(png_bytes, {"Content-Type": "image/png"})
    assert result["width"] == 32
    assert result["height"] == 16
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/unit/test_processors.py -v`
Expected: FAIL with missing processors or unsupported metadata extraction

- [ ] **Step 3: Write minimal implementation**

```python
class HtmlProcessor:
    content_types = ("text/html",)

    def extract_metadata(self, body: bytes, headers: dict[str, str]) -> dict[str, Any]:
        soup = BeautifulSoup(body, "html.parser")
        links = soup.find_all("a", href=True)
        return {"title": soup.title.string if soup.title else None, "discovered_link_count": len(links)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/unit/test_processors.py::test_html_processor_extracts_title_and_link_count -v`
Expected: PASS

- [ ] **Step 5: Add fetch-client classification and deterministic artifact storage**

```python
class FetchClient:
    async def fetch(self, url: str) -> FetchResponse:
        response = await self._http_client.get("/fetch", params={"url": url})
        return FetchResponse.model_validate(response.json())


class ArtifactStorage:
    def build_storage_path(self, *, content_type: str, normalized_url: str, content_hash: str) -> Path:
        bucket = self._bucket_for_content_type(content_type)
        filename = f"{hashlib.sha256(normalized_url.encode()).hexdigest()}-{content_hash}"
        return self._root / bucket / filename
```

- [ ] **Step 6: Run processor tests**

Run: `venv/bin/pytest tests/unit/test_processors.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/crawler/services tests/unit/test_processors.py
git commit -m "feat: add fetch client, storage, and content processors"
```

## Task 6: Implement Crawl Orchestration And Child-Job Logic

**Files:**
- Create: `src/crawler/services/orchestrator.py`
- Modify: `src/crawler/repos/jobs.py`
- Modify: `src/crawler/repos/urls.py`
- Modify: `src/crawler/repos/discoveries.py`
- Test: `tests/integration/test_crawl_pipeline.py`
- Test: `tests/integration/test_child_jobs.py`

**Interfaces:**
- Consumes: `FetchClient.fetch`, `ArtifactStorage.build_storage_path`, `ProcessorRegistry.processor_for`, repository methods from Task 4
- Produces: `class CrawlOrchestrator`, methods `async def process_url(self, *, url_id: UUID, worker_id: str) -> None`, `async def discover_links(...) -> DiscoveryOutcome`, `async def create_child_job_if_needed(...) -> UUID | None`

- [ ] **Step 1: Write the failing integration tests**

```python
async def test_process_html_url_persists_artifact_metadata_and_discoveries(app_container) -> None:
    await orchestrator.process_url(url_id=seed_url_id, worker_id="worker-a")
    saved = await artifact_repo.get_for_url(seed_url_id)
    metadata = await metadata_repo.get_for_artifact(saved.id)
    assert saved.content_type == "text/html"
    assert saved.storage_path.startswith("output/html/")
    assert metadata.metadata_json["title"] == "Example title"


async def test_matching_rule_spawns_child_job_and_skips_parent_enqueuing(app_container) -> None:
    child_job_id = await job_repo.find_child_job(parent_job_id, "https://example.com/products/42")
    assert child_job_id is not None
    assert await url_repo.exists_in_job(parent_job_id, "https://example.com/products/42") is False


async def test_external_links_are_recorded_but_not_enqueued(app_container) -> None:
    await orchestrator.process_url(url_id=seed_url_id, worker_id="worker-a")
    discovery = await discovery_repo.get_by_target("https://external.example.org/page")
    assert discovery.is_same_hostname is False
    assert await url_repo.exists_in_job(parent_job_id, "https://external.example.org/page") is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/integration/test_crawl_pipeline.py tests/integration/test_child_jobs.py -v`
Expected: FAIL with missing orchestrator logic or child-job behavior

- [ ] **Step 3: Write minimal implementation**

```python
class CrawlOrchestrator:
    async def process_url(self, *, url_id: UUID, worker_id: str) -> None:
        url_row = await self._urls.mark_fetching(url_id=url_id, worker_id=worker_id)
        response = await self._fetch_client.fetch(url_row.normalized_url)
        processor = self._processors.processor_for(response.headers["Content-Type"])
        metadata = processor.extract_metadata(response.body or b"", response.headers)
        artifact = await self._artifacts.save(...)
        await self._metadata.save(
            artifact_id=artifact.id,
            metadata_type=processor.metadata_type,
            metadata_json=metadata,
        )
        await self._urls.mark_done(url_id=url_id, content_artifact_id=artifact.id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/integration/test_crawl_pipeline.py::test_process_html_url_persists_artifact_metadata_and_discoveries -v`
Expected: PASS

- [ ] **Step 5: Complete status handling, retry transitions, and child-job creation**

```python
if response.status_code == 429:
    next_retry_at = compute_next_retry_at(...)
    await self._urls.mark_retry_wait(url_id=url_id, next_eligible_at=next_retry_at, error_code="rate_limited", error_detail="retry after")
elif response.status_code == 404:
    await self._urls.mark_failed_permanent(url_id=url_id, error_code="not_found", error_detail="404")
elif should_spawn_child(normalized_link, job.config["child_rules"]):
    child_job_id = await self._jobs.get_or_create_child_job(parent_job_id=job.id, seed_url=normalized_link, seed_hostname=job.seed_hostname, inherited_config=job.config)
else:
    await self._discoveries.record_link(
        job_id=job.id,
        source_url_id=url_id,
        target_normalized_url=normalized_link,
        is_same_hostname=is_same_hostname(job.seed_hostname, normalized_link),
    )
```

- [ ] **Step 6: Run orchestration integration tests**

Run: `venv/bin/pytest tests/integration/test_crawl_pipeline.py tests/integration/test_child_jobs.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/crawler/services/orchestrator.py src/crawler/repos/jobs.py src/crawler/repos/urls.py src/crawler/repos/discoveries.py tests/integration/test_crawl_pipeline.py tests/integration/test_child_jobs.py
git commit -m "feat: add crawl orchestration and child job handling"
```

## Task 7: Add API Schemas And Crawl Lifecycle Endpoints

**Files:**
- Create: `src/crawler/api/schemas.py`
- Create: `src/crawler/api/dependencies.py`
- Create: `src/crawler/api/routes/crawls.py`
- Modify: `src/crawler/main.py`
- Test: `tests/integration/test_job_creation.py`
- Test: `tests/integration/test_pause_resume_cancel.py`

**Interfaces:**
- Consumes: `JobRepository.create_job`, `JobRepository.request_pause`, `JobRepository.request_resume`, `JobRepository.request_cancel`
- Produces: `CreateCrawlRequest`, `CrawlStatusResponse`, `DiscoveredUrlResponse`, endpoints `POST /crawls`, `GET /crawls/{job_id}`, `POST /crawls/{job_id}/pause`, `POST /crawls/{job_id}/resume`, `POST /crawls/{job_id}/cancel`, `GET /crawls/{job_id}/urls`, `GET /crawls/{job_id}/attempts`, `GET /crawls/{job_id}/children`, `GET /crawls/{job_id}/parent`, `GET /crawls/{job_id}/discoveries`

- [ ] **Step 1: Write the failing API tests**

```python
async def test_post_crawls_creates_seed_job_and_seed_url(async_client) -> None:
    response = await async_client.post("/crawls", json={"seed_url": "https://example.com", "child_rules": []})
    assert response.status_code == 201
    assert response.json()["status"] == "pending"


async def test_pause_resume_cancel_endpoints_transition_job(async_client, created_job_id) -> None:
    pause_response = await async_client.post(f"/crawls/{created_job_id}/pause")
    resume_response = await async_client.post(f"/crawls/{created_job_id}/resume")
    cancel_response = await async_client.post(f"/crawls/{created_job_id}/cancel")
    assert pause_response.status_code == 202
    assert resume_response.status_code == 202
    assert cancel_response.status_code == 202
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/integration/test_job_creation.py tests/integration/test_pause_resume_cancel.py -v`
Expected: FAIL with missing routes or schema validation

- [ ] **Step 3: Write minimal implementation**

```python
class CreateCrawlRequest(BaseModel):
    seed_url: AnyHttpUrl
    child_rules: list[dict[str, str]] = Field(default_factory=list)


router = APIRouter(prefix="/crawls")


@router.post("", status_code=201, response_model=CrawlStatusResponse)
async def create_crawl(payload: CreateCrawlRequest, jobs: JobRepository = Depends(get_job_repository)) -> CrawlStatusResponse:
    return await jobs.create_job_from_request(payload)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/integration/test_job_creation.py::test_post_crawls_creates_seed_job_and_seed_url -v`
Expected: PASS

- [ ] **Step 5: Add read-only inspection endpoints and progress projection**

```python
@router.get("/{job_id}/urls", response_model=list[DiscoveredUrlResponse])
async def list_job_urls(job_id: UUID, urls: UrlRepository = Depends(get_url_repository)) -> list[DiscoveredUrlResponse]:
    return await urls.list_for_job(job_id)


@router.get("/{job_id}", response_model=CrawlStatusResponse)
async def get_job_status(job_id: UUID, jobs: JobRepository = Depends(get_job_repository)) -> CrawlStatusResponse:
    return await jobs.get_status_projection(job_id)
```

- [ ] **Step 6: Run API integration tests**

Run: `venv/bin/pytest tests/integration/test_job_creation.py tests/integration/test_pause_resume_cancel.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/crawler/api src/crawler/main.py tests/integration/test_job_creation.py tests/integration/test_pause_resume_cancel.py
git commit -m "feat: add crawl lifecycle and inspection APIs"
```

## Task 8: Implement RabbitMQ Publishing, Worker Loops, And Scheduler Recovery

**Files:**
- Create: `src/crawler/messaging/rabbitmq.py`
- Create: `src/crawler/services/scheduler.py`
- Create: `src/crawler/workers/crawler_worker.py`
- Create: `src/crawler/workers/scheduler_worker.py`
- Test: `tests/unit/test_scheduler.py`
- Test: `tests/integration/test_rabbitmq_recovery.py`

**Interfaces:**
- Consumes: `CrawlOrchestrator.process_url`, `UrlRepository.claim_runnable_urls`, `UrlRepository.release_expired_leases`, `JobRepository.list_resumable_jobs`
- Produces: `class RabbitPublisher`, `class SchedulerService`, `async def run_crawler_worker() -> None`, `async def run_scheduler_worker() -> None`, `async def reconcile_once(now: datetime) -> ReconcileResult`

- [ ] **Step 1: Write the failing scheduler tests**

```python
async def test_scheduler_republishes_runnable_work_when_queue_message_was_lost(fake_repos, fake_publisher) -> None:
    await SchedulerService(fake_repos.jobs, fake_repos.urls, fake_publisher).reconcile_once(now=frozen_now)
    assert fake_publisher.published == [{"job_id": str(job_id)}]


async def test_scheduler_releases_expired_leases_before_requeue(fake_repos, fake_publisher) -> None:
    released = await SchedulerService(fake_repos.jobs, fake_repos.urls, fake_publisher).reconcile_once(now=frozen_now)
    assert released.expired_leases_released == 1


async def test_scheduler_advances_pausing_canceling_and_completed_jobs(fake_repos, fake_publisher) -> None:
    result = await SchedulerService(fake_repos.jobs, fake_repos.urls, fake_publisher).reconcile_once(now=frozen_now)
    assert result.paused_jobs == 1
    assert result.canceled_jobs == 1
    assert result.completed_jobs == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `venv/bin/pytest tests/unit/test_scheduler.py tests/integration/test_rabbitmq_recovery.py -v`
Expected: FAIL with missing scheduler recovery logic or publisher integration

- [ ] **Step 3: Write minimal implementation**

```python
class SchedulerService:
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `venv/bin/pytest tests/unit/test_scheduler.py::test_scheduler_republishes_runnable_work_when_queue_message_was_lost -v`
Expected: PASS

- [ ] **Step 5: Complete worker loops and explicit heartbeat behavior**

```python
async def run_crawler_worker() -> None:
    async for message in consumer:
        async with message.process():
            claimed = await urls.claim_runnable_urls(worker_id=worker_id, limit=batch_size)
            for row in claimed:
                heartbeat_task = asyncio.create_task(
                    heartbeat_loop(
                        url_id=row.id,
                        worker_id=worker_id,
                        interval_seconds=settings.heartbeat_interval_seconds,
                        lease_duration_seconds=settings.lease_duration_seconds,
                    )
                )
                try:
                    await orchestrator.process_url(url_id=row.id, worker_id=worker_id)
                finally:
                    heartbeat_task.cancel()


async def run_scheduler_worker() -> None:
    while True:
        await scheduler.reconcile_once(now=utcnow())
        await asyncio.sleep(settings.scheduler_poll_interval_seconds)
```

- [ ] **Step 6: Run scheduler and messaging tests**

Run: `venv/bin/pytest tests/unit/test_scheduler.py tests/integration/test_rabbitmq_recovery.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/crawler/messaging src/crawler/services/scheduler.py src/crawler/workers tests/unit/test_scheduler.py tests/integration/test_rabbitmq_recovery.py
git commit -m "feat: add scheduler recovery and worker runtime loops"
```

## Task 9: Full-System Verification, Documentation, And Operational Checks

**Files:**
- Modify: `README.md`
- Modify: `.env.example`
- Modify: `docker-compose.yml`
- Test: `tests/integration/test_job_creation.py`
- Test: `tests/integration/test_claiming_and_leases.py`
- Test: `tests/integration/test_crawl_pipeline.py`
- Test: `tests/integration/test_pause_resume_cancel.py`
- Test: `tests/integration/test_child_jobs.py`
- Test: `tests/integration/test_rabbitmq_recovery.py`

**Interfaces:**
- Consumes: all interfaces from Tasks 1-8
- Produces: verified startup instructions, local run commands, end-to-end verification checklist

- [ ] **Step 1: Write the failing operational test or checklist**

```python
async def test_end_to_end_crawl_job_reaches_terminal_state(async_client, fake_fetch_service) -> None:
    response = await async_client.post("/crawls", json={"seed_url": "https://example.com", "child_rules": []})
    assert response.status_code == 201
    assert response.json()["status"] in {"pending", "running"}
```

- [ ] **Step 2: Run verification suite to surface missing pieces**

Run: `venv/bin/pytest tests/integration -v`
Expected: FAIL until all remaining wiring, migrations, and worker coordination are complete

- [ ] **Step 3: Complete README and environment documentation**

```markdown
## Local development

1. `docker compose up -d postgres rabbitmq`
2. `venv/bin/alembic upgrade head`
3. `venv/bin/uvicorn crawler.main:create_app --factory --reload`
4. `venv/bin/python -m crawler.workers.crawler_worker`
5. `venv/bin/python -m crawler.workers.scheduler_worker`
```

- [ ] **Step 4: Run full verification**

Run: `venv/bin/pytest tests/unit -v`
Expected: PASS

Run: `venv/bin/pytest tests/integration -v`
Expected: PASS

Run: `venv/bin/alembic upgrade head`
Expected: PASS with no pending migration errors

Run: `venv/bin/python -c "from crawler.main import create_app; app = create_app(); print(app.routes)"`
Expected: PASS and prints registered routes

- [ ] **Step 5: Commit**

```bash
git add README.md .env.example docker-compose.yml tests/integration
git commit -m "docs: finalize crawler verification and runbook"
```

## Spec Coverage Check

- Multi-job concurrency, retries, and resumability: Tasks 2, 4, 6, and 8
- Strict seed-hostname crawl boundary: Task 3 and Task 6
- Child-job rules and parent/child tracking: Tasks 2, 6, and 7
- PostgreSQL as source of truth and RabbitMQ as wake-up only: Tasks 2, 4, and 8
- Lease and heartbeat safety for long tasks: Task 2, Task 4, and Task 8
- Processing HTML, images, videos, and PDFs with extensible handlers: Task 5 and Task 6
- Lifecycle and inspection endpoints: Task 7
- Scheduler recovery when RabbitMQ messages are lost: Task 8
- Output artifact persistence and metadata extraction: Task 5 and Task 6

## Placeholder Scan

- No `TBD`, `TODO`, or “implement later” placeholders remain
- Every task lists exact files, interfaces, commands, and code-shape examples
- Every behavior-changing task includes RED -> GREEN -> REFACTOR sequencing

## Type Consistency Check

- `Settings`, repository classes, `CrawlOrchestrator`, and `SchedulerService` signatures are defined before later tasks consume them
- Job and URL states used in tests align with the approved design doc
- Child-job creation uses the same `seed_hostname` and `child_rules` terminology throughout the plan
