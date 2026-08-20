from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, Field

from crawler.db.models.enums import JobStatus, UrlStatus


class CreateCrawlRequest(BaseModel):
    seed_url: AnyHttpUrl
    child_rules: list[dict[str, str]] = Field(default_factory=list)


class CrawlProgressResponse(BaseModel):
    total_discovered: int
    runnable: int
    in_progress: int
    retry_waiting: int
    done: int
    permanently_failed: int
    canceled: int
    child_jobs_spawned: int


class CrawlStatusResponse(BaseModel):
    id: UUID
    seed_url: str
    seed_hostname: str
    parent_job_id: UUID | None
    status: JobStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    pause_requested_at: datetime | None
    cancel_requested_at: datetime | None
    progress: CrawlProgressResponse | None = None


class DiscoveredUrlResponse(BaseModel):
    id: UUID
    job_id: UUID
    normalized_url: str
    discovered_from_url_id: UUID | None
    status: UrlStatus
    content_type: str | None
    http_status_code: int | None
    fetch_attempts: int
    next_eligible_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    error_code: str | None
    error_detail: str | None


class CrawlAttemptResponse(BaseModel):
    id: UUID
    crawl_url_id: UUID
    attempt_number: int
    started_at: datetime
    finished_at: datetime | None
    result_status: str
    http_status_code: int | None
    retry_after_seconds: int | None
    response_headers: dict[str, str] | None
    error_detail: str | None


class DiscoveryResponse(BaseModel):
    id: UUID
    job_id: UUID
    source_url_id: UUID
    target_normalized_url: str
    target_url_id: UUID | None
    is_same_hostname: bool
    spawned_child_job_id: UUID | None
    discovered_at: datetime
