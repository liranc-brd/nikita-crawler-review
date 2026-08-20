from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable
from uuid import UUID

from bs4 import BeautifulSoup
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.domain.retry_policy import compute_next_retry_at
from crawler.domain.url_policy import is_same_hostname, normalize_url, should_spawn_child
from crawler.db.models.attempts import CrawlAttempt
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.repos.attempts import AttemptRepository
from crawler.repos.discoveries import DiscoveryRepository
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository
from crawler.services.fetch_client import FetchClient, FetchResponse
from crawler.services.processors.registry import ProcessorRegistry
from crawler.services.storage import ArtifactStorage


@dataclass(frozen=True)
class DiscoveryOutcome:
    enqueued_urls: int
    recorded_links: int
    spawned_child_jobs: int


class CrawlOrchestrator:
    def __init__(
        self,
        *,
        session: AsyncSession,
        fetch_client: FetchClient,
        storage: ArtifactStorage,
        processors: ProcessorRegistry,
        jobs: JobRepository,
        urls: UrlRepository,
        discoveries: DiscoveryRepository,
        attempts: AttemptRepository,
    ) -> None:
        self._session = session
        self._fetch_client = fetch_client
        self._storage = storage
        self._processors = processors
        self._jobs = jobs
        self._urls = urls
        self._discoveries = discoveries
        self._attempts = attempts

    async def process_url(self, *, url_id: UUID, worker_id: str) -> None:
        claimed_url = await self._urls.get_claimed_url(url_id=url_id, worker_id=worker_id)
        if claimed_url is None:
            return

        job = await self._jobs.get(claimed_url.job_id)
        if job is None:
            raise RuntimeError("crawl job does not exist")
        if job.status is not JobStatus.RUNNING:
            return

        url_row = await self._urls.mark_fetching(url_id=url_id, worker_id=worker_id)
        if url_row is None:
            return
        url_row_id = url_row.id
        job_id = url_row.job_id
        normalized_url = url_row.normalized_url
        fetch_attempts = url_row.fetch_attempts
        job_config = dict(job.config)
        attempt = await self._attempts.start_attempt(
            crawl_url_id=url_row_id,
        )
        attempt_id = attempt.id
        await self._session.commit()

        try:
            response = await self._fetch_client.fetch(normalized_url)
        except Exception as error:
            result_status = await self._mark_transient_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                fetch_attempts=fetch_attempts,
                job_config=job_config,
                http_status_code=None,
                error_code="fetch_error",
                error_detail=str(error),
            )
            if result_status is None:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=result_status,
                http_status_code=None,
                retry_after_seconds=None,
                response_headers=None,
                error_detail=str(error),
            )
            await self._session.commit()
            return

        if response.status_code == 404:
            transitioned = await self._mark_permanent_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                http_status_code=404,
                error_code="not_found",
                error_detail="404",
            )
            if not transitioned:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=UrlStatus.FAILED_PERMANENT.value,
                http_status_code=404,
                retry_after_seconds=None,
                response_headers=response.headers,
                error_detail="404",
            )
            await self._session.commit()
            return
        if response.status_code == 403:
            transitioned = await self._mark_permanent_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                http_status_code=403,
                error_code="blocked",
                error_detail="403",
            )
            if not transitioned:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=UrlStatus.FAILED_PERMANENT.value,
                http_status_code=403,
                retry_after_seconds=None,
                response_headers=response.headers,
                error_detail="403",
            )
            await self._session.commit()
            return
        if response.status_code == 429:
            retry_after_seconds = _retry_after_seconds(response.headers, job_config)
            result_status = await self._mark_transient_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                fetch_attempts=fetch_attempts,
                job_config=job_config,
                http_status_code=429,
                error_code="rate_limited",
                error_detail="retry after",
                retry_after_seconds=retry_after_seconds,
            )
            if result_status is None:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=result_status,
                http_status_code=429,
                retry_after_seconds=retry_after_seconds,
                response_headers=response.headers,
                error_detail="retry after",
            )
            await self._session.commit()
            return
        if response.status_code == 500:
            result_status = await self._mark_transient_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                fetch_attempts=fetch_attempts,
                job_config=job_config,
                http_status_code=500,
                error_code="server_error",
                error_detail="500",
            )
            if result_status is None:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=result_status,
                http_status_code=500,
                retry_after_seconds=None,
                response_headers=response.headers,
                error_detail="500",
            )
            await self._session.commit()
            return
        if response.status_code != 200:
            result_status = await self._mark_transient_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                fetch_attempts=fetch_attempts,
                job_config=job_config,
                http_status_code=response.status_code,
                error_code="unexpected_status",
                error_detail=str(response.status_code),
            )
            if result_status is None:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=result_status,
                http_status_code=response.status_code,
                retry_after_seconds=None,
                response_headers=response.headers,
                error_detail=str(response.status_code),
            )
            await self._session.commit()
            return

        try:
            content_type = _content_type_from_headers(response.headers)
            processor = self._processors.processor_for(content_type)
            body = response.body or b""
            metadata = processor.extract_metadata(body, response.headers)
            locked_job = await self._jobs.lock_running_job(job_id)
            if locked_job is None:
                await self._session.rollback()
                return
            staged_artifact = await self._storage.stage_artifact(
                job_id=job_id,
                crawl_url_id=url_row_id,
                content_type=content_type,
                url=normalized_url,
                body=body,
                headers=response.headers,
            )
            processed, created_storage_path = await self._process_success_response(
                url_row=url_row,
                response=response,
                content_type=content_type,
                metadata=metadata,
                staged_artifact=staged_artifact,
                worker_id=worker_id,
            )
        except Exception as error:
            if "staged_artifact" in locals():
                if "created_storage_path" in locals() and created_storage_path:
                    await self._storage.delete_persisted_artifact(
                        storage_path=staged_artifact.artifact.storage_path
                    )
                else:
                    await self._storage.delete_staged_artifact(staged_artifact)
            await self._session.rollback()
            result_status = await self._mark_transient_failure(
                url_id=url_row_id,
                worker_id=worker_id,
                fetch_attempts=fetch_attempts,
                job_config=job_config,
                http_status_code=response.status_code,
                error_code="processing_error",
                error_detail=str(error),
            )
            if result_status is None:
                await self._session.rollback()
                return
            await self._finish_attempt(
                attempt_id=attempt_id,
                result_status=result_status,
                http_status_code=response.status_code,
                retry_after_seconds=None,
                response_headers=response.headers,
                error_detail=str(error),
            )
            await self._session.commit()
            return

        if not processed:
            if created_storage_path:
                await self._storage.delete_persisted_artifact(
                    storage_path=staged_artifact.artifact.storage_path
                )
            else:
                await self._storage.delete_staged_artifact(staged_artifact)
            await self._session.rollback()
            return

        await self._finish_attempt(
            attempt_id=attempt_id,
            result_status="success",
            http_status_code=response.status_code,
            retry_after_seconds=None,
            response_headers=response.headers,
            error_detail=None,
        )
        await self._session.commit()

    async def _process_success_response(
        self,
        *,
        url_row: CrawlUrl,
        response: FetchResponse,
        content_type: str,
        metadata: dict[str, object],
        staged_artifact,
        worker_id: str,
    ) -> tuple[bool, bool]:
        if not await self._urls.mark_processing(
            url_id=url_row.id,
            worker_id=worker_id,
            content_type=content_type,
            http_status_code=response.status_code,
        ):
            return False, False

        created_storage_path = await self._storage.promote_staged_artifact(staged_artifact)
        artifact = staged_artifact.artifact
        self._session.add(artifact)
        await self._session.flush()
        await self._storage.persist_metadata(
            artifact_id=artifact.id,  # type: ignore[attr-defined]
            metadata_type=self._processors.processor_for(content_type).metadata_type,
            metadata=metadata,
        )
        await self.discover_links(
            job_id=url_row.job_id,
            source_url_id=url_row.id,
            source_url=url_row.normalized_url,
            links=_html_links(response.body or b"", content_type),
        )
        if not await self._urls.mark_done(
            url_id=url_row.id,
            worker_id=worker_id,
            content_artifact_id=artifact.id,  # type: ignore[attr-defined]
        ):
            return False, created_storage_path
        return True, created_storage_path

    async def discover_links(
        self,
        *,
        job_id: UUID,
        source_url_id: UUID,
        source_url: str,
        links: Iterable[str],
    ) -> DiscoveryOutcome:
        job = await self._jobs.lock_running_job(job_id)
        if job is None:
            raise RuntimeError("crawl job does not exist")

        resolved_links = list(links)
        enqueued_urls = 0
        recorded_links = 0
        spawned_child_jobs = 0
        for link in resolved_links:
            normalized_link = normalize_url(link, base_url=source_url)
            same_hostname = is_same_hostname(job.seed_hostname, normalized_link)
            child_job_id = await self._create_child_job_if_needed_locked(
                parent_job=job,
                seed_url=normalized_link,
            )
            target_url_id = None
            if same_hostname and child_job_id is None:
                target_url = await self._urls.seed_url(
                    job_id=job.id,
                    seed_url=normalized_link,
                    discovered_from_url_id=source_url_id,
                )
                target_url_id = target_url.id
                enqueued_urls += 1
            if child_job_id is not None:
                spawned_child_jobs += 1
            await self._discoveries.record_discovery(
                job_id=job.id,
                source_url_id=source_url_id,
                target_url=normalized_link,
                target_url_id=target_url_id,
                is_same_hostname=same_hostname,
                spawned_child_job_id=child_job_id,
            )
            recorded_links += 1
        return DiscoveryOutcome(
            enqueued_urls=enqueued_urls,
            recorded_links=recorded_links,
            spawned_child_jobs=spawned_child_jobs,
        )

    async def create_child_job_if_needed(
        self,
        *,
        parent_job: CrawlJob,
        seed_url: str,
    ) -> UUID | None:
        locked_parent_job = await self._jobs.lock_running_job(parent_job.id)
        if locked_parent_job is None:
            return None
        return await self._create_child_job_if_needed_locked(
            parent_job=locked_parent_job,
            seed_url=seed_url,
        )

    async def _create_child_job_if_needed_locked(
        self,
        *,
        parent_job: CrawlJob,
        seed_url: str,
    ) -> UUID | None:
        if not is_same_hostname(parent_job.seed_hostname, seed_url):
            return None
        if not should_spawn_child(seed_url, parent_job.config.get("child_rules", [])):
            return None

        child_job_id = await self._jobs.get_or_create_child_job(
            parent_job_id=parent_job.id,
            seed_url=seed_url,
            seed_hostname=parent_job.seed_hostname,
            inherited_config=parent_job.config,
        )
        await self._urls.seed_url(job_id=child_job_id, seed_url=seed_url)
        return child_job_id

    async def _mark_permanent_failure(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        http_status_code: int | None,
        error_code: str,
        error_detail: str,
    ) -> bool:
        return await self._urls.mark_failed_permanent(
            url_id=url_id,
            worker_id=worker_id,
            http_status_code=http_status_code,
            error_code=error_code,
            error_detail=error_detail,
        )

    async def _mark_transient_failure(
        self,
        *,
        url_id: UUID,
        worker_id: str,
        fetch_attempts: int,
        job_config: dict[str, object],
        http_status_code: int | None,
        error_code: str,
        error_detail: str,
        retry_after_seconds: int | None = None,
    ) -> str | None:
        attempt_number = fetch_attempts + 1
        max_attempts = int(job_config.get("max_attempts", 3))
        if attempt_number >= max_attempts:
            if not await self._mark_permanent_failure(
                url_id=url_id,
                worker_id=worker_id,
                http_status_code=http_status_code,
                error_code="retry_exhausted",
                error_detail=error_detail,
            ):
                return None
            return UrlStatus.FAILED_PERMANENT.value

        next_retry_at = compute_next_retry_at(
            now=datetime.now(UTC).replace(tzinfo=None),
            attempt_number=attempt_number,
            base_backoff_seconds=int(job_config.get("base_backoff_seconds", 10)),
            max_backoff_seconds=int(job_config.get("max_backoff_seconds", 300)),
            retry_after_seconds=retry_after_seconds,
        )
        if not await self._urls.mark_retry_wait(
            url_id=url_id,
            worker_id=worker_id,
            next_eligible_at=next_retry_at,
            error_code=error_code,
            error_detail=error_detail,
            http_status_code=http_status_code,
        ):
            return None
        return UrlStatus.RETRY_WAIT.value

    async def _finish_attempt(
        self,
        *,
        attempt_id: UUID,
        result_status: str,
        http_status_code: int | None,
        retry_after_seconds: int | None,
        response_headers: dict[str, str] | None,
        error_detail: str | None,
    ) -> None:
        await self._attempts.finish_attempt(
            attempt_id=attempt_id,
            result_status=result_status,
            http_status_code=http_status_code,
            retry_after_seconds=retry_after_seconds,
            response_headers=response_headers,
            error_detail=error_detail,
        )


def _content_type_from_headers(headers: dict[str, str]) -> str:
    for name, value in headers.items():
        if name.lower() == "content-type":
            return value
    raise ValueError("fetch response did not include Content-Type")


def _html_links(body: bytes, content_type: str) -> list[str]:
    if content_type.split(";", maxsplit=1)[0].strip().lower() != "text/html":
        return []
    soup = BeautifulSoup(body, "html.parser")
    return [str(anchor["href"]) for anchor in soup.find_all("a", href=True)]


def _retry_after_seconds(headers: dict[str, str], config: dict[str, object]) -> int | None:
    if not config.get("respect_retry_after", True):
        return None
    for name, value in headers.items():
        if name.lower() == "retry-after":
            try:
                return max(0, int(value))
            except ValueError:
                return None
    return None
