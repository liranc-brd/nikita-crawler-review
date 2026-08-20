from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.api.dependencies import get_job_repository, get_session
from crawler.api.schemas import (
    CrawlAttemptResponse,
    CrawlProgressResponse,
    CrawlStatusResponse,
    CreateCrawlRequest,
    DiscoveredUrlResponse,
    DiscoveryResponse,
)
from crawler.config import Settings
from crawler.db.models.attempts import CrawlAttempt
from crawler.db.models.enums import JobStatus, UrlStatus
from crawler.db.models.discoveries import DiscoveredLink
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.messaging.rabbitmq import publish_job_wakeup_once
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository


router = APIRouter(prefix="/crawls")
logger = logging.getLogger(__name__)


@router.post("", status_code=status.HTTP_201_CREATED, response_model=CrawlStatusResponse)
async def create_crawl(
    payload: CreateCrawlRequest,
    session: AsyncSession = Depends(get_session),
) -> CrawlStatusResponse:
    jobs = JobRepository(session)
    urls = UrlRepository(session)
    job = await jobs.create_job(
        seed_url=str(payload.seed_url),
        config={"max_attempts": 3, "child_rules": payload.child_rules},
    )
    job.status = JobStatus.RUNNING
    job.started_at = datetime.now(UTC).replace(tzinfo=None)
    await urls.seed_url(job_id=job.id, seed_url=job.seed_url)
    await session.commit()
    try:
        await publish_job_wakeup_once(
            rabbitmq_url=Settings().rabbitmq_url,
            job_id=job.id,
        )
    except Exception:
        logger.warning("failed to publish initial crawl wakeup", extra={"job_id": str(job.id)})
    return _crawl_status_response(job)


@router.get("/{job_id}", response_model=CrawlStatusResponse)
async def get_crawl_status(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> CrawlStatusResponse:
    job = await _get_job_or_404(job_id, JobRepository(session))
    progress = await _get_progress(job_id, session)
    return _crawl_status_response(job, progress=progress)


@router.post("/{job_id}/pause", status_code=status.HTTP_202_ACCEPTED)
async def pause_crawl(
    job_id: UUID,
    jobs: JobRepository = Depends(get_job_repository),
) -> None:
    await _get_job_or_404(job_id, jobs)
    await jobs.request_pause(job_id)


@router.post("/{job_id}/resume", status_code=status.HTTP_202_ACCEPTED)
async def resume_crawl(
    job_id: UUID,
    jobs: JobRepository = Depends(get_job_repository),
) -> None:
    await _get_job_or_404(job_id, jobs)
    await jobs.request_resume(job_id)


@router.post("/{job_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_crawl(
    job_id: UUID,
    jobs: JobRepository = Depends(get_job_repository),
) -> None:
    await _get_job_or_404(job_id, jobs)
    await jobs.request_cancel(job_id)


@router.get("/{job_id}/urls", response_model=list[DiscoveredUrlResponse])
async def list_crawl_urls(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[DiscoveredUrlResponse]:
    await _get_job_or_404(job_id, JobRepository(session))
    crawl_urls = (
        await session.scalars(
            select(CrawlUrl).where(CrawlUrl.job_id == job_id).order_by(CrawlUrl.id)
        )
    ).all()
    return [_discovered_url_response(crawl_url) for crawl_url in crawl_urls]


@router.get("/{job_id}/attempts", response_model=list[CrawlAttemptResponse])
async def list_crawl_attempts(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[CrawlAttemptResponse]:
    await _get_job_or_404(job_id, JobRepository(session))
    attempts = (
        await session.scalars(
            select(CrawlAttempt)
            .join(CrawlUrl, CrawlAttempt.crawl_url_id == CrawlUrl.id)
            .where(CrawlUrl.job_id == job_id)
            .order_by(CrawlAttempt.started_at, CrawlAttempt.id)
        )
    ).all()
    return [_attempt_response(attempt) for attempt in attempts]


@router.get("/{job_id}/children", response_model=list[CrawlStatusResponse])
async def list_child_crawls(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[CrawlStatusResponse]:
    await _get_job_or_404(job_id, JobRepository(session))
    children = (
        await session.scalars(
            select(CrawlJob)
            .where(CrawlJob.parent_job_id == job_id)
            .order_by(CrawlJob.created_at, CrawlJob.id)
        )
    ).all()
    return [_crawl_status_response(child) for child in children]


@router.get("/{job_id}/parent", response_model=CrawlStatusResponse | None)
async def get_parent_crawl(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> CrawlStatusResponse | None:
    job = await _get_job_or_404(job_id, JobRepository(session))
    if job.parent_job_id is None:
        return None
    parent = await _get_job_or_404(job.parent_job_id, JobRepository(session))
    return _crawl_status_response(parent)


@router.get("/{job_id}/discoveries", response_model=list[DiscoveryResponse])
async def list_crawl_discoveries(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[DiscoveryResponse]:
    await _get_job_or_404(job_id, JobRepository(session))
    discoveries = (
        await session.scalars(
            select(DiscoveredLink)
            .where(DiscoveredLink.job_id == job_id)
            .order_by(DiscoveredLink.discovered_at, DiscoveredLink.id)
        )
    ).all()
    return [_discovery_response(discovery) for discovery in discoveries]


async def _get_job_or_404(job_id: UUID, jobs: JobRepository) -> CrawlJob:
    job = await jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="crawl not found")
    return job


async def _get_progress(job_id: UUID, session: AsyncSession) -> CrawlProgressResponse:
    status_counts = dict(
        (
            await session.execute(
                select(CrawlUrl.status, func.count())
                .where(CrawlUrl.job_id == job_id)
                .group_by(CrawlUrl.status)
            )
        ).all()
    )
    child_jobs_spawned = await session.scalar(
        select(func.count()).select_from(CrawlJob).where(CrawlJob.parent_job_id == job_id)
    )

    def count(status: UrlStatus) -> int:
        return status_counts.get(status, 0)

    return CrawlProgressResponse(
        total_discovered=sum(status_counts.values()),
        runnable=count(UrlStatus.QUEUED),
        in_progress=sum(
            count(url_status)
            for url_status in (UrlStatus.CLAIMED, UrlStatus.FETCHING, UrlStatus.PROCESSING)
        ),
        retry_waiting=count(UrlStatus.RETRY_WAIT),
        done=count(UrlStatus.DONE),
        permanently_failed=count(UrlStatus.FAILED_PERMANENT),
        canceled=count(UrlStatus.CANCELED),
        child_jobs_spawned=child_jobs_spawned or 0,
    )


def _crawl_status_response(
    job: CrawlJob,
    *,
    progress: CrawlProgressResponse | None = None,
) -> CrawlStatusResponse:
    return CrawlStatusResponse(
        id=job.id,
        seed_url=job.seed_url,
        seed_hostname=job.seed_hostname,
        parent_job_id=job.parent_job_id,
        status=job.status,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        pause_requested_at=job.pause_requested_at,
        cancel_requested_at=job.cancel_requested_at,
        progress=progress,
    )


def _discovered_url_response(crawl_url: CrawlUrl) -> DiscoveredUrlResponse:
    return DiscoveredUrlResponse(
        id=crawl_url.id,
        job_id=crawl_url.job_id,
        normalized_url=crawl_url.normalized_url,
        discovered_from_url_id=crawl_url.discovered_from_url_id,
        status=crawl_url.status,
        content_type=crawl_url.content_type,
        http_status_code=crawl_url.http_status_code,
        fetch_attempts=crawl_url.fetch_attempts,
        next_eligible_at=crawl_url.next_eligible_at,
        started_at=crawl_url.started_at,
        finished_at=crawl_url.finished_at,
        error_code=crawl_url.error_code,
        error_detail=crawl_url.error_detail,
    )


def _attempt_response(attempt: CrawlAttempt) -> CrawlAttemptResponse:
    return CrawlAttemptResponse(
        id=attempt.id,
        crawl_url_id=attempt.crawl_url_id,
        attempt_number=attempt.attempt_number,
        started_at=attempt.started_at,
        finished_at=attempt.finished_at,
        result_status=attempt.result_status,
        http_status_code=attempt.http_status_code,
        retry_after_seconds=attempt.retry_after_seconds,
        response_headers=attempt.response_headers,
        error_detail=attempt.error_detail,
    )


def _discovery_response(discovery: DiscoveredLink) -> DiscoveryResponse:
    return DiscoveryResponse(
        id=discovery.id,
        job_id=discovery.job_id,
        source_url_id=discovery.source_url_id,
        target_normalized_url=discovery.target_normalized_url,
        target_url_id=discovery.target_url_id,
        is_same_hostname=discovery.is_same_hostname,
        spawned_child_job_id=discovery.spawned_child_job_id,
        discovered_at=discovery.discovered_at,
    )
