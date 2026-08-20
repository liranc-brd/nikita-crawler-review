from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from crawler.db.models.enums import JobStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.models.urls import CrawlUrl
from crawler.repos.jobs import JobRepository

from test_crawl_pipeline import (
    CrawlTestContainer,
    app_container,
    async_session,
    session_factory,
    test_database_lock,
)


@pytest.mark.anyio
async def test_matching_rule_spawns_child_job_and_skips_parent_enqueuing(
    app_container: CrawlTestContainer,
) -> None:
    parent_job = await app_container.async_session.get(CrawlJob, app_container.parent_job_id)
    assert parent_job is not None
    parent_job.config = {
        "max_attempts": 3,
        "child_rules": [{"kind": "path_prefix", "value": "/products"}],
    }
    await app_container.async_session.commit()

    await app_container.orchestrator.process_url(
        url_id=app_container.seed_url_id,
        worker_id="worker-a",
    )

    child_job_id = await app_container.job_repo.find_child_job(
        app_container.parent_job_id,
        f"https://{parent_job.seed_hostname}/products/42",
    )
    assert child_job_id is not None
    app_container.async_session.info["task6_job_ids"].add(child_job_id)
    child_job = await app_container.async_session.get(CrawlJob, child_job_id)
    assert child_job is not None
    assert child_job.status is JobStatus.RUNNING
    assert await app_container.url_repo.exists_in_job(
        app_container.parent_job_id,
        f"https://{parent_job.seed_hostname}/products/42",
    ) is False

    child_seed = await app_container.async_session.scalar(
        select(CrawlUrl).where(CrawlUrl.job_id == child_job_id)
    )
    assert child_seed is not None


@pytest.mark.anyio
async def test_child_job_creation_is_idempotent_across_sessions(
    app_container: CrawlTestContainer,
    session_factory,
) -> None:
    parent_job = await app_container.async_session.get(CrawlJob, app_container.parent_job_id)
    assert parent_job is not None
    child_seed_url = f"https://{parent_job.seed_hostname}/products/42"
    first_lookup_complete = asyncio.Event()
    lookup_count = 0

    async def create_child_job() -> object:
        nonlocal lookup_count
        async with session_factory() as session:
            original_scalar = session.scalar
            scalar_calls = 0

            async def synchronize_first_lookup(*args, **kwargs):
                nonlocal lookup_count, scalar_calls
                result = await original_scalar(*args, **kwargs)
                scalar_calls += 1
                if scalar_calls == 1:
                    lookup_count += 1
                    if lookup_count == 2:
                        first_lookup_complete.set()
                    await first_lookup_complete.wait()
                return result

            session.scalar = synchronize_first_lookup
            child_job_id = await JobRepository(session).get_or_create_child_job(
                parent_job_id=parent_job.id,
                seed_url=child_seed_url,
                seed_hostname=parent_job.seed_hostname,
                inherited_config=parent_job.config,
            )
            await session.commit()
            return child_job_id

    child_job_ids = await asyncio.gather(create_child_job(), create_child_job())
    saved_child_job_ids = list(
        (
            await app_container.async_session.scalars(
                select(CrawlJob.id).where(CrawlJob.parent_job_id == parent_job.id)
            )
        ).all()
    )
    app_container.async_session.info["task6_job_ids"].update(saved_child_job_ids)

    assert child_job_ids[0] == child_job_ids[1]
    assert saved_child_job_ids == [child_job_ids[0]]
