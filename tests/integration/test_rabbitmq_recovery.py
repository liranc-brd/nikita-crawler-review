from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import datetime
from uuid import UUID, uuid4

import aio_pika
import pytest
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler.config import Settings
from crawler.db.models.enums import JobStatus
from crawler.db.models.jobs import CrawlJob
from crawler.db.session import async_session_factory
from crawler.messaging.rabbitmq import RabbitPublisher
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository
from crawler.services.scheduler import SchedulerService


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
        await connection.execute(text("SELECT pg_advisory_lock(2026082008)"))
        yield
        await connection.execute(text("SELECT pg_advisory_unlock(2026082008)"))


@pytest.mark.anyio
async def test_scheduler_republishes_durable_work_after_a_lost_rabbitmq_wakeup(
    session_factory: async_sessionmaker[AsyncSession],
    test_database_lock: None,
) -> None:
    job_id: UUID | None = None
    queue_name = f"crawler-task-8-{uuid4().hex}"
    settings = Settings()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True)
    frozen_now = datetime(2026, 8, 20, 12, 0, 0)

    try:
        async with session_factory() as session:
            jobs = JobRepository(session)
            urls = UrlRepository(session)
            job = await jobs.create_job(
                seed_url=f"https://task8-{uuid4().hex}.example.com",
                config={"max_attempts": 3, "child_rules": []},
            )
            job.status = JobStatus.RUNNING
            await urls.seed_url(job_id=job.id, seed_url=job.seed_url)
            job_id = job.id
            await session.commit()

            publisher = RabbitPublisher(channel, queue_name=queue_name)
            result = await SchedulerService(jobs, urls, publisher).reconcile_once(now=frozen_now)
            await session.commit()

        message = await queue.get(timeout=5)
        await message.ack()

        assert json.loads(message.body) == {"job_id": str(job_id)}
        assert result.republished_jobs == 1
    finally:
        await queue.delete(if_unused=False, if_empty=False)
        await channel.close()
        await connection.close()
        if job_id is not None:
            async with session_factory() as cleanup_session:
                await cleanup_session.execute(delete(CrawlJob).where(CrawlJob.id == job_id))
                await cleanup_session.commit()
