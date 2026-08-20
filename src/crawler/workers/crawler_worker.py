from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
from datetime import UTC, datetime
from uuid import UUID

import aio_pika
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crawler.config import Settings
from crawler.db.session import async_session_factory
from crawler.messaging.rabbitmq import declare_job_wakeup_queue
from crawler.repos.attempts import AttemptRepository
from crawler.repos.discoveries import DiscoveryRepository
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository
from crawler.services.fetch_client import FetchClient
from crawler.services.orchestrator import CrawlOrchestrator
from crawler.services.processors.registry import ProcessorRegistry
from crawler.services.storage import ArtifactStorage


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


async def heartbeat_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    url_id: UUID,
    worker_id: str,
    interval_seconds: int,
    lease_duration_seconds: int,
) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        async with session_factory() as session:
            async with session.begin():
                extended = await UrlRepository(session).heartbeat_url(
                    url_id=url_id,
                    worker_id=worker_id,
                    lease_duration_seconds=lease_duration_seconds,
                    now=_utcnow(),
                )
        if not extended:
            return


async def run_crawler_worker() -> None:
    settings = Settings()
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    session_factory = async_session_factory(settings)
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)

    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await declare_job_wakeup_queue(channel)
        async with httpx.AsyncClient(base_url=settings.fetch_service_base_url) as http_client:
            async with queue.iterator() as consumer:
                async for message in consumer:
                    async with message.process(requeue=True):
                        await _process_wakeup(
                            session_factory=session_factory,
                            http_client=http_client,
                            settings=settings,
                            worker_id=worker_id,
                        )
    finally:
        await connection.close()
        engine = session_factory.kw["bind"]
        await engine.dispose()


async def _process_wakeup(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    settings: Settings,
    worker_id: str,
) -> None:
    async with session_factory() as session:
        urls = UrlRepository(session, lease_duration_seconds=settings.lease_duration_seconds)
        claimed = await urls.claim_runnable_urls(
            worker_id=worker_id,
            limit=settings.crawler_batch_size,
        )
        await session.commit()

    heartbeat_tasks = {
        row.id: asyncio.create_task(
            heartbeat_loop(
                session_factory=session_factory,
                url_id=row.id,
                worker_id=worker_id,
                interval_seconds=settings.heartbeat_interval_seconds,
                lease_duration_seconds=settings.lease_duration_seconds,
            )
        )
        for row in claimed
    }

    try:
        for row in claimed:
            try:
                async with session_factory() as processing_session:
                    orchestrator = CrawlOrchestrator(
                        session=processing_session,
                        fetch_client=FetchClient(http_client),
                        storage=ArtifactStorage(root=settings.artifact_root, session=processing_session),
                        processors=ProcessorRegistry(),
                        jobs=JobRepository(processing_session),
                        urls=UrlRepository(
                            processing_session,
                            lease_duration_seconds=settings.lease_duration_seconds,
                        ),
                        discoveries=DiscoveryRepository(processing_session),
                        attempts=AttemptRepository(processing_session),
                    )
                    await orchestrator.process_url(url_id=row.id, worker_id=worker_id)
            finally:
                heartbeat_task = heartbeat_tasks.pop(row.id)
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
    finally:
        for heartbeat_task in heartbeat_tasks.values():
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
    logger.info("processed crawler wakeup", extra={"claimed_urls": len(claimed)})


def main() -> None:
    from crawler.logging import configure_logging

    configure_logging()
    asyncio.run(run_crawler_worker())


if __name__ == "__main__":
    main()
