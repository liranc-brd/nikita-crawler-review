from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import aio_pika

from crawler.config import Settings
from crawler.db.session import async_session_factory
from crawler.messaging.rabbitmq import RabbitPublisher
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository
from crawler.services.scheduler import ReconcileResult, SchedulerService


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


async def reconcile_once(now: datetime) -> ReconcileResult:
    settings = Settings()
    session_factory = async_session_factory(settings)
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        async with session_factory() as session:
            async with session.begin():
                result = await SchedulerService(
                    JobRepository(session),
                    UrlRepository(session, lease_duration_seconds=settings.lease_duration_seconds),
                    RabbitPublisher(channel),
                ).reconcile_once(now=now)
        logger.info(
            "scheduler reconciliation complete",
            extra={
                "expired_leases_released": result.expired_leases_released,
                "republished_jobs": result.republished_jobs,
                "paused_jobs": result.paused_jobs,
                "canceled_jobs": result.canceled_jobs,
                "completed_jobs": result.completed_jobs,
            },
        )
        return result
    finally:
        await connection.close()
        engine = session_factory.kw["bind"]
        await engine.dispose()


async def run_scheduler_worker() -> None:
    settings = Settings()
    while True:
        await reconcile_once(_utcnow())
        await asyncio.sleep(settings.scheduler_poll_interval_seconds)
