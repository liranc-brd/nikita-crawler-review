from __future__ import annotations

import json
from uuid import UUID

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue


JOB_WAKEUP_QUEUE = "crawler.job_wakeups"


async def declare_job_wakeup_queue(
    channel: AbstractChannel,
    *,
    queue_name: str = JOB_WAKEUP_QUEUE,
) -> AbstractQueue:
    return await channel.declare_queue(queue_name, durable=True)


class RabbitPublisher:
    def __init__(self, channel: AbstractChannel, *, queue_name: str = JOB_WAKEUP_QUEUE) -> None:
        self._channel = channel
        self._queue_name = queue_name

    async def publish_job_wakeup(self, job_id: UUID) -> None:
        await declare_job_wakeup_queue(self._channel, queue_name=self._queue_name)
        message = aio_pika.Message(
            body=json.dumps({"job_id": str(job_id)}).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self._channel.default_exchange.publish(message, routing_key=self._queue_name)
