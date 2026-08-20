from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Enum, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crawler.db.base import Base
from crawler.db.models.enums import JobStatus


class CrawlJob(Base):
    __tablename__ = "crawl_jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    seed_url: Mapped[str]
    seed_hostname: Mapped[str]
    parent_job_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("crawl_jobs.id", ondelete="SET NULL")
    )
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status", values_callable=lambda values: [value.value for value in values]),
        default=JobStatus.PENDING,
    )
    config: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    started_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    pause_requested_at: Mapped[datetime | None]
    cancel_requested_at: Mapped[datetime | None]
