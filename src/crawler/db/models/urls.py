from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Enum, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from crawler.db.base import Base
from crawler.db.models.enums import UrlStatus


class CrawlUrl(Base):
    __tablename__ = "crawl_urls"
    __table_args__ = (
        UniqueConstraint("job_id", "normalized_url", name="uq_crawl_urls_job_id_normalized_url"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    normalized_url: Mapped[str]
    url_hash: Mapped[str]
    discovered_from_url_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("crawl_urls.id", ondelete="SET NULL")
    )
    status: Mapped[UrlStatus] = mapped_column(
        Enum(UrlStatus, name="url_status", values_callable=lambda values: [value.value for value in values]),
        default=UrlStatus.DISCOVERED,
    )
    content_type: Mapped[str | None]
    http_status_code: Mapped[int | None]
    fetch_attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_eligible_at: Mapped[datetime | None]
    claimed_by: Mapped[str | None]
    claimed_at: Mapped[datetime | None]
    lease_expires_at: Mapped[datetime | None]
    last_heartbeat_at: Mapped[datetime | None]
    started_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    content_artifact_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            "content_artifacts.id",
            name="fk_crawl_urls_content_artifact_id",
            ondelete="SET NULL",
            use_alter=True,
        ),
        unique=True,
    )
    error_code: Mapped[str | None]
    error_detail: Mapped[str | None]
