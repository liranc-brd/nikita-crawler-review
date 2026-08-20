from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, ForeignKeyConstraint, Integer, func
from sqlalchemy.orm import Mapped, mapped_column

from crawler.db.base import Base


class ContentArtifact(Base):
    __tablename__ = "content_artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["crawl_url_id", "job_id"],
            ["crawl_urls.id", "crawl_urls.job_id"],
            name="fk_content_artifacts_crawl_url_job_id",
            ondelete="CASCADE",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    crawl_url_id: Mapped[uuid.UUID] = mapped_column(unique=True)
    content_type: Mapped[str]
    storage_path: Mapped[str]
    filename: Mapped[str]
    content_length: Mapped[int | None] = mapped_column(Integer)
    content_hash: Mapped[str]
    etag: Mapped[str | None]
    last_modified: Mapped[str | None]
    saved_at: Mapped[datetime] = mapped_column(server_default=func.now())
