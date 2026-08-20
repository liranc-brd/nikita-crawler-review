from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from crawler.db.base import Base


class DiscoveredLink(Base):
    __tablename__ = "discovered_links"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_jobs.id", ondelete="CASCADE"))
    source_url_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_urls.id", ondelete="CASCADE"))
    target_normalized_url: Mapped[str]
    target_url_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("crawl_urls.id", ondelete="SET NULL")
    )
    is_same_hostname: Mapped[bool] = mapped_column(Boolean)
    spawned_child_job_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("crawl_jobs.id", ondelete="SET NULL")
    )
    discovered_at: Mapped[datetime] = mapped_column(server_default=func.now())
