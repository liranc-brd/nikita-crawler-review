from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crawler.db.base import Base


class CrawlAttempt(Base):
    __tablename__ = "crawl_attempts"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    crawl_url_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("crawl_urls.id", ondelete="CASCADE"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime]
    finished_at: Mapped[datetime | None]
    result_status: Mapped[str]
    http_status_code: Mapped[int | None]
    retry_after_seconds: Mapped[int | None]
    response_headers: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error_detail: Mapped[str | None]
