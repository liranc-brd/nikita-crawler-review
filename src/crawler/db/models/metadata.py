from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from crawler.db.base import Base


class ContentMetadata(Base):
    __tablename__ = "content_metadata"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    artifact_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("content_artifacts.id", ondelete="CASCADE")
    )
    metadata_type: Mapped[str]
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB)
