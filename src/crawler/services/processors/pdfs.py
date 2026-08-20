from __future__ import annotations

from io import BytesIO
from typing import Any

from pypdf import PdfReader


class PdfProcessor:
    content_types = ("application/pdf",)
    metadata_type = "pdf"

    def extract_metadata(self, body: bytes, headers: dict[str, str]) -> dict[str, Any]:
        reader = PdfReader(BytesIO(body))
        title = reader.metadata.title if reader.metadata else None
        return {"page_count": len(reader.pages), "title": title}
