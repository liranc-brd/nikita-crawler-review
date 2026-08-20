from __future__ import annotations

from typing import Any, Protocol


class ContentProcessor(Protocol):
    content_types: tuple[str, ...]
    metadata_type: str

    def extract_metadata(self, body: bytes, headers: dict[str, str]) -> dict[str, Any]: ...
