from __future__ import annotations

from io import BytesIO
from typing import Any

from PIL import Image


class ImageProcessor:
    content_types = ("image/jpeg", "image/png", "image/gif", "image/webp")
    metadata_type = "image"

    def extract_metadata(self, body: bytes, headers: dict[str, str]) -> dict[str, Any]:
        with Image.open(BytesIO(body)) as image:
            return {
                "width": image.width,
                "height": image.height,
                "file_size": len(body),
            }
