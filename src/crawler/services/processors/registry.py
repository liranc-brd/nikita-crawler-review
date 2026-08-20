from __future__ import annotations

from collections.abc import Iterable

from crawler.services.processors.base import ContentProcessor
from crawler.services.processors.html import HtmlProcessor
from crawler.services.processors.images import ImageProcessor
from crawler.services.processors.pdfs import PdfProcessor
from crawler.services.processors.videos import VideoProcessor


class ProcessorRegistry:
    def __init__(self, processors: Iterable[ContentProcessor] | None = None) -> None:
        self._processors = tuple(
            processors
            if processors is not None
            else (HtmlProcessor(), ImageProcessor(), PdfProcessor(), VideoProcessor())
        )

    def processor_for(self, content_type: str) -> ContentProcessor:
        media_type = content_type.split(";", maxsplit=1)[0].strip().lower()
        for processor in self._processors:
            if media_type in processor.content_types:
                return processor
        raise ValueError(f"unsupported content type: {media_type}")
