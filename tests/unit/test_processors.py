from __future__ import annotations

from io import BytesIO
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from PIL import Image
from pypdf import PdfWriter

from crawler.services.processors.html import HtmlProcessor


def test_html_processor_extracts_title_and_link_count() -> None:
    body = (
        b"<html><head><title>Hello</title></head><body>"
        b"<a href='/a'>A</a><a href='https://other.com'>B</a>"
        b"</body></html>"
    )

    result = HtmlProcessor().extract_metadata(body, {"Content-Type": "text/html"})

    assert result["title"] == "Hello"
    assert result["discovered_link_count"] == 2


def test_image_processor_extracts_dimensions() -> None:
    from crawler.services.processors.images import ImageProcessor

    buffer = BytesIO()
    Image.new("RGB", (32, 16)).save(buffer, format="PNG")

    result = ImageProcessor().extract_metadata(
        buffer.getvalue(), {"Content-Type": "image/png"}
    )

    assert result["width"] == 32
    assert result["height"] == 16


def test_pdf_processor_extracts_page_count_and_title() -> None:
    from crawler.services.processors.pdfs import PdfProcessor

    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.add_metadata({"/Title": "Quarterly report"})
    buffer = BytesIO()
    writer.write(buffer)

    result = PdfProcessor().extract_metadata(
        buffer.getvalue(), {"Content-Type": "application/pdf"}
    )

    assert result == {"page_count": 1, "title": "Quarterly report"}


def test_video_processor_extracts_file_size_when_duration_is_unavailable() -> None:
    from crawler.services.processors.videos import VideoProcessor

    body = b"not a video"

    result = VideoProcessor().extract_metadata(body, {"Content-Type": "video/mp4"})

    assert result == {"file_size": len(body), "duration": None}


def test_registry_selects_processor_from_media_type_without_parameters() -> None:
    from crawler.services.processors.html import HtmlProcessor
    from crawler.services.processors.registry import ProcessorRegistry

    processor = ProcessorRegistry([HtmlProcessor()]).processor_for(
        "text/html; charset=utf-8"
    )

    assert isinstance(processor, HtmlProcessor)


def test_artifact_storage_builds_deterministic_content_type_path(tmp_path: Path) -> None:
    from crawler.services.storage import ArtifactStorage

    storage = ArtifactStorage(root=tmp_path / "output")

    path = storage.build_storage_path(
        content_type="image/png",
        normalized_url="https://example.com/logo",
        content_hash="body-hash",
    )

    assert path == (
        tmp_path
        / "output"
        / "images"
        / "ea391e953aee5f58509583c40c84c1c9dfa40e22ea65028ffa7c406490f4c9b1-body-hash"
    )


@pytest.mark.anyio
async def test_artifact_storage_persists_body_and_metadata(tmp_path: Path) -> None:
    from crawler.services.storage import ArtifactStorage

    storage = ArtifactStorage(root=tmp_path / "output")
    job_id = uuid4()
    crawl_url_id = uuid4()

    artifact = await storage.persist_artifact(
        job_id=job_id,
        crawl_url_id=crawl_url_id,
        content_type="text/html",
        url="https://example.com/page",
        body=b"<html></html>",
        headers={"ETag": "etag-value", "Last-Modified": "yesterday"},
    )
    metadata = await storage.persist_metadata(
        artifact_id=artifact.id,
        metadata_type="html",
        metadata={"title": "Hello"},
    )

    assert Path(artifact.storage_path).read_bytes() == b"<html></html>"
    assert artifact.job_id == job_id
    assert artifact.crawl_url_id == crawl_url_id
    assert artifact.content_hash == "b633a587c652d02386c4f16f8c6f6aab7352d97f16367c3c40576214372dd628"
    assert artifact.etag == "etag-value"
    assert metadata.artifact_id == artifact.id
    assert metadata.metadata_json == {"title": "Hello"}


@pytest.mark.anyio
async def test_artifact_storage_preserves_lowercase_validator_headers(tmp_path: Path) -> None:
    from crawler.services.storage import ArtifactStorage

    artifact = await ArtifactStorage(root=tmp_path / "output").persist_artifact(
        job_id=uuid4(),
        crawl_url_id=uuid4(),
        content_type="text/html",
        url="https://example.com/page",
        body=b"<html></html>",
        headers={"etag": "lowercase-etag", "last-modified": "yesterday"},
    )

    assert artifact.etag == "lowercase-etag"
    assert artifact.last_modified == "yesterday"


@pytest.mark.anyio
async def test_fetch_client_returns_validated_fetch_response() -> None:
    from crawler.services.fetch_client import FetchClient

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fetch"
        assert request.url.params == httpx.QueryParams({"url": "https://example.com/page"})
        return httpx.Response(
            200,
            json={
                "status_code": 200,
                "headers": {"Content-Type": "text/html"},
                "body": "<html></html>",
            },
        )

    async with httpx.AsyncClient(
        base_url="http://mock-api.mock.com", transport=httpx.MockTransport(handler)
    ) as http_client:
        response = await FetchClient(http_client=http_client).fetch("https://example.com/page")

    assert response.status_code == 200
    assert response.headers == {"Content-Type": "text/html"}
    assert response.body == b"<html></html>"
