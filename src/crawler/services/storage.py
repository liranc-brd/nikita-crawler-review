from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.models.artifacts import ContentArtifact
from crawler.db.models.metadata import ContentMetadata
from crawler.domain.url_policy import normalize_url


@dataclass(frozen=True)
class StagedArtifact:
    artifact: ContentArtifact
    staging_path: Path


class ArtifactStorage:
    def __init__(self, *, root: Path = Path("output"), session: AsyncSession | None = None) -> None:
        self._root = root
        self._session = session

    def build_storage_path(
        self, *, content_type: str, normalized_url: str, content_hash: str
    ) -> Path:
        bucket = self._bucket_for_content_type(content_type)
        url_hash = hashlib.sha256(normalized_url.encode()).hexdigest()
        return self._root / bucket / f"{url_hash}-{content_hash}"

    @property
    def root(self) -> Path:
        return self._root

    async def persist_artifact(
        self,
        *,
        job_id: UUID,
        crawl_url_id: UUID,
        content_type: str,
        url: str,
        body: bytes,
        headers: dict[str, str],
    ) -> ContentArtifact:
        normalized_url = normalize_url(url)
        content_hash = hashlib.sha256(body).hexdigest()
        headers_by_name = {name.lower(): value for name, value in headers.items()}
        storage_path = self.build_storage_path(
            content_type=content_type,
            normalized_url=normalized_url,
            content_hash=content_hash,
        )
        await asyncio.to_thread(self._write_body, storage_path, body)
        artifact = ContentArtifact(
            job_id=job_id,
            crawl_url_id=crawl_url_id,
            content_type=content_type,
            storage_path=str(storage_path),
            filename=storage_path.name,
            content_length=len(body),
            content_hash=content_hash,
            etag=headers_by_name.get("etag"),
            last_modified=headers_by_name.get("last-modified"),
        )
        if self._session is not None:
            self._session.add(artifact)
            await self._session.flush()
        return artifact

    async def stage_artifact(
        self,
        *,
        job_id: UUID,
        crawl_url_id: UUID,
        content_type: str,
        url: str,
        body: bytes,
        headers: dict[str, str],
    ) -> StagedArtifact:
        normalized_url = normalize_url(url)
        content_hash = hashlib.sha256(body).hexdigest()
        headers_by_name = {name.lower(): value for name, value in headers.items()}
        final_path = self.build_storage_path(
            content_type=content_type,
            normalized_url=normalized_url,
            content_hash=content_hash,
        )
        staging_path = final_path.with_name(
            f"{final_path.name}.staging-{job_id.hex}-{crawl_url_id.hex}"
        )
        await asyncio.to_thread(self._write_body, staging_path, body)
        artifact = ContentArtifact(
            job_id=job_id,
            crawl_url_id=crawl_url_id,
            content_type=content_type,
            storage_path=str(final_path),
            filename=final_path.name,
            content_length=len(body),
            content_hash=content_hash,
            etag=headers_by_name.get("etag"),
            last_modified=headers_by_name.get("last-modified"),
        )
        return StagedArtifact(artifact=artifact, staging_path=staging_path)

    async def promote_staged_artifact(self, staged_artifact: StagedArtifact) -> bool:
        final_path = Path(staged_artifact.artifact.storage_path)
        return await asyncio.to_thread(
            self._promote_body,
            staged_artifact.staging_path,
            final_path,
        )

    async def lock_storage_path(self, *, storage_path: str) -> None:
        if self._session is None:
            raise RuntimeError("storage path locking requires a database session")
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": f"artifact-path:{storage_path}"},
        )

    async def delete_staged_artifact(self, staged_artifact: StagedArtifact) -> None:
        await asyncio.to_thread(self._delete_body, staged_artifact.staging_path)

    async def delete_persisted_artifact(self, *, storage_path: str) -> None:
        await asyncio.to_thread(self._delete_body, Path(storage_path))

    async def persist_metadata(
        self,
        *,
        artifact_id: UUID,
        metadata_type: str,
        metadata: dict[str, Any],
    ) -> ContentMetadata:
        content_metadata = ContentMetadata(
            artifact_id=artifact_id,
            metadata_type=metadata_type,
            metadata_json=metadata,
        )
        if self._session is not None:
            self._session.add(content_metadata)
            await self._session.flush()
        return content_metadata

    @staticmethod
    def _write_body(path: Path, body: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)

    @staticmethod
    def _delete_body(path: Path) -> None:
        path.unlink(missing_ok=True)

    @staticmethod
    def _promote_body(staging_path: Path, final_path: Path) -> bool:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            staging_path.unlink(missing_ok=True)
            return False
        os.replace(staging_path, final_path)
        return True

    @staticmethod
    def _bucket_for_content_type(content_type: str) -> str:
        media_type = content_type.split(";", maxsplit=1)[0].strip().lower()
        if media_type == "text/html":
            return "html"
        if media_type.startswith("image/"):
            return "images"
        if media_type.startswith("video/"):
            return "videos"
        if media_type == "application/pdf":
            return "pdfs"
        raise ValueError(f"unsupported content type: {media_type}")
