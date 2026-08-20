from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.models.discoveries import DiscoveredLink
from crawler.domain.url_policy import normalize_url


class DiscoveryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record_discovery(
        self,
        *,
        job_id: UUID,
        source_url_id: UUID,
        target_url: str,
        is_same_hostname: bool,
        target_url_id: UUID | None = None,
        spawned_child_job_id: UUID | None = None,
    ) -> DiscoveredLink:
        discovery = DiscoveredLink(
            job_id=job_id,
            source_url_id=source_url_id,
            target_normalized_url=normalize_url(target_url),
            target_url_id=target_url_id,
            is_same_hostname=is_same_hostname,
            spawned_child_job_id=spawned_child_job_id,
        )
        self._session.add(discovery)
        await self._session.flush()
        return discovery

    async def get_by_target(self, target_url: str) -> DiscoveredLink | None:
        return await self._session.scalar(
            select(DiscoveredLink).where(
                DiscoveredLink.target_normalized_url == normalize_url(target_url)
            )
        )
