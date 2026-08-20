from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.config import Settings
from crawler.db.session import async_session_factory
from crawler.repos.jobs import JobRepository
from crawler.repos.urls import UrlRepository


async def get_session() -> AsyncIterator[AsyncSession]:
    session_factory = async_session_factory(Settings())
    engine = session_factory.kw["bind"]
    try:
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    finally:
        await engine.dispose()


async def get_job_repository(
    session: AsyncSession = Depends(get_session),
) -> JobRepository:
    return JobRepository(session)


async def get_url_repository(
    session: AsyncSession = Depends(get_session),
) -> UrlRepository:
    return UrlRepository(session)
