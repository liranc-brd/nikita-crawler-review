from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def async_session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(
        "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler"
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.anyio
async def test_job_schema_persists_seed_job(async_session) -> None:
    from crawler.db.models.jobs import CrawlJob, JobStatus

    job = CrawlJob(
        seed_url="https://example.com",
        seed_hostname="example.com",
        status=JobStatus.PENDING,
        config={"max_attempts": 3, "child_rules": []},
    )
    async_session.add(job)
    await async_session.commit()
    await async_session.refresh(job)

    assert job.id is not None
    assert job.seed_hostname == "example.com"

    await async_session.delete(job)
    await async_session.commit()
