"""Shared test fixtures.

Unit tests need no infrastructure at all.

Integration tests need a Postgres and a Redis, and get one of three ways, in
order of preference:

1. **Something already reachable** at `TEST_DATABASE_URL` / `TEST_REDIS_URL` --
   typically `docker compose up -d postgres redis`. Fastest, because nothing has
   to start.
2. **Ephemeral containers**, started automatically via testcontainers when
   nothing is reachable. `pytest` then works on its own, against a throwaway
   database that cannot collide with a locally installed PostgreSQL or clobber
   development data.
3. **Skipped**, when there is no Docker daemon either -- so `pytest` is always
   runnable and never fails merely for want of infrastructure.

Set `TEST_FORCE_TESTCONTAINERS=1` to always take path 2, which is what you want
when you care about isolation more than speed.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Base

DEFAULT_DATABASE_URL = "postgresql+asyncpg://bulk:bulk@localhost:5433/bulk_actions_test"
DEFAULT_REDIS_URL = "redis://localhost:6380/15"

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", DEFAULT_DATABASE_URL)
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", DEFAULT_REDIS_URL)
FORCE_TESTCONTAINERS = os.getenv("TEST_FORCE_TESTCONTAINERS", "").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# Deciding where the infrastructure comes from
# ---------------------------------------------------------------------------


def _reachable(url: str, default_port: int) -> bool:
    parsed = urlparse(url.replace("postgresql+asyncpg", "postgresql"))
    try:
        with socket.create_connection(
            (parsed.hostname or "localhost", parsed.port or default_port), timeout=1.5
        ):
            return True
    except OSError:
        return False


def _existing_infra_reachable() -> bool:
    return _reachable(TEST_DATABASE_URL, 5432) and _reachable(TEST_REDIS_URL, 6379)


def _testcontainers_usable() -> bool:
    """True when testcontainers is installed *and* a Docker daemon answers."""
    try:
        import docker  # noqa: F401  (testcontainers dependency)
        from testcontainers.core.docker_client import DockerClient
    except ImportError:
        return False
    try:
        DockerClient().client.ping()
        return True
    except Exception:
        return False


USE_TESTCONTAINERS = FORCE_TESTCONTAINERS or (
    not _existing_infra_reachable() and _testcontainers_usable()
)
INTEGRATION_AVAILABLE = USE_TESTCONTAINERS or _existing_infra_reachable()

requires_infra = pytest.mark.skipif(
    not INTEGRATION_AVAILABLE,
    reason=(
        "No Postgres/Redis and no Docker daemon. Run `docker compose up -d postgres redis`, "
        "or install `testcontainers` and start Docker."
    ),
)


@dataclass(frozen=True)
class Infra:
    database_url: str
    redis_url: str


@pytest.fixture(scope="session")
def infra():
    """Resolve the infrastructure the integration tests will run against."""
    if not INTEGRATION_AVAILABLE:
        pytest.skip("infrastructure unavailable")

    if not USE_TESTCONTAINERS:
        yield Infra(TEST_DATABASE_URL, TEST_REDIS_URL)
        return

    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer

    # Pinned to the same images docker-compose runs, so the tests exercise the
    # versions the application is actually deployed against.
    postgres = PostgresContainer(
        "postgres:16-alpine", username="bulk", password="bulk", dbname="bulk_actions_test"
    )
    redis = RedisContainer("redis:7-alpine")
    postgres.start()
    redis.start()

    database_url = postgres.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql+asyncpg://"
    )
    redis_url = f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"

    try:
        yield Infra(database_url, redis_url)
    finally:
        redis.stop()
        postgres.stop()


# ---------------------------------------------------------------------------
# Schema and connections
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _schema(infra: Infra) -> None:
    """Create the test database and its schema, once per run.

    Synchronous on purpose: a session-scoped *async* fixture would need a
    session-scoped event loop shared by every function-scoped test. Doing the
    one-off DDL over a sync driver keeps the async fixtures per-test and the
    loop scoping trivial.
    """
    import psycopg
    from sqlalchemy import create_engine

    parsed = urlparse(infra.database_url.replace("postgresql+asyncpg", "postgresql"))
    dbname = (parsed.path or "/postgres").lstrip("/")

    # A testcontainer already created the database; an externally supplied one
    # may not have.
    if not USE_TESTCONTAINERS:
        with psycopg.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=parsed.username,
            password=parsed.password,
            dbname="postgres",
            autocommit=True,
        ) as admin, admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{dbname}"')

    sync_engine = create_engine(infra.database_url.replace("+asyncpg", "+psycopg"))
    try:
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
    finally:
        sync_engine.dispose()


@pytest_asyncio.fixture
async def engine(infra: Infra, _schema):
    eng = create_async_engine(infra.database_url)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine) -> AsyncIterator[AsyncSession]:
    """A clean database per test."""
    async with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as s:
        yield s
        await s.rollback()


@pytest_asyncio.fixture
async def redis(infra: Infra):
    from redis.asyncio import Redis

    client = Redis.from_url(infra.redis_url, decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


# ---------------------------------------------------------------------------
# Test doubles and data
# ---------------------------------------------------------------------------


class FakeArqPool:
    """Records enqueued jobs so a test can drive the pipeline deterministically.

    Running a real arq worker inside the test suite would trade determinism for
    nothing: the tasks are plain coroutines, so a test can invoke them directly
    and still exercise every line of the production path.
    """

    def __init__(self) -> None:
        self.jobs: list[dict[str, Any]] = []

    async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> None:
        job_id = kwargs.get("_job_id")
        # Mirror arq's behaviour: a repeated job id is dropped.
        if job_id and any(j["job_id"] == job_id for j in self.jobs):
            return None
        self.jobs.append(
            {
                "function": function,
                "args": args,
                "job_id": job_id,
                "defer_until": kwargs.get("_defer_until"),
                "defer_by": kwargs.get("_defer_by"),
            }
        )
        return None

    def pop(self, function: str) -> list[dict[str, Any]]:
        matching = [j for j in self.jobs if j["function"] == function]
        self.jobs = [j for j in self.jobs if j["function"] != function]
        return matching


@pytest.fixture
def arq() -> FakeArqPool:
    return FakeArqPool()


@pytest_asyncio.fixture
async def account(session) -> Any:
    from app.models.account import Account

    acc = Account(id=uuid.uuid4(), name="Acme Logistics", rate_limit_per_minute=10_000)
    session.add(acc)
    await session.commit()
    return acc
