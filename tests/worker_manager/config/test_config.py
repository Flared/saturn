from saturn_engine.worker_manager.config import Configuration


def test_get_async_postgresql_url() -> None:
    assert (
        Configuration._get_async_postgresql_url("postgresql://uu:pp@postgresql/saturn")
        == "postgresql+asyncpg://uu:pp@postgresql/saturn?statement_cache_size=0"
    )
