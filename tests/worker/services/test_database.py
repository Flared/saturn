import pytest
from sqlalchemy import select

from saturn_engine.config import Config
from saturn_engine.worker.services.databases import Databases
from saturn_engine.worker.services.manager import ServicesManager


@pytest.fixture
def config(config: Config) -> Config:
    return config.load_object(
        {
            "databases": {
                "engines": {"default": "sqlite+aiosqlite://"},
                "sync_engines": {},
            }
        }
    )


@pytest.mark.asyncio
async def test_databases(services_manager: ServicesManager) -> None:
    databases_service = services_manager._load_service(Databases)
    await databases_service.open()
    async with databases_service.session() as session:
        assert await session.scalar(select(1)) == 1
