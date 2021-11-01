import os
from functools import cached_property

from saturn_engine.utils import lazy

from .declarative import StaticDefinitions
from .declarative import load_definitions_from_directory


class Configuration:
    @cached_property
    def flask_host(self) -> str:
        return os.environ.get("SATURN_FLASK_HOST", "127.0.0.1")

    @cached_property
    def flask_port(self) -> int:
        return int(os.environ.get("SATURN_FLASK_PORT", 5000))

    @cached_property
    def database_url(self) -> str:
        database_url: str = os.environ.get("SATURN_DATABASE_URL", "sqlite:///test.db")
        return database_url

    @cached_property
    def async_database_url(self) -> str:
        database_url: str = self.database_url
        database_url = database_url.replace("sqlite:/", "sqlite+aiosqlite:/")
        database_url = database_url.replace("postgresql:/", "postgresql+asyncpg:/")
        return database_url

    @cached_property
    def static_definitions_directory(self) -> str:
        return os.environ.get(
            "SATURN_STATIC_DEFINITIONS_DIR",
            "/opt/saturn/definitions",
        )

    @cached_property
    def static_definitions(self) -> StaticDefinitions:
        """
        Static definitions contain objects defined in a declarative configuration:
        - Inventories
        - Topics
        - Jobs
        - JobDefinitions
        """
        return load_definitions_from_directory(self.static_definitions_directory)


@lazy()
def config() -> Configuration:
    return Configuration()
