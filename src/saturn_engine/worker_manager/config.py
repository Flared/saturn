import os
from functools import cached_property

from saturn_engine.utils import lazy


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
        return self.database_url.replace("sqlite:/", "sqlite+aiosqlite:/")


@lazy
def config() -> Configuration:
    return Configuration()
