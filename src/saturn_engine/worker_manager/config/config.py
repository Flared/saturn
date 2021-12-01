import os
import urllib.parse
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
        if "postgresql:/" in database_url:
            return self._get_async_postgresql_url(database_url)
        elif "sqlite:/" in database_url:
            return database_url.replace("sqlite:/", "sqlite+aiosqlite:/")
        return database_url

    @staticmethod
    def _get_async_postgresql_url(postgresql_url: str) -> str:
        async_postgresql_url = postgresql_url.replace(
            "postgresql:/", "postgresql+asyncpg:/"
        )

        # Disable prepared statements to support pgbouncer.
        # https://magicstack.github.io/asyncpg/current/faq.html#why-am-i-getting-prepared-statement-errors
        scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(
            async_postgresql_url
        )
        query_params = urllib.parse.parse_qs(query_string)
        query_params["statement_cache_size"] = ["0"]
        new_query_string = urllib.parse.urlencode(query_params, doseq=True)
        async_postgresql_url = urllib.parse.urlunsplit(
            (scheme, netloc, path, new_query_string, fragment)
        )

        return async_postgresql_url

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
