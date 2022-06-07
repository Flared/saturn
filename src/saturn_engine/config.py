from .config_definitions import Env
from .config_definitions import RabbitMQConfig
from .config_definitions import RedisConfig
from .config_definitions import SaturnConfig
from .config_definitions import ServicesManagerConfig
from .config_definitions import WorkerConfig
from .config_definitions import WorkerManagerConfig
from .default_config import client_config as default_client_config
from .default_config import config as default_config
from .utils.config import Config as _Config


class Config(_Config[SaturnConfig]):
    def __init__(self) -> None:
        super().__init__()
        self._interfaces[""] = SaturnConfig


def default_config_with_env() -> Config:
    return Config().load_object(default_config).load_envvar("SATURN_SETTINGS")


def default_client_config_with_env() -> Config:
    return (
        Config()
        .load_object(default_client_config)
        .load_envvar("SATURN_CLIENT_SETTINGS")
    )


__all__ = (
    "Config",
    "SaturnConfig",
    "WorkerConfig",
    "WorkerManagerConfig",
    "RabbitMQConfig",
    "RedisConfig",
    "ServicesManagerConfig",
    "Env",
)
