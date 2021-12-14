from .config_definitions import Env
from .config_definitions import RabbitMQConfig
from .config_definitions import RayConfig
from .config_definitions import SaturnConfig
from .config_definitions import WorkerConfig
from .default_config import config as default_config
from .utils.config import Config as _Config


class Config(_Config[SaturnConfig]):
    def __init__(self) -> None:
        super().__init__(SaturnConfig)


__all__ = (
    "Config",
    "SaturnConfig",
    "WorkerConfig",
    "RabbitMQConfig",
    "RayConfig",
    "Env",
    "default_config",
)
