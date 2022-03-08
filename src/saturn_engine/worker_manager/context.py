from functools import cached_property

from saturn_engine.config import WorkerManagerConfig
from saturn_engine.worker_manager.config.declarative import load_definitions_from_path

from .config.static_definitions import StaticDefinitions


class WorkerManagerContext:
    """
    The Worker Manager context contains a loaded config
    and cached static definitions.
    """

    def __init__(self, config: WorkerManagerConfig) -> None:
        self.config: WorkerManagerConfig = config

    @cached_property
    def static_definitions(self) -> StaticDefinitions:
        """
        Static definitions contain objects defined in a declarative configuration:
        - Inventories
        - Topics
        - Jobs
        - JobDefinitions
        """
        return load_definitions_from_path(self.config.static_definitions_directory)
