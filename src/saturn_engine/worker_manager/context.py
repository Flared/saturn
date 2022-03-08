from functools import cached_property

from saturn_engine.config import WorkerManagerConfig
from saturn_engine.worker_manager.config.declarative import filter_with_jobs_selector
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
        definitions = load_definitions_from_path(
            self.config.static_definitions_directory
        )
        if self.config.static_definitions_jobs_selector:
            definitions = filter_with_jobs_selector(
                definitions=definitions,
                selector=self.config.static_definitions_jobs_selector,
            )
        return definitions
