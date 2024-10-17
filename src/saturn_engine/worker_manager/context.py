from saturn_engine.config import WorkerManagerConfig
from saturn_engine.stores import topologies_store
from saturn_engine.utils.sqlalchemy import AnySession
from saturn_engine.worker_manager.config.declarative import filter_with_jobs_selector
from saturn_engine.worker_manager.config.declarative import load_definitions_from_paths

from .config.static_definitions import StaticDefinitions


class WorkerManagerContext:
    """
    The Worker Manager context contains a loaded config
    and cached static definitions.
    """

    def __init__(self, config: WorkerManagerConfig) -> None:
        self.config: WorkerManagerConfig = config
        self._static_definitions: StaticDefinitions | None

    @property
    def static_definitions(self) -> StaticDefinitions:
        if not self._static_definitions:
            raise ValueError("Static definitions need to be set before usage")
        return self._static_definitions

    def load_static_definition(self, session: AnySession) -> None:
        self._static_definitions = _load_static_definition(
            session=session, config=self.config
        )


def _load_static_definition(
    config: WorkerManagerConfig, session: AnySession
) -> StaticDefinitions:
    """
    Static definitions contain objects defined in a declarative configuration:
    - Inventories
    - Topics
    - Jobs
    - JobDefinitions
    """
    patches = topologies_store.get_patches(session=session)
    definitions = load_definitions_from_paths(
        config.static_definitions_directories, patches=patches
    )

    if config.static_definitions_jobs_selector:
        definitions = filter_with_jobs_selector(
            definitions=definitions,
            selector=config.static_definitions_jobs_selector,
        )
    return definitions
