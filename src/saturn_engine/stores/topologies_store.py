from saturn_engine.models.topology_patches import TopologyPatch
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.utils.sqlalchemy import AnySession
from saturn_engine.utils.sqlalchemy import upsert


def patch(*, session: AnySession, patch: BaseObject) -> TopologyPatch:
    topology_patch = TopologyPatch.from_topology(topology=patch)

    stmt = (
        upsert(session)(TopologyPatch)
        .values(topology_patch.get_insert_values())
        .execution_options(populate_existing=True)
        .on_conflict_do_update(
            index_elements=[
                TopologyPatch.kind,
                TopologyPatch.name,
            ],
            set_={
                TopologyPatch.data: topology_patch.data,  # type: ignore
            },
        )
    )
    session.execute(stmt)  # type: ignore
    return topology_patch
