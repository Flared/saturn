import typing as t

import sqlalchemy as sa
from sqlalchemy.orm import Mapped

from saturn_engine.models.compat import mapped_column
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.utils.options import asdict

from .base import Base


class TopologyPatch(Base):
    __tablename__ = "topology_patches"

    kind: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    name: Mapped[str] = mapped_column(sa.Text, primary_key=True)

    data: Mapped[dict[str, t.Any]] = mapped_column(sa.JSON, nullable=False)

    def __init__(self, kind: str, name: str, data: dict[str, t.Any]) -> None:
        self.kind = kind
        self.name = name
        self.data = data

    @classmethod
    def from_topology(cls, topology: BaseObject) -> "TopologyPatch":
        return cls(
            kind=topology.kind, name=topology.metadata.name, data=asdict(topology)
        )

    def as_base_object(self) -> BaseObject:
        return BaseObject(**self.data)

    class InsertValues(t.TypedDict):
        kind: str
        name: str
        data: dict[str, t.Any]

    def get_insert_values(self) -> "TopologyPatch.InsertValues":
        return TopologyPatch.InsertValues(
            kind=self.kind, name=self.name, data=self.data
        )
