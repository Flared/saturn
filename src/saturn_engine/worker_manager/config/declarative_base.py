import dataclasses


@dataclasses.dataclass
class ObjectMetadata:
    name: str


@dataclasses.dataclass
class BaseObject:
    metadata: ObjectMetadata
    apiVersion: str
    kind: str
