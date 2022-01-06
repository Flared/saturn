import dataclasses

from saturn_engine.core import Resource


@dataclasses.dataclass
class TestApiKey(Resource):
    key: str
