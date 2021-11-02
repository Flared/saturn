import dataclasses
import time

from saturn_engine.core import Resource


@dataclasses.dataclass
class FoobarApiKey(Resource):
    key: str


def hello(who: str) -> None:
    print(f"hello {who}")


def foobar(api_key: FoobarApiKey) -> None:
    print(f"requested with {api_key.key}")
    time.sleep(5)
