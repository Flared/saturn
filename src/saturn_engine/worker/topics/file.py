from typing import TextIO
from typing import cast

import dataclasses
import json
import sys
from collections.abc import AsyncGenerator

from saturn_engine.core import TopicMessage

from . import Topic


class FileTopic(Topic):
    """A topic that loads or dumps its message into a file"""

    @dataclasses.dataclass
    class Options:
        path: str
        mode: str

    def __init__(self, options: Options, **kwargs: object) -> None:
        if options.path == "-":
            if options.mode == "r":
                self.fd = sys.stdin
            elif options.mode == "w":
                self.fd = sys.stdout
            else:
                raise ValueError(f"Unkown mode: {options.mode}")
        else:
            self.fd = cast(TextIO, open(options.path, options.mode))

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        for line in self.fd:
            data = json.loads(line)
            yield TopicMessage(id=data.get("id", None), args=data["args"])

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        self.fd.write(json.dumps({"id": message.id, "args": message.args}) + "\n")
        self.fd.flush()
        return True

    async def close(self) -> None:
        if self.fd in (sys.stdout, sys.stderr):
            return
        self.fd.close()
