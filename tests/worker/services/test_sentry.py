import typing as t

import pytest
import sentry_sdk
import sentry_sdk.transport
from sentry_sdk.envelope import Envelope

from saturn_engine.config import Config
from saturn_engine.core import PipelineResults
from saturn_engine.core import TopicMessage
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.extras.sentry import Sentry
from saturn_engine.worker.services.manager import ServicesManager

if t.TYPE_CHECKING:
    from sentry_sdk._types import Event


class FakeTransport(sentry_sdk.transport.Transport):
    def __init__(self) -> None:
        super().__init__()
        self.events: list["Event"] = []
        self.envelopes: list[Envelope] = []
        self._queue = None

    def capture_event(self, event: "Event") -> None:
        self.events.append(event)

    def append_envelope(self, envelope: Envelope) -> None:
        for item in envelope:
            if item.headers.get("type") in ("event", "transaction"):
                assert item.payload.json
                self.capture_event(item.payload.json)


@pytest.fixture
def sentry_init(request: pytest.FixtureRequest) -> t.Iterator[t.Callable]:
    def inner(*a: t.Any, **kw: t.Any) -> None:
        kw.setdefault("transport", FakeTransport())
        hub = sentry_sdk.Hub.current
        client = sentry_sdk.Client(*a, **kw)
        hub.bind_client(client)

    if request.node.get_closest_marker("forked"):
        # Do not run isolation if the test is already running in
        # ultimate isolation (seems to be required for celery tests that
        # fork)
        yield inner
    else:
        with sentry_sdk.Hub(None):
            yield inner


@pytest.fixture
def config(config: Config) -> Config:
    return config.load_object(
        {
            "services_manager": {
                "services": [
                    "saturn_engine.worker.services.extras.sentry.Sentry",
                ]
            }
        }
    )


def captured_sentry_events() -> list[dict]:
    return sentry_sdk.Hub.current.client.transport.events  # type: ignore


@pytest.mark.asyncio
async def test_sentry(
    sentry_init: t.Any,
    services_manager: ServicesManager,
    executable_maker: t.Callable[..., ExecutableMessage],
) -> None:
    sentry_init()
    services_manager.services.cast_service(Sentry)
    xmsg = executable_maker(
        message=TopicMessage(args={}, config={"tracer": {"rate": 0.5}})
    )

    @services_manager.services.s.hooks.message_executed.emit
    async def scope(xmsg: ExecutableMessage) -> PipelineResults:
        raise ValueError("Test")

    await services_manager.services.s.hooks.message_polled.emit(xmsg)
    with pytest.raises(ValueError):
        await scope(xmsg)

    events = captured_sentry_events()
    assert len(events) == 1
    event = events[0]
    assert event["exception"]["values"][0]["type"] == "ValueError"
    assert (
        event["extra"]["saturn"]["pipeline_message"]["info"]["name"]
        == "tests.conftest.pipeline"
    )
    assert event["tags"]["saturn.job.name"] == "fake-queue"
    assert event["tags"]["saturn.job.labels.owner"] == "team-saturn"
