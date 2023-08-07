import typing as t

import asyncio
import json
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from selectors import DefaultSelector
from unittest import mock
from unittest.mock import AsyncMock
from unittest.mock import Mock

import aiohttp
import yarl
from freezegun.api import FrozenDateTimeFactory

from saturn_engine.worker.services import Services


# From https://github.com/spulec/freezegun/issues/290
class TimeForwardSelector(DefaultSelector):
    def __init__(
        self,
        *,
        on_idle: t.Callable[[], None],
        frozen_time: FrozenDateTimeFactory,
    ) -> None:
        super().__init__()
        self._current_time: float = 0
        self._on_idle = on_idle
        self.forward_time = True
        self.frozen_time = frozen_time

    def select(self, timeout: t.Optional[float] = None) -> t.Any:
        select_timeout = timeout
        if self.forward_time:
            time_delta = timeout or 0
            self._current_time += time_delta
            self.frozen_time.tick(time_delta)
            select_timeout = 0

        events = super().select(select_timeout)

        if not events and timeout is None:
            self._on_idle()
        return events


class TimeForwardLoop(asyncio.SelectorEventLoop):  # type: ignore
    _selector: TimeForwardSelector

    def __init__(self, frozen_time: FrozenDateTimeFactory, freezer: t.Any) -> None:
        super().__init__(
            selector=TimeForwardSelector(
                on_idle=self.on_idle,
                frozen_time=frozen_time,
            ),
        )
        self.freezer = freezer
        self._idled: t.Optional[asyncio.Event] = None

    @property
    def forward_time(self) -> bool:
        return self._selector.forward_time

    @forward_time.setter
    def forward_time(self, value: bool) -> None:
        if not value:
            self.freezer.stop()
        self._selector.forward_time = value

    def time(self) -> float:
        if self.forward_time:
            return self._selector._current_time
        return super().time()

    def on_idle(self) -> None:
        self.idled.set()

    @property
    def idled(self) -> asyncio.Event:
        if self._idled is None:
            self._idled = asyncio.Event()
        return self._idled

    async def wait_idle(self) -> None:
        await self.idled.wait()
        self.idled.clear()

    @asynccontextmanager
    async def until_idle(self) -> AsyncIterator[None]:
        self.idled.clear()
        yield
        await self.wait_idle()


class FakeHttpClient:
    def __init__(
        self, *, responses: dict[str, dict[str, Mock]], loop: asyncio.AbstractEventLoop
    ):
        self.loop = loop
        self.responses = responses

        self.get = Mock(side_effect=self.response_context("get"))
        self.post = Mock(side_effect=self.response_context("post"))
        self.put = Mock(side_effect=self.response_context("put"))

    def response_context(self, method: str) -> t.Callable:
        @asynccontextmanager
        async def request(url: str, *args: t.Any, **kwargs: t.Any) -> AsyncIterator:
            response = self.responses[method][url](*args, **kwargs)
            obj = self.make_response(method, url, response)
            yield obj

        return request

    def make_response(
        self, method: str, url: str, response: t.Any
    ) -> aiohttp.ClientResponse:
        if isinstance(response, aiohttp.ClientResponse):
            return response
        elif isinstance(response, dict):
            return self.make_json_response(method=method, url=url, response=response)
        elif isinstance(response, str):
            return self.make_text_response(method=method, url=url, response=response)
        elif isinstance(response, bytes):
            return self.make_raw_response(method=method, url=url, response=response)
        raise ValueError(f"Invalid response type: {response.__class__}")

    def make_json_response(
        self,
        *,
        method: str,
        url: str,
        response: dict,
        headers: t.Optional[dict[str, str]] = None,
        status: int = 200,
        reason: str = "OK",
    ) -> aiohttp.ClientResponse:
        return self.make_text_response(
            method=method,
            url=url,
            response=json.dumps(response),
            headers={"Content-Type": "application/json"} | (headers or {}),
            status=status,
            reason=reason,
        )

    def make_text_response(
        self,
        *,
        method: str,
        url: str,
        response: str,
        headers: t.Optional[dict[str, str]] = None,
        status: int = 200,
        reason: str = "OK",
    ) -> aiohttp.ClientResponse:
        return self.make_raw_response(
            method=method,
            url=url,
            response=response.encode(),
            headers=headers,
            status=status,
            reason=reason,
        )

    def make_raw_response(
        self,
        *,
        method: str,
        url: str,
        response: bytes,
        headers: t.Optional[dict[str, str]] = None,
        status: int = 200,
        reason: str = "OK",
    ) -> aiohttp.ClientResponse:
        response_obj = aiohttp.ClientResponse(
            method,
            yarl.URL(url),
            request_info=mock.Mock(),
            writer=None,  # type: ignore
            continue100=None,
            timer=None,  # type: ignore
            traces=[],
            loop=self.loop,
            session=None,  # type: ignore
        )
        response_obj._body = response
        response_obj._headers = headers or {}  # type: ignore
        response_obj.status = status
        response_obj.reason = reason
        return response_obj


class HttpClientMock:
    def __init__(self, *, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.responses: dict[str, dict[str, Mock]] = {
            "get": {},
            "post": {},
            "put": {},
        }

    def get(self, path: str) -> Mock:
        return self.set_mock("get", path)

    def put(self, path: str) -> Mock:
        return self.set_mock("put", path)

    def post(self, path: str) -> Mock:
        return self.set_mock("post", path)

    def set_mock(self, method: str, path: str) -> Mock:
        self.responses[method].setdefault(path, Mock())
        return self.responses[method][path]

    def reset_mock(self) -> None:
        for mocks in self.responses.values():
            for req_mock in mocks.values():
                req_mock.reset_mock()

    def client(self) -> aiohttp.ClientSession:
        return t.cast(
            aiohttp.ClientSession,
            FakeHttpClient(responses=self.responses, loop=self.loop),
        )


def async_context_mock_handler(
    mock: AsyncMock,
) -> t.Callable[[t.Any], AsyncGenerator[None, t.Any]]:
    async def scope(event: t.Any) -> AsyncGenerator[None, t.Any]:
        await mock.before(event)
        try:
            result = yield
            await mock.success(event, result)
        except Exception as e:
            await mock.errors(event, e)

    return scope


def register_hooks_handler(services: Services) -> AsyncMock:
    _hooks_handler = AsyncMock()
    services.s.hooks.message_polled.register(_hooks_handler.message_polled)
    services.s.hooks.message_scheduled.register(_hooks_handler.message_scheduled)
    services.s.hooks.message_submitted.register(_hooks_handler.message_submitted)
    services.s.hooks.message_executed.register(
        async_context_mock_handler(_hooks_handler.message_executed)
    )
    services.s.hooks.message_published.register(
        async_context_mock_handler(_hooks_handler.message_published)
    )
    return _hooks_handler


class EqualAny:
    def __eq__(self, other: t.Any) -> bool:
        return True


class EqualAnyOrder:
    def __init__(self, expected: t.Iterable):
        self.expected = expected

    def __eq__(self, other: t.Any) -> bool:
        return list(sorted(self.expected)) == list(sorted(other))
