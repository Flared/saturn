import asyncio
from collections.abc import Collection
from datetime import timedelta

import pytest

from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.utils import utcnow
from saturn_engine.worker.resources.provider import PeriodicSyncOptions
from saturn_engine.worker.resources.provider import PeriodicSyncProvider
from saturn_engine.worker.resources.provider import ProvidedResource
from saturn_engine.worker.resources.provider import StaticResourcesProvider
from saturn_engine.worker.services.manager import ServicesManager


@pytest.mark.asyncio
async def test_static_resources(services_manager: ServicesManager) -> None:
    options = StaticResourcesProvider.Options(
        resources=[
            ProvidedResource(name="test-1", data={"x": 1}),
            ProvidedResource(name="test-2", data={"x": 2}),
        ]
    )
    provider = StaticResourcesProvider(
        options=options,
        services=services_manager.services,
        definition=ResourcesProviderItem(
            name="test-provider",
            type="ResourcesProviderItem",
            resource_type="tests.resource",
            options={},
        ),
    )
    resources_manager = services_manager.services.s.resources_manager

    await provider._open()

    assert set(resources_manager.resources["tests.resource"].resources.keys()) == {
        "test-1",
        "test-2",
    }

    await provider._close()

    assert not resources_manager.resources["tests.resource"].resources


class FakePeriodicResourcesProvider(
    PeriodicSyncProvider["FakePeriodicResourcesProvider.Options"]
):
    resources: list[ProvidedResource]

    class Options(PeriodicSyncOptions):
        pass

    async def open(self) -> None:
        self.syncing = asyncio.Event()
        self.synced = asyncio.Event()
        self.synced.set()

    async def sync(self) -> Collection[ProvidedResource]:
        self.syncing.set()
        await self.synced.wait()
        self.syncing.clear()
        return self.resources


@pytest.mark.asyncio
async def test_periodic_sync_provider(services_manager: ServicesManager) -> None:
    start_date = utcnow()
    options = FakePeriodicResourcesProvider.Options(sync_interval=600)
    provider = FakePeriodicResourcesProvider(
        options=options,
        services=services_manager.services,
        definition=ResourcesProviderItem(
            name="test-provider",
            type="ResourcesProviderItem",
            resource_type="tests.resource",
            options={"x": 42},
        ),
    )
    resources_manager = services_manager.services.s.resources_manager
    provider.resources = [
        ProvidedResource(name="test-1", data={}),
        ProvidedResource(name="test-2", data={}),
    ]
    await provider._open()

    await provider.syncing.wait()
    provider.synced.set()
    await provider.syncing.wait()

    # By now we should have done at least one sync and sleep through the next
    # sync.
    assert set(resources_manager.resources["tests.resource"].resources.keys()) == {
        "test-1",
        "test-2",
    }
    assert utcnow() >= (start_date + timedelta(minutes=10))

    provider.resources = [
        ProvidedResource(name="test-2", data={}),
        ProvidedResource(name="test-3", data={}),
    ]

    provider.synced.set()
    await provider.syncing.wait()

    assert set(resources_manager.resources["tests.resource"].resources.keys()) == {
        "test-2",
        "test-3",
    }
    assert utcnow() >= (start_date + timedelta(minutes=20))

    await provider._close()
