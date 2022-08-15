import pytest

from saturn_engine.core.api import ResourcesProviderItem
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

    await provider.open()

    assert set(resources_manager.resources["tests.resource"].resources.keys()) == {
        "test-1",
        "test-2",
    }

    await provider.close()

    assert not resources_manager.resources["tests.resource"].resources
