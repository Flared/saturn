# Resources Provider

Author: @isra17 Summary: Resources Provider aims to dynamically provision
resource to be used for pipelines. 

Status: implemented

## Background

At the moment, a pipeline function can define resources as parameter to ask
Saturn to acquire exclusive access to some resource. These resources are defined
statically in the topology file as `SaturnResource` kind.

However, new needs arose where we want resources to be provisionned from a
database. For example, a system might want to provision a resource from a set of
proxies to be used. The amount of proxies could change over time and cannot be
defined in the topology files. A similar use-case could happen with an API key
pools that can be managed from an external systems. New key could be added or
old key removed.

## Proposal

We propose to add a new kind of object `SaturnResourcesProvider` that would
define a class to provision resources to the resource manager and allow to keep
the resource manager in sync with some potential database.

```yaml
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnResourcesProvider
metadata:
  name: proxies-resource-provider
spec:
  type: org.example.ProxiesResourceProvider
  resource_type: org.example.ProxyResource
  options:
    database_url: postgres://example.org
```

The provider `type` is a fully qualified path to the provider implementation.
The `resource_type` allows the manager to schedule provider alongside pipelines
that use provided resource. `resource_type` semantic is the same as
`SaturnResource`'s `type`. Finally, `options` is a non-structured mapping passed
to the provider initializer. This can be used to define provider parameters such
as services or database URL.

### Static Provider

To simplify API, we propose to create a builtin provider of type
`StaticResources`. This type can be used to proxy `SaturnResource` and limit the
objects returned by the manager to the worker. The manager should parse
`SaturnResource` and convert them internally as `StaticResources`. For example,
the following topology:

```yaml
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnResource
metadata:
  name: proxy-resource
spec:
  type: org.example.ProxyResource
  data: {url: "http://example.org"}
```

would be converted into:

```yaml
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnResourcesProvider
metadata:
  name: proxy-resource-provider
spec:
  type: StaticResources
  resource_type: org.example.ProxyResource
  options:
    resources:
    - name: proxy-resource
      data: {url: "http://example.org"}
```

### Worker Usage

The worker load and initialize the provider object. The provider is a service
that get initialized with its option and the worker resource manager. The
service is then able to start a background task to monitor any resource changes
and add them to the resource manager.

A simple implementation for `StaticResources` could be:

```python
class StaticResources(ResourceProvider):
  @dataclasses.dataclass
  class Options:
    resources: list[Resource]

  async def open(self) -> None:
    for resource in self.options.resources:
      self.add(resource)
```

`ResourceProvider` should implement a few helper method such as `add` and
`remove` that keeps track of resources being managed and clean them on `close`.

### Edge-case

Few things to note and ensure works:

 - Resource can be removed while being in use: The resource manager must be able
   to handle operation on resource that does not exists any more. Just ignoring
   the operation should work most of the time.

 - Resource distribution among multiple workers: In the future we might want to
   be able to distribute work and resources to multiple worker. While the static
   resource made it easy to eventually distribute them among workers, the
   provider cannot be splitted as-is to many worker. A workaround could be to
   add a `quota` option being added to all provider. This option could allow the
   manager to distribute the same provider to multiple worker, with quotas that
   sums up to `1.0`. The provider can then use this quota to only select a
   subset of resources. For example, a manager could send the provider to two
   workers, each with a quota of `0.5`. This way, each workers' provider will
   only lock 50% of the available resources.

 - The provider must load all resource in memory: The current design is
   pre-emptive. It must loads all resources for them to be used. This is a
   tradeoff for simplicity. It is assumed that the resources count of a provider
   will always be small enough to be held in memory. If a use-case implies too
   many resources, there might be an issue with the topology design, and maybe
   it should be a use-case for an inventory.
