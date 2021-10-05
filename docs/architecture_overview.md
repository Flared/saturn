# Architecture Overview


## Concepts

**Inventory**: Inventories are lists of things of the same type that serve as input to queues. They are used to generate queue messages. Example: List of pages to crawl.

**Topics**: Topics are proxies to message queues. They forward queue messages that serves as input to pipelines. A topic may be associated with one or more resources. Example: Topic *t1* lists pages that have to be crawled using API Key *r1*. A topic may only be assigned to a single worker.

**Job**: Jobs are topics that are proxies to inventories.

**Queues**: Queues contain messages that will be distributed to associated topics. The messages it contains are kept until they were consumed by all topics. Example: Queue *q1* is associated with topics *t1* and *t2*.

**Worker**: Workers are assigned topics and they run the associated pipelines.

**WorkerManager**: The Worker Manager assigns topics to workers.

**Pipeline**: Pipelines are functions that accept queue messages as inputs. They may or may not write messages to other queues.

![class_diagram](https://www.plantuml.com/plantuml/proxy?cache=no&fmt=svg&src=https://github.com/Flared/saturn/raw/main/docs/plantuml/class_diagram.plantuml)
