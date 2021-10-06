# Architecture Overview


## Concepts

**Inventory**: Inventories are lists of things of the same type that serve as input to queues. They are used to generate queue messages. Example: List of pages to crawl.

**Job**: A job is the execution of a pipeline over all inventory items. A job effectively feeds an inventory into a queue.

**Job Group**: A job group links jobs together to manage rescheduling of jobs once the group is completed instead of a single job.

**Topics**: Topics are components that broadcast messages to all subscribed queues.

**Queues**: Queues feed pipelines. A queue holds properties such as resources to enrich messages coming from inventory or topics.

**Pipeline**: A pipeline takes messages read from a queue as input to some function. Pipelines might require resources to process messages.

**Resource**: A resource is an object that is used by workers to run a pipeline. For example, an API Key might be required by a pipeline to enrich data with a third-party service. Resources effectively allow to manage pipeline concurrency and rate-limit.

**Worker**: Workers run pipelines by processing a set of queues and jobs

**WorkerManager**: The Worker Manager distributes the queues and jobs processing to the available workers.

![class_diagram](https://www.plantuml.com/plantuml/proxy?cache=no&fmt=svg&src=https://github.com/Flared/saturn/raw/main/docs/plantuml/class_diagram.plantuml)
