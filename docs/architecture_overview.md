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

## Design

Saturn's goal is to periodically process inventory with multiple pipelines and schedule them efficiently across workers.
The input can come from both Inventories and Topics.
Inventories are used for persistent items that are used to initiate pipelines while Topics are intermediary items that connect pipelines.
Topics items are artifacts of inventory items.
Pipeline is the process unit.
Pipelines can run in parallel and usually the input/output boundaries are the parallelism boundaries.
A pipeline must be connected to either an Inventory or a Topic.
Pipeline is implemented as a series of Tasks.
These tasks can be traced and logged to monitor and debug a pipeline run.
It must be possible to rerun pipeline and tasks based on the tracing information.

### Pipelines-Resources

Some pipelines require exclusive resources to use credential or rate-limit.
For example, Github dorking requires both an auth token and rate-limited API calls.
Resources can be global or item specific.
For example, Github can use the global API access to periodically process an inventory every week or a user can provide its own API access token, allowing a faster processing of his items in the inventory.
Rate-limited pipelines can be parallelized by using different ressource.
For this reason, each inventory can be split into “sub-inventory” based on their ressource.

## Back-Pressure

Saturn has no unbounded queue.
Each queue used to transmit messages from a topic or inventory to a pipeline has a limit.
Once this limit is reached, the queue will refuse any new message request.
This allows us to keep the resource usage low and properly track the processing rate of inventory.
A whole pipeline chain will be as fast as its slowest part.
The workers must be smart enough to schedule downstream pipelines to make room to process new upstream pipelines.

## Worker Scheduling

Workers are responsible for subscribing to queues and jobs.
They must poll multiple queues and allocate processing resources to optimize the resource usage and avoid situations where a few topics and inventory take over all processing resources.
Basic algorithms such as round robin and priority mechanisms should avoid these situations.
