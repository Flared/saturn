:: _job:

########
 Topics
########

Now that we have our :ref:`first pipeline <first-pipeline>`, we can use it within a job
to process a set of data.

A job is made of three main compoent:

**Input**

   A job input will yields multiple message to be processed by a pipeline. This can be a
   streaming topic or a batch of object.

**Pipeline**

   A pipeline is a function that does the actual processing, receiving inputs and
   creating new messages that can be forwarded to outputs.

**Outputs**

   Outputs are topics that can publish new message. It is common for a job output to
   also be the input of another jobs.

All Saturn components are defined through YAML topology files.

*************
 Input Topic
*************

A job input and outputs are references to Topics or Inventories defined as component, so
let's start with this:

.. code:: yaml

   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnTopic
   metadata:
     name: input-topic
   spec:
     type: StaticTopic
     options:
       messages:
       - {"id": "0", "args": {"word": "hello"}}
       - {"id": "1", "args": {"word": "world"}}

This first component define a topic that yield items defined in the YAML. It can be seen
as a static list of message to process.

The first two line allow to define the kind of component we are defining:

.. code:: yaml

   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnTopic

Here the most important part is `SaturnTopic` that identify this YAML object as a Topic.

We then set some metadata:

.. code:: yaml

   metadata:
     name: input-topic

``name`` is the only required metadata. It is going to be used by other component to
refer to this Topic.

Finally the spec, define component instance itself:

.. code:: yaml

   spec:
     type: StaticTopic

The ``type`` attribute define the class used to create this Topic. Saturn comes with a
few builtins topic type that are useful for most basic scenario.

.. code:: yaml

   options:
     messages:
     - {"id": "0", "args": {"word": "hello"}}
     - {"id": "1", "args": {"word": "world"}}

Each topic have ``options`` that is used to configure a specific instance. Here we
define the message we want to yield from this topic.
