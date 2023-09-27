.. _first-pipeline:

###########
 Pipelines
###########

To create our first pipeline all we need is to define a Python function that receive
parameters from a Saturn message and do some processing or returns new messages.

Let's start by creating a new file named ``pipelines.py``:

.. code:: python

   from abc.collection import Iterator
   from saturn_engine.core import TopicMessage
   from saturn_engine.core.pipeline import PipelineResult


   def upper_word(word: str) -> TopicMessage:
       return TopicMessage(args={"upper": word.upper()})

What does this code does?

First it import a few object we need for typing definition. Saturn is written in modern
Python and aims for a 100% typing coverage using `mypy
<https://mypy.readthedocs.io/en/stable/>`_. Parameters types are very important since
Saturn is going to use them to convert message into pipeline parameters.

Let's move on to the function definition:

.. code:: python

   def upper_word(word: str) -> Iterator[PipelineResult]:

Saturn pipelines are simple Python function. They take arbitrary parameters and simply
returns new messages or events. This make testing a pipeline as simple as calling a
Python function. The typing is used by Saturn to ensure a message is converted into
valid pipeline parameters. This means that the pipeline will always receive ``word`` as
a ``str`` or fail otherwise. Pipeline can return new message that will be forwarded by
Saturn to the next step. The message type is ``TopicMessage``.

We keep the pipeline implementation as simple as possible for this example:

.. code:: python

   return TopicMessage(args={"upper": word.upper()})

In this case we simply return a new Saturn message with the given word as uppercase for
its arguments.

That's as simple as this. We built our first Saturn pipeline, now let's plug it into a
job in our next tutorial: <TODO>
