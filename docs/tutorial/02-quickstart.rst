:: _quickstart:

############
 Quickstart
############

*****************
 Create Topology
*****************

Saturn main input is the topology file defining all the components: Executor, topics,
inventories, resources and jobs. Let's start by creating a new file ``topology.yaml``:

.. literalinclude:: /example/quickstart/topology.yaml
   :caption: topology.yaml

This file defines an input topic that will yield three message with ``"hello"``,
``"world"`` and ``"!"``.

*****************
 Create Pipeline
*****************

We will go into detail for each component in the next sections. For now let's implement
our pipeline ```pipelines.capitalize``.

Let's create a new file ``pipelines.py``:

.. literalinclude:: /example/quickstart/pipelines.py
   :caption: pipelines.py

A Saturn pipeline is a simple function. Here it simply create a new message with a
capitalized word.

****************
 Running Saturn
****************

Now we are ready to launch our job within Saturn.

Let's start the worker manager. This service will load our topology and expose it to any
Saturn workers that are ready to run jobs. The worker manager also track state so that
Saturn workers can come and go without losing any progress.

Run the following command in a terminal:

.. code:: bash

   python -m saturn_engine.worker_manager.server

After a few seconds you should see the web application being ready:

.. code::

   $ python -m saturn_engine.worker_manager.server
    * Serving Flask app 'server'
    * Debug mode: off
    * Running on http://127.0.0.1:5000
   Press CTRL+C to quit

The worker manager will be waiting for a Saturn worker to connect to it.

Let's start it with the following command in a second terminal:

.. code:: bash

   python -m saturn_engine.worker.runner

You should quickly see the worker start and print some lines in the terminal:

.. code::

   2023-09-28 16:33:30,539 [INFO] saturn.worker.broker.Broker: Initializing worker
   2023-09-28 16:33:30,539 [INFO] saturn.worker.broker.Broker: Starting worker
   2023-09-28 16:33:30,554 [INFO] saturn.worker.broker.Broker: Worker sync
   {"id": "9738bd26-88bd-419b-92b9-333a4fbf2391", "args": {"word": "Hello"}}
   {"id": "b56c7845-913b-4d4b-97fa-a7ef8a8f2914", "args": {"word": "World"}}
   {"id": "4c140bbe-a663-4242-b27e-c14d25d5c3b6", "args": {"word": "!"}}

Note how the printed object have capitalized word! They have been printed in the
terminal since our jobs output is set to an stdout topic writer.

You have now run your Saturn job, now we should go through each of the component we just
used and explain them a little more in detail.
