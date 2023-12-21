.. _cancelled-job:

#######################
 Gracefully cancel job
#######################

When a Saturn Worker die or timeout, it will stop listening for an executor result. In these cases, it might be better to let the executor gracefully stop its job. This can be achieved by looking at a `CancellationToken` passed to the pipeline. Note that this feature is only supported with ARQ executors.

.. literalinclude:: /example/how_to/cancel.py
   :caption: cancel.py

This is achieved by adding an argument with type annotation of `CancellationToken`. This object exposes the `is_cancelled` property or the `threading.Event` object with the `event` property.
