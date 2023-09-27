.. _concurrency:

Concurrency
===========

One of the main features of Saturn is to manage jobs concurrency at different levels.
For many reasons, you might want to restrict the parallelism to match available resources or to adhere to external constraints.

The following knobs are available to control concurrency:

* :ref:`job_concurrency`
* :ref:`resource_concurrency`
* :ref:`executor_concurrency`

.. _job_concurrency:

Job Concurrency
---------------

Job concurrency restrict the concurrent message processed by a job. It's defined by setting the ``spec.config.job.max_concurrency`` attribute on a job definition.

For example, the following job would run at most two pipelines in parallel:


.. code-block:: yaml
   :emphasize-lines: 10, 11, 12

   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnJobDefinition
   metadata:
     name: slow-job
   spec:
     minimalInterval: "@hourly"
     template:
       input: ...
       pipeline: ...
       config:
         job:
           max_concurrency: 2

This option is useful to limit the jobs that uses a higher amount of computing resources. Setting ``max_concurrency`` to ``1``, removes any parallel execution.

.. _resource_concurrency:

Resource Concurrency
--------------------

A resource also limits the concurrent message processed by jobs. However, resources can be used over multiple jobs. This way, it's possible to limit the concurrency of a group of jobs. Resource also allows more advanced rate limit rule such as `N` execution per `M` unit of time. When many jobs use the same resource, the job messages are processed in a round-robin order, so every job gets the same amount of execution counts.

For example, the following two jobs share the same resource, a Github API key. This ensures both jobs will be able to use the same Github API key while keeping the rate limit under 4,000 calls per hours with at least one seconds between each call. Resource also restricts the concurrency to one call at any time for any resource. Note that a job refers to resources by its type and not its name. This allows Saturn to use multiple resources for a single job. It is then possible to increase the concurrency by having multiple resources of the same type.

.. code-block:: yaml
   :emphasize-lines: 11, 12, 24, 25, 32-39

   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnJobDefinition
   metadata:
     name: job-a
   spec:
     minimalInterval: "@hourly"
     template:
       input: ...
       pipeline:
         name: examples.pipelines.list_github_events
         resources:
           api_key: example.resources.GithubApiKey
   ---
   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnJobDefinition
   metadata:
     name: job-b
   spec:
     minimalInterval: "@hourly"
     template:
       input: ...
       pipeline:
         name: examples.pipelines.list_github_users
         resources:
           api_key: example.resources.GithubApiKey
   ---
   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnResource
   metadata:
     name: github-api-key
   spec:
     type: example.resources.GithubApiKey
     data:
       key: GITHUB_API_KEY
     default_delay: 1
     rate_limit:
       strategy: moving-window
       rate_limits:
       - 5000 per hour
   ---

Resources can reproduce job concurrency options by creating multiple dummy resources and set them on a single job.

.. _executor_concurrency:

Executor Concurrency
--------------------

Finally, executor themselves have a limit on how many jobs they can run concurrently. This setting depends on the executor. For example, the :py:class:`ProcessExecutor` will have concurrency equal to its ``max_workers`` option:

.. code-block:: yaml
   :emphasize-lines: 8

   apiVersion: saturn.flared.io/v1alpha1
   kind: SaturnExecutor
   metadata:
     name: default
   spec:
     type: ProcessExecutor
     options:
       max_workers: 2

The executor above would have the concurrency of ``2``. Since the executor runs all jobs, its concurrency is likely to limit most of the jobs concurrency. If a job doesn't have a ``max_concurrency`` or any resources, it will only be limited by the executor.
