..
   Saturn documentation master file, created by
   sphinx-quickstart on Tue Jul 18 17:05:17 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

########
 Saturn
########

Welcome to **Saturn** documentation. **Saturn** is a Python framework to run and
orchestrate job pipelines. It's developed by `Flare <https://flare.io>`_ as their core
job management system to power most of their data collection and analysis pipelines. It
targets pipelines high-throughput and efficient scheduling.

.. note::

   This project is under active development.

.. toctree::
   :maxdepth: 2
   :hidden:

   tutorial/index
   how_to/index
   topics/index
   references/index

TODO
----

 * Topics

   * Architecture

     * Inventory
     * Topics
     * Executors
     * Services

   * Error Handling
   * Backpressure
   * Job states

 * References

   * All Builtings Topics
   * All Builtings Inventory
   * All Builtings Services

 * Guides

   * Create an Inventory or a Topic
   * Create a Service
   * Debug backpressure
   * Common Patterns

     * Subjobs
     * Loop until condition is met

   * Developer Guide

     * Setup local environment
     * Run tests and linting
     * Release

This documentation have been inspired and adapted from project like `Divio
<https://documentation.divio.com/>`_ or `Django CMS
<https://docs.django-cms.org/en/latest>`_.
