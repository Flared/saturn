.. _install:

##############
 Installation
##############

We strongly recommend you use the latest Python version. Saturn support Python 3.10 and newer.

Create an environment
~~~~~~~~~~~~~~~~~~~~~

It is recommended to start your project inside a :doc:`virtual environment <python:library/venv>`:


..  code-block:: bash

    python3 -m venv venv # create a virtualenv
    source venv/bin/activate  # activate it

Install Saturn
~~~~~~~~~~~~~~

Saturn can then be added to your project by using the official Pypi package:

.. tabs::

   .. group-tab:: Pip

      ..  code-block:: bash

          pip install saturn-engine[all]

   .. group-tab:: Poetry

      ..  code-block:: bash

          poetry add saturn-engine -E all
