Command Line Interface
======================
Installing Datacube-explorer installs the ``cubedash-gen`` script, a `Click`_ command line
interface, in your virtualenv. Executed from the terminal, this script gives
access to built-in, extension, and application-defined commands. The ``--help``
option will give more information about any commands and options.

.. _Click: https://click.palletsprojects.com/

Summary generation
-------------------

Initialise and create product summaries:

.. code-block:: text

    cubedash-gen --init --all