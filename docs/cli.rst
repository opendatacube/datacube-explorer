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


.. click:: cubedash.generate:cli
    :prog: cubedash-gen
    :show-nested:


Run application
---------------

A simple `cubedash-run` command is available to run Explorer locally:

.. code-block:: text

    $ cubedash-run
        * Running on http://localhost:8080/ (Press CTRL+C to quit)


.. click:: cubedash.run:cli
    :prog: cubedash-run
    :show-nested:


(see `cubedash-run --help` for list of options)

But Explorer can be run using any typical python wsgi server, for example gunicorn:

.. code-block:: text

    pip install gunicorn
    gunicorn -b '127.0.0.1:8080' -w 4 cubedash:app

Products will begin appearing one-by-one as the summaries are generated in the
background.  If impatient, you can manually navigate to a product using
`/<product_name`. (Eg `/ls5_nbar_albers`)
