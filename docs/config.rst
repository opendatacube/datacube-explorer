Configuration Handling
======================

Application Configuration Values
----------------------------

The following configuration values are used internally by Datacube-explorer:


.. py:data:: CUBEDASH_CORS

    Enable Cross Origin Resource Sharing (CORS) for ``stac`` and ``api``.

    Default: ``True``

.. py:data:: CUBEDASH_PRODUCT_GROUP_BY_REGEX

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``None``

.. py:data:: CUBEDASH_PRODUCT_GROUP_BY_FIELD

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``product_type``

.. py:data:: CUBEDASH_PRODUCT_GROUP_SIZE

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``5``

.. py:data:: CUBEDASH_DEFAULT_GROUP_NAME

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``Other Products``

.. py:data:: CUBEDASH_HARD_SEARCH_LIMIT

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``150``

.. py:data:: CUBEDASH_DEFAULT_API_LIMIT

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``500``

.. py:data:: CUBEDASH_PROVENANCE_DISPLAY_LIMIT

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``25``

.. py:data:: CUBEDASH_DEFAULT_TIMEZONE

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``Australia/Darwin``

.. py:data:: CUBEDASH_SISTER_SITES

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``None``

.. py:data:: CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``[]``


.. py:data:: CUBEDASH_DEFAULT_ARRIVALS_DAY_COUNT

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``14``

.. py:data:: CUBEDASH_SHOW_PERF_TIMES

    Add server timings to http headers.

    Default: ``False``

.. py:data:: CUBEDASH_THEME

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``odc``

.. py:data:: CUBEDASH_DEFAULT_LICENSE

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``None``

.. py:data:: STAC_ENDPOINT_ID

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``odc-explorer``

.. py:data:: STAC_ENDPOINT_TITLE

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``Default ODC Explorer instance``

.. py:data:: STAC_ENDPOINT_DESCRIPTION

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``Configure stac endpoint information in your Explorer `settings.env.py` file``

.. py:data:: STAC_ABSOLUTE_HREFS

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``True``

.. py:data:: STAC_DEFAULT_PAGE_SIZE

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``20``

.. py:data:: STAC_PAGE_SIZE_LIMIT

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``1000``

.. py:data:: STAC_DEFAULT_FULL_ITEM_INFORMATION

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``True``

.. py:data:: CUBEDASH_DATA_S3_REGION

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``ap-southeast-2``

.. py:data:: default_map_zoom

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``False``

.. py:data:: default_map_center

    If there is no handler for an ``HTTPException``-type exception, re-raise it
    to be handled by the interactive debugger instead of returning it as a
    simple error response.

    Default: ``False``


Configuring from Python Files
-----------------------------

mount `settings.env.py` to datacube-explorer

Configuring from Environment Variables
--------------------------------------

Environment variables can be set in the shell before starting the
server:

.. tabs::

   .. group-tab:: Bash

      .. code-block:: text

         $ export FLASK_ENV=development
         $ export FLASK_APP=cubedash
         $ cubedash-run
          * Running on http://localhost:8080/ (Press CTRL+C to quit)