Configuration Handling
======================

.. _explorer-app-settings:

Application Configuration Settings
------------------------------------

The following configuration values are used internally by Datacube-explorer:


.. py:data:: CACHE_TYPE

    Enable Flask-Cache https://pythonhosted.org/Flask-Caching/#configuring-flask-caching settings.

    Default: ``NullCache``

.. py:data:: CUBEDASH_CORS

    Enable Cross Origin Resource Sharing (CORS) for ``stac`` and ``api``.

    Default: ``True``

.. py:data:: CUBEDASH_DATA_S3_REGION

    TODO:

    Default: ``ap-southeast-2``

.. py:data:: CUBEDASH_DEFAULT_API_LIMIT

    Query limit for search datasets using Explorer's spatial table

    Default: ``500``


.. py:data:: CUBEDASH_DEFAULT_ARRIVALS_DAY_COUNT

    In a time window between `(today, today - number of days)` show on Audit arrival page

    Default: ``14``

.. py:data:: CUBEDASH_DEFAULT_GROUP_NAME

    Group name for default group and products not matching regex.

    Default: ``Other Products``

.. py:data:: CUBEDASH_DEFAULT_LICENSE

    TODO:

    Default: ``None``


.. py:data:: CUBEDASH_DEFAULT_TIMEZONE

    default grouping timezone for display datasets time in local timezone

    Default: ``Australia/Darwin``

.. py:data:: CUBEDASH_HARD_SEARCH_LIMIT

    limit for number of SQL search for datasets query.

    Default: ``150``

.. py:data:: CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST

    List containing product names to hide from product menu and audit pages.

    Default: ``[]``

.. py:data:: CUBEDASH_PRODUCT_GROUP_BY_FIELD

    TODO:

    Default: ``product_type``


.. py:data:: CUBEDASH_PRODUCT_GROUP_BY_REGEX

    Tuple containing regex for product name matching and group name

    Default: ``None``
    Example: ``((r'^usgs_','USGS products'), (r'_albers$','C2 Albers products'), (r'level1','Level 1 products'), )``


.. py:data:: CUBEDASH_PRODUCT_GROUP_SIZE

   TODO:

    Default: ``5``

.. py:data:: CUBEDASH_PROVENANCE_DISPLAY_LIMIT

    Limit for displaying source datasets and derived datasets of a dataset

    Default: ``25``


.. py:data:: CUBEDASH_SHOW_PERF_TIMES

    Whether to add server timings to http headers or not.

    Default: ``False``

.. py:data:: CUBEDASH_SISTER_SITES

    Tuple containing related explorer instance name and domain

    Default: ``None``
    Example: ``(('Production - ODC', 'http://prod.odc.example'), ('Production - NCI', 'http://nci.odc.example'), )``


.. py:data:: CUBEDASH_THEME

    Theme name to apply to explorer instance, options are ``odc``, ``dea``, ``deafrica``. Those can be viewed in folder under ``cubedash > templates >> themes``

    Default: ``odc``

.. py:data:: SHOW_DATA_LOCATION

    S3 buckets for which to return a browseable bucket link instead of the plain S3 link

    Default: ``{}``
    Example: ``{ 'dea-public-data': 'data.dea.ga.gov.au'}``

.. py:data:: default_map_center

    Leaflet map https://leafletjs.com/reference.html#map-center, variates by explorer theme.

    Default: ``[0.0, 60.0]``

.. py:data:: default_map_zoom

    Leaflet map https://leafletjs.com/reference.html#map-zoom

    Default: ``3``

.. py:data:: STAC_ABSOLUTE_HREFS

    TODO:

    Default: ``True``

.. py:data:: STAC_DEFAULT_FULL_ITEM_INFORMATION

    Request the full Item information. This forces us to go to the ODC dataset table for every record, which can be extremely slow.

    Default: ``True``

.. py:data:: STAC_DEFAULT_PAGE_SIZE

    TODO:

    Default: ``20``

.. py:data:: STAC_ENDPOINT_DESCRIPTION

    description shown on ``/stac`` page.

    Default: ``Configure stac endpoint information in your Explorer `settings.env.py` file``

.. py:data:: STAC_ENDPOINT_ID

    id shown on ``/stac`` page.

    Default: ``odc-explorer``

.. py:data:: STAC_ENDPOINT_TITLE

    title shown on ``/stac`` page.

    Default: ``Default ODC Explorer instance``

.. py:data:: STAC_PAGE_SIZE_LIMIT

    TODO:

    Default: ``1000``

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
         $ export CUBEDASH_DEFAULT_TIMEZONE=Australia/Darwin
         $ cubedash-run
          * Running on http://localhost:8080/ (Press CTRL+C to quit)
