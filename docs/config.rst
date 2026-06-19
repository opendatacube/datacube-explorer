.. role:: python(code)
   :language: python

Configuration
=============

ODC Explorer can be configured either from a file or environment variables.

Configuring from Python Files
-----------------------------

On startup, ODC Explorer looks for and loads the file named
:file:`settings.env.py`, a Python file with options as
top level global variables.


Configuring from Environment Variables
--------------------------------------

Environment variables can be set in the shell before starting the
server:

.. code-block:: console

   $ export FLASK_ENV=development
   $ export FLASK_APP=cubedash
   $ export CUBEDASH_DEFAULT_TIMEZONE=Australia/Darwin
   $ cubedash-run
    * Running on http://localhost:8080/ (Press CTRL+C to quit)

.. _explorer-app-settings:

Server Settings
---------------

The following configuration settings are provided by ODC Explorer:


.. confval:: CACHE_TYPE

    Enable `Flask-Cache <https://pythonhosted.org/Flask-Caching/#configuring-flask-caching>`__ settings.

    :default: ``NullCache``

.. confval:: CUBEDASH_CORS

    Enable Cross Origin Resource Sharing (CORS) for ``stac`` and ``api``.

    :default: ``True``

.. confval:: CUBEDASH_DATA_S3_REGION

    .. todo:: Describe the S3 Region Option

    :default: ``ap-southeast-2``

.. confval:: CUBEDASH_DEFAULT_API_LIMIT

    Query limit for search datasets using Explorer's spatial table

    :default: ``500``


.. confval:: CUBEDASH_DEFAULT_ARRIVALS_DAY_COUNT

    In a time window between `(today, today - number of days)` show on Audit arrival page

    :default: ``14``

.. confval:: CUBEDASH_DEFAULT_GROUP_NAME

    Group name for default group and products not matching regex.

    :default: ``Other Products``

.. confval:: CUBEDASH_DEFAULT_LICENSE

    .. todo:: Describe the default license option.

    :default: ``None``


.. confval:: CUBEDASH_DEFAULT_TIMEZONE

    default grouping timezone for display datasets time in local timezone

    :default: ``Australia/Darwin``

.. confval:: CUBEDASH_HARD_SEARCH_LIMIT

    limit for number of SQL search for datasets query.

    :default: ``150``

.. confval:: CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST

    List containing product names to hide from product menu and audit pages.

    :default: ``[]``

.. confval:: CUBEDASH_PRODUCT_GROUP_BY_FIELD

   Which Product Field will be used for grouping in the UI.

   :default: ``product_type``


.. confval:: CUBEDASH_PRODUCT_GROUP_BY_REGEX

   Tuple containing regexes for product name matching and group name

   :default: ``None``

   .. code-block:: python
      :caption: Example

      (
           (r'^usgs_','USGS products'),
           (r'_albers$','C2 Albers products'),
           (r'level1','Level 1 products'),
       )


.. confval:: CUBEDASH_PRODUCT_GROUP_SIZE

   TODO:

   :default: ``5``

.. confval:: CUBEDASH_PROVENANCE_DISPLAY_LIMIT

    Limit for displaying source datasets and derived datasets of a dataset

    :default: ``25``


.. confval:: CUBEDASH_SHOW_PERF_TIMES

    Whether to add server timings to http headers or not.

    :default: ``False``

.. confval:: CUBEDASH_SISTER_SITES

    Tuple containing related explorer instance name and domain

    :default: ``None``


    .. code-block:: python
       :caption: Example


       (
           ('Production - ODC', 'http://prod.odc.example'),
           ('Production - NCI', 'http://nci.odc.example'),
        )



.. confval:: CUBEDASH_THEME

    Theme name to apply to explorer instance, options are ``odc``, ``dea``, ``deafrica``. Those can be viewed in folder under ``cubedash > templates >> themes``

    :default: ``odc``

.. confval:: SHOW_DATA_LOCATION

    S3 buckets for which to return a browseable bucket link instead of the plain S3 link

    :default: ``{}``
    :type: ``Mapping[str, str]```

    Example: :python:`{ 'dea-public-data': 'data.dea.ga.gov.au'}`

.. confval:: default_map_center

    Leaflet map https://leafletjs.com/reference.html#map-center, variates by explorer theme.

    :default: ``[0.0, 60.0]``
    :type: ``tuple[float, float]``

.. confval:: default_map_zoom

    Leaflet Map default zoom. `See the LeafletJS docs <https://leafletjs.com/reference.html#map-zoom>`__.`

    :default: ``3``

STAC API Configuration
-----------------------

.. confval:: STAC_ABSOLUTE_HREFS

    TODO:

    :default: ``True``

.. confval:: STAC_DEFAULT_FULL_ITEM_INFORMATION

    Request the full Item information. This forces us to go to the ODC dataset table for every record, which can be extremely slow.

    :default: ``True``

.. confval:: STAC_ENDPOINT_DESCRIPTION

    description shown on ``/stac`` page.

    :default: ``Configure stac endpoint information in your Explorer `settings.env.py` file``

.. confval:: STAC_ENDPOINT_ID

    id shown on ``/stac`` page.

    :default: ``odc-explorer``

.. confval:: STAC_ENDPOINT_TITLE

    title shown on ``/stac`` page.

    :default: ``Default ODC Explorer instance``

.. confval:: STAC_DEFAULT_PAGE_SIZE

    Default number of results returned in a STAC response.

    :default: ``20``

.. confval:: STAC_PAGE_SIZE_LIMIT

    The maximum number of STAC results in a response.

    :default: ``1000``
