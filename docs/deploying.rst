Deploying
=========

Configure deployment
--------------------

Add a file to the current directory called ``settings.env.py``

You can alter default config values, all the Explorer application config settings can be found at :any:`explorer-app-settings`.

.. code-block:: text

    # Default product to display (picks first available)
    CUBEDASH_DEFAULT_PRODUCTS = ('ls8_nbar_albers', 'ls7_nbar_albers')

    # Optional title for this Explorer instance to put at the top of every page.
    # Eg. "NCI"
    # If the STAC_ENDPOINT_TITLE is set (below), it will be the default for this value.
    CUBEDASH_INSTANCE_TITLE = None

    # Specify product grouping in the top menu.
    # Expects a series of `(regex, group_label)` pairs. Each product will be grouped into the first regexp that matches
    # anywhere in its name. Unmatched products have their own group see CUBEDASH_DEFAULT_GROUP_NAME, group names shouldn't
    include the default name.
    # eg "(('^usgs_','USGS products'), ('_albers$','C2 Albers products'), ('level1','Level 1 products'), )"
    CUBEDASH_PRODUCT_GROUP_BY_REGEX = None
    # CUBEDASH_PRODUCT_GROUP_BY_REGEX = (r'^usgs_','USGS products'), (r'_albers$','C2 Albers products'), (r'level1','Level 1 products'), )
    # Otherwise, group by a single metadata field in the products:
    CUBEDASH_PRODUCT_GROUP_BY_FIELD = 'product_type'
    # Ungrouped products will be grouped together in this size.
    CUBEDASH_PRODUCT_GROUP_SIZE = 5
    # Ungrouped products will be grouped together using this name
    CUBEDASH_DEFAULT_GROUP_NAME = 'Other Products'
    # Maximum search results
    CUBEDASH_HARD_SEARCH_LIMIT = 100
    # Dataset records returned by '/api'
    CUBEDASH_DEFAULT_API_LIMIT = 500
    CUBEDASH_HARD_API_LIMIT = 4000
    # Maximum number of source/derived datasets to show
    CUBEDASH_PROVENANCE_DISPLAY_LIMIT = 20

    CUBEDASH_DEFAULT_TIMEZONE = "Australia/Darwin"

    CUBEDASH_SISTER_SITES = None
    # CUBEDASH_SISTER_SITES = (('Production - ODC', 'http://prod.odc.example'), ('Production - NCI', 'http://nci.odc.example'), )

    CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST = None
    # CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST = [
    #    "ls5_pq_scene",
    #    "ls7_pq_scene",
    # ]

    # How many days of recent datasets to show on the "/arrivals" page?
    CUBEDASH_DEFAULT_ARRIVALS_DAY_COUNT = 14

    # Include load performance metrics in http response.
    CUBEDASH_SHOW_PERF_TIMES = False

    # Which theme to use (in the cubedash/themes folder)
    CUBEDASH_THEME = 'odc'

    # The default license to show for products that don't have one.
    #     license is optional, but the stac API collections will not pass validation if it's null)
    #     Either a SPDX License identifier, 'various' or 'proprietary'
    #     Example value: "CC-BY-SA-4.0"
    CUBEDASH_DEFAULT_LICENSE = None

    # Customise '/stac' endpoint information
    STAC_ENDPOINT_ID = 'my-odc-explorer'
    STAC_ENDPOINT_TITLE = 'My ODC Explorer'
    STAC_ENDPOINT_DESCRIPTION = 'Optional Longer description of this endpoint'

    STAC_DEFAULT_PAGE_SIZE = 20
    STAC_PAGE_SIZE_LIMIT = 1000

    # Should search results include the full properties of every Stac Item by default?
    # Full searches are much slower because they use ODC's own raw metadata table.
    # (Users can append "_full=true" to requests to manually ask for full metadata.
    #  Or preferrably, follow the `self` link of the Item record to get the whole record)
    STAC_DEFAULT_FULL_ITEM_INFORMATION = True

    # If you'd like S3 URIs to be transformed to HTTPS links then
    # set this to a valid AWS region string. Otherwise set it to None to not do this.
    CUBEDASH_DATA_S3_REGION = "ap-southeast-2"

    # Default map view when no data is loaded.
    # The default values will depend on the CUBEDASH_THEME (eg. 'africa' theme defults to Africa)
    default_map_zoom = 3
    default_map_center = [-26.2756326, 134.9387844]

    # S3 buckets for which data browser url should be returned
    SHOW_DATA_LOCATION = { "dea-public-data": "data.dea.ga.gov.au" }

Sentry error reporting is supported and can be setup as per :any:`sentry-env`


Roles for production deployments
---------------------------------

The `roles`_ directory contains sql files for creating
Postgres roles for Explorer. These are suitable for running each Explorer
task with minimum needed security permissions.

Three roles are created:

- **explorer-viewer**: A read-only user of datacube and Explorer. Suitable for the web interface and cli (`cubedash-view`) commands.
- **explorer-generator**: Suitable for generating and updating summaries (ie. Running ``cubedash-gen``)
- **explorer-owner**: For creating and updating the schema. (ie. Running ``cubedash-gen --init``)

Note that these roles extend the built-in datacube role ``agdc_user``. If you
created your datacube without permissions, a stand-alone creator of the ``agdc_user``
role is available as a prerequisite in the same `roles`_

.. _roles: https://github.com/opendatacube/datacube-explorer/tree/develop/cubedash/summary/roles


Deploying with Helm Chart
--------------------------

Prerequisites
^^^^^^^^^^^^^

Make sure you have Helm `installed <https://helm.sh/docs/using_helm/#installing-helm>`_.

Get Repo Info
^^^^^^^^^^^^^^

.. code::

    helm repo add datacube-charts https://opendatacube.github.io/datacube-charts/charts/
    helm repo update


See `helm repo <https://helm.sh/docs/helm/helm_repo/>`_ for command documentation.


Deploy with default config
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code::

    helm upgrade --install datacube-explorer datacube-charts/datacube-explorer


Deploy in a custom namespace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code::

    helm upgrade --install datacube-explorer --namespace=web datacube-charts/datacube-explorer

Chart values
^^^^^^^^^^^^

.. code::

    helm show values datacube-charts/datacube-explorer
