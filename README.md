# Data Cube Explorer
[![Linting](https://github.com/opendatacube/datacube-explorer/workflows/Linting/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3ALinting)
[![Tests](https://github.com/opendatacube/datacube-explorer/workflows/Tests/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3ATests)
[![Docker](https://github.com/opendatacube/datacube-explorer/workflows/Docker/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3ADocker)
[![Scan](https://github.com/opendatacube/datacube-explorer/workflows/Scan/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3AScan)
[![coverage](https://codecov.io/gh/opendatacube/datacube-explorer/branch/develop/graph/badge.svg)](https://codecov.io/gh/opendatacube/datacube-explorer)
[![Doc](https://readthedocs.org/projects/datacube-explorer/badge/?version=latest)](http://datacube-explorer.readthedocs.org/en/latest/)

![Explorer Screenshot](screenshot.png)

## Usage (quick-start)

Assuming you already have an Open Data Cube instance, Explorer will use
its existing settings.

Install Explorer:

    pip install datacube-explorer

Generate summaries for all of your products:

    cubedash-gen --init --all

Run Explorer locally:

    cubedash-run

It will now be viewable on [http://localhost:8090](https://localhost:8090)

## Developer Setup

These directions are for running from a local folder in development. But it will run from any typical Python WSGI server.

Firstly, install the Open Data Cube. Use of a [Data Cube conda environment](https://datacube-core.readthedocs.io/en/latest/installation/setup/common_install.html)
is recommended. You may need to also `conda install -c conda-forge postgis`

Test that you can run `datacube system check`, and that it's connecting
to the correct datacube instance.

### Dependencies

Now install the explorer dependencies:

    # These two should come from conda if you're using it, not pypi
    conda install fiona shapely

    pip install -e .

### Summary generation

Initialise and create product summaries:

    cubedash-gen --init --all

(This can take a long time the first time, depending on your datacube size.)

Other available options can be seen by running `cubedash-gen --help`.

### Run

A `cubedash-run` command is available to run Explorer locally:

    $ cubedash-run
        * Running on http://localhost:8080/ (Press CTRL+C to quit)

(see `cubedash-run --help` for list of options)

But Explorer can be run using any typical Python WSGI server, for example [gunicorn](https://gunicorn.org/):

    pip install gunicorn
    gunicorn -b '127.0.0.1:8080' -w 4 cubedash:app

Products will begin appearing one-by-one as the summaries are generated in the
background.  If impatient, you can manually navigate to a product using
`/<product_name`. (Eg `/ls5_nbar_albers`)

### Code Style

All code is formatted using [black](https://github.com/ambv/black), and checked
with [pyflakes](https://github.com/PyCQA/pyflakes).

They are included when installing the test dependencies:

    pip install --upgrade --no-deps --extra-index-url https://packages.dea.ga.gov.au/ 'datacube' 'digitalearthau'

    pip install -e .[test]

Run `make lint` to check your changes, and `make format` to format your code
automatically.

You may want to configure your editor to run black automatically on file save
(see the Black page for directions), or install the pre-commit hook within Git:

### Pre-commit setup

A [pre-commit](https://pre-commit.com/) config is provided to automatically format
and check your code changes. This allows you to immediately catch and fix
issues before you raise a failing pull request (which run the same checks under
Travis).

Install pre-commit from pip, and initialise it in your repo:

    pip install pre-commit
    pre-commit install

Your code will now be formatted and validated before each commit. You can also
invoke it manually by running `pre-commit run`

**Note**: If you use Conda, install from conda-forge (This is *required* because the pip
version uses virtualenvs which are incompatible with Conda's environments)

    conda install pre_commit

## FAQ


### Can I use a different datacube environment?

Set ODC's environment variable before running the server:

    export DATACUBE_ENVIRONMENT=staging

You can always see which environment/settings will be used by running `datacube system check`.

See the ODC documentation for config and [datacube environments](https://datacube-core.readthedocs.io/en/latest/user/config.html#runtime-config)

### Can I add custom scripts or text to the page (such as analytics)?

Create one of the following `*.env.html` files:

- Global include: for `<script>` and other tags at the bottom of every page.

      cubedash/templates/include-global.env.html

- Footer text include. For human text such as Copyright statements.

      echo "Server <strong>staging-1.test</strong>" > cubedash/templates/include-footer.env.html

(`*.env.html` is the naming convention used for environment-specific templates: they are ignored by
Git)

### How can I configure the deployment?

Add a file to the current directory called `settings.env.py`

You can alter default [Flask](http://flask.pocoo.org/docs/1.0/config/) or
[Flask Cache](https://pythonhosted.org/Flask-Caching/#configuring-flask-caching) settings
(default "CACHE_TYPE: NullCache"), as well as some cubedash-specific settings:

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

[Sentry](https://sentry.io/) error reporting is supported by adding a `SENTRY_CONFIG` section.
See [their documentation](https://docs.sentry.io/clients/python/integrations/flask/#settings).

### How do I modify the CSS/Javascript?

The CSS is compiled from [Sass](https://sass-lang.com/), and the Javascript is compiled from
[Typescript](https://www.typescriptlang.org/).

Install [npm](https://www.npmjs.com/get-npm), and then install them both:

    npm install -g sass typescript

You can now run `make static` to rebuild all the static files, or
individually with `make style` or `make js`.

Alternatively, if using [PyCharm](https://www.jetbrains.com/pycharm), open a
Sass file and you will be prompted to enable a `File Watcher` to
compile automatically.

PyCharm will also compile the Typescript automatically by ticking
the "Recompile on changes" option in `Languages & Frameworks ->
Typescript`.

### How do I run the integration tests?

The integration tests run against a real PostgreSQL database, which is
automatically started and stopped using Docker. This requires Docker to
be available, but no further database setup is required.

Install the test dependencies: `pip install -e .[test]`

The run the tests with: `pytest integration_tests`

### How do I add test data for the automated tests?

Most of the automated tests for Datacube Explorer require sample data to run. This comprises
definitions of ODC *Metadata Types*, *Products* and *Datasets*.

These are contained within YAML files in the [`integration_tests/data`](https://github.com/opendatacube/datacube-explorer/tree/develop/integration_tests/data) directory.

Test data is loaded using a pytest fixture called `auto_odc_db`, which is activated per
test module, and will automatically populate the database using files referenced in module
global variables. Activate and use it similar to the following example:


    pytestmark = pytest.mark.usefixtures("auto_odc_db")

    METADATA_TYPES = ["metadata/qga_eo.yaml"]
    PRODUCTS = ["products/ga_s2_ard.odc-product.yaml"]
    DATASETS = ["s2a_ard_granule.yaml.gz"]


To add sample datasets required for the test case, create a `.yaml` file
with the product name and place all the sample datasets split by `---` in the yaml.

If the sample datasets file is large, compress it with `gzip <dataset_file>.yaml` and reference
that file instead.

## Roles for production deployments

The [roles](cubedash/summary/roles) directory contains sql files for creating
Postgres roles for Explorer. These are suitable for running each Explorer
task with minimum needed security permissions.

Three roles are created:

- **explorer-viewer**: A read-only user of datacube and Explorer. Suitable for the web interface and cli (`cubedash-view`) commands.
- **explorer-generator**: Suitable for generating and updating summaries (ie. Running `cubedash-gen`)
- **explorer-owner**: For creating and updating the schema. (ie. Running `cubedash-gen --init`)

Note that these roles extend the built-in datacube role `agdc_user`. If you
created your datacube without permissions, a stand-alone creator of the `agdc_user`
role is available as a prerequisite in the same [roles](cubedash/summary/roles)
directory.

## Docker for Development and running tests

You need to have Docker and Docker Compose installed on your system.

To create your environment, run `make up` or `docker-compose up`.

You need an ODC database, so you'll need to refer to the [ODC docs](https://datacube-core.readthedocs.io/en/latest/) for help on indexing, but you can create the database by running `make initdb` or `docker-compose exec explorer datacube system init`. (This is not enough, you still need to add a product and index datasets.)

When you have some ODC data indexed, you can run `make index` to create the Explorer indexes.

Once Explorer indexes have been created, you can browse the running application at [http://localhost:5000](http://localhost:5000).

You can run tests by first creating a test database `make create-test-db-docker` and then running tests with `make test-docker`.

And you can run a single test in Docker using a command like this: `docker-compose --file docker-compose.yml run explorer pytest integration_tests/test_dataset_listing.py`


## Docker-compose for Development and running tests
### Testing with app.config
edit `.docker/settings_docker.py` and setup application config. Then `docker-compose -f docker-compose.yml -f docker-compose.override.yml up` to bring up explorer docker with database, explorer with settings


## STAC API Extensions

The STAC endpoint implements the [query](https://github.com/stac-api-extensions/query), [filter](https://github.com/stac-api-extensions/filter), [fields](https://github.com/stac-api-extensions/fields), and [sort](https://github.com/stac-api-extensions/sort) extensions, all of which are bound to the `search` endpoint as used with POST requests, with fields and sort additionally bound to the features endpoint.

Fields contained in the item properties must be prefixed with `properties.`, ex `properties.dea:dataset_maturity`.

The implementation of `fields` differs somewhat from the suggested include/exclude semantics in that it does not permit for invalid STAC entities, so the `id`, `type`, `geometry`, `bbox`, `links`, `assets`, `properties.datetime`, and `stac_version` fields will always be included, regardless of user input.

The implementation of `filter` is limited, and currently only supports CQL2 JSON syntax with the following basic CQL2 operators: `AND`, `OR`, `=`, `>`, `>=`, `<`, `<=`, `<>`, `IS NULL`.
