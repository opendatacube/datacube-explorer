# Data Cube Explorer
[![Linting](https://github.com/opendatacube/datacube-explorer/workflows/Linting/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3ALinting)
[![Tests](https://github.com/opendatacube/datacube-explorer/workflows/Tests/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3ATests)
[![Docker](https://github.com/opendatacube/datacube-explorer/workflows/Docker/badge.svg)](https://github.com/opendatacube/datacube-explorer/actions?query=workflow%3ADocker)
[![coverage](https://codecov.io/gh/opendatacube/datacube-explorer/branch/develop/graph/badge.svg)](https://codecov.io/gh/opendatacube/datacube-explorer)

![Explorer Screenshot](screenshot.png)

## Developer Setup

These directions are for running from a local folder in development. But it will run from any typical Python WSGI server. 

Firstly, install Data Cube. Use of a [Data Cube conda environment](https://datacube-core.readthedocs.io/en/latest/ops/conda.html)
is recommended.

Test that you can run `datacube system check`, and that it's connecting
to the correct datacube instance.

### Dependencies

Now install the explorer dependencies:

    # These two should come from conda if you're using it, not pypi
    conda install fiona shapely
    
    pip install -e .

### Summary generation

Initialise and create product summaries:

    nohup cubedash-gen --init --all &>> summary-gen.log &

(This can take a while the first time, depending on your datacube size. 
We're using `nohup .. &` to run in the background.)

### Run

Explorer can be run using any typical python wsgi server, for example:

    pip install gunicorn
    gunicorn -b '127.0.0.1:8080' -w 4 cubedash:app

Convenience scripts are available for running in development with hot-reload
(`./run-dev.sh`) or gunicorn (`./run.sh`). Install the optional deployment
dependencies for the latter: `pip install -e .[deployment]`

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

If you don't use Conda, install pre-commit from pip:

    pip install pre-commit

If you do use Conda, install from conda-forge (*required* because the pip
version uses virtualenvs which are incompatible with Conda's environments)

    conda install pre_commit

Now install the pre-commit hook to the current repository:

    pre-commit install

Your code will now be formatted and validated before each commit. You can also
invoke it manually by running `pre-commit run`

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
(default "CACHE_TYPE: simple"), as well as some cubedash-specific settings:

    # Default product to display (picks first available)
    CUBEDASH_DEFAULT_PRODUCTS = ('ls8_nbar_albers', 'ls7_nbar_albers')
    
    # Limited regex syntax to match product names with a user-defined group.
    # Pairs of regex,group are ";" separated. Regex and group are "," separated.
    # As such ";" and "," can't be used in the regexs.
    # eg ".albers., Albers projection; level1, Level 1 products"
    # If re.search(regex, product name) then return the corresponding group.
    # If CUBEDASH_PRODUCT_GROUP_BY_REGEX string parses then regex matching will be used in place of CUBEDASH_PRODUCT_GROUP_BY_FIELD.
    # If CUBEDASH_PRODUCT_GROUP_BY_REGEX string is malformed an error will be logged and we default back to CUBEDASH_PRODUCT_GROUP_BY_FIELD.
    # eg ".*albers.*,Albers projection;level1,Level 1 products" 
    CUBEDASH_PRODUCT_GROUP_BY_REGEX = None 
    # Which field should we use when grouping products in the top menu?
    CUBEDASH_PRODUCT_GROUP_BY_FIELD = 'product_type'
    # Ungrouped products will be grouped together in this size.
    CUBEDASH_PRODUCT_GROUP_SIZE = 5
    
    # Maximum search results
    CUBEDASH_HARD_SEARCH_LIMIT = 100
    # Maximum number of source/derived datasets to show
    CUBEDASH_PROVENANCE_DISPLAY_LIMIT = 20
    
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


[Sentry](https://sentry.io/) error reporting is supported by adding a `SENTRY_CONFIG` section.
See [their documentation](https://docs.sentry.io/clients/python/integrations/flask/#settings).  


### How do I modify the css/javascript?

The CSS is compiled from [Sass](https://sass-lang.com/), and the Javascript is compiled from 
[Typescript](https://www.typescriptlang.org/).

Install [npm](https://www.npmjs.com/get-npm), and then install them both:

    npm install -g sass typescript

You can now run `make static` to rebuild all the static files, or
individually with `make style` or `make js`.

Alternatively, if using PyCharm, open a Sass file and you will be prompted 
to enable a `File Watcher` to compile automatically.

PyCharm will also compile the Typescript automatically by ticking
the "Recompile on changes" option in `Languages & Frameworks ->
Typescript`.

### How do I run the integration tests?
    
The integration tests run against a real postgres database, which is dropped and 
recreated between each test method:

Install the test dependencies: `pip install -e .[test]`

#### Simple test setup

Set up a database on localhost that doesn't prompt for a password locally (eg. add credentials to `~/.pgpass`)

Then: `createdb dea_integration`

And the tests should be runnable with no configuration: `pytest integration_tests`

#### Custom test configuration (using other hosts, postgres servers)

Add a `.datacube_integration.conf` file to your home directory in the same format as 
[datacube config files](https://datacube-core.readthedocs.io/en/latest/user/config.html#runtime-config).

(You might already have one if you run datacube's integration tests)

Then run pytest: `pytest integration_tests`

__Warning__ All data in this database will be dropped while running tests. Use a separate one from your normal development db.

## Docker for Development and running tests
You need to have Docker and Docker Compose installed on your system.

To create your environment, run `make up` or `docker-compose up`.

You need an ODC database, so you'll need to refer to the [ODC docs](https://datacube-core.readthedocs.io/en/latest/) for help on indexing, but you can create the database by running `make initdb` or `docker-compose exec explorer datacube system init`. (This is not enough, you still need to add a product and index datasets.)

When you have some ODC data indexed, you can run `make index` to create the Explorer indexes.

Once Explorer indexes have been created, you can browse the running application at [http://localhost:5000](http://localhost:5000).

You can run tests by first creating a test database `make create-test-db-docker` and then running tests with `make test-docker`.

And you can run a single test in Docker using a command like this: ` docker-compose --file docker-compose.yml run explorer pytest integration_tests/test_dataset_listing.py`
