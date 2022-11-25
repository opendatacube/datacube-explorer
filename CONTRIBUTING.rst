How to contribute to Datacube-explorer
======================================

Thank you for considering contributing to Datacube-explorer!

Community
---------

This project welcomes community participation.

`Join the ODC Slack <http://slack.opendatacube.org>`__ if you need help
setting up or using this project, or the Open Data Cube more generally.
Conversation about datacube-explorer is mostly concentrated in the Slack
channel ``#explorer``.

Please help us to keep the Open Data Cube community open and inclusive by
reading and following our `Code of Conduct <code-of-conduct.md>`__.

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/opendatacube/datacube-explorer/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug"
and "help wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

datacube-explorer could always use more documentation, whether as part of the
official datacube-explorer docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/opendatacube/datacube-ows/issues .

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Reporting issues
----------------

Include the following information in your post:

-   Describe what you expected to happen.
-   If possible, include a `minimal reproducible example`_ to help us
    identify the issue. This also helps check that the issue is not with
    your own code.
-   Describe what actually happened. Include the full traceback if there
    was an exception.
-   List your Python and Datacube-explorer versions. If possible, check if this
    issue is already fixed in the latest releases or the latest code in
    the repository.

.. _minimal reproducible example: https://stackoverflow.com/help/minimal-reproducible-example


Submitting patches
------------------

If there is not an open issue for what you want to submit, prefer
opening one for discussion before working on a PR. You can work on any
issue that doesn't have an open PR linked to it or a maintainer assigned
to it. These show up in the sidebar. No need to ask if you can work on
an issue that interests you.

Include the following in your patch:

-   Use `Black`_ to format your code. This and other tools will run
    automatically if you install `pre-commit`_ using the instructions
    below.
-   Include tests if your patch adds or changes code. Make sure the test
    fails without your patch.
-   Update any relevant docs pages and docstrings. Docs pages and
    docstrings should be wrapped at 72 characters.
-   Add an entry in ``CHANGES.rst``. Use the same style as other
    entries. Also include ``.. versionchanged::`` inline changelogs in
    relevant docstrings.

.. _Black: https://black.readthedocs.io
.. _pre-commit: https://pre-commit.com


First time setup
~~~~~~~~~~~~~~~~

-   Download and install the `latest version of git`_.
-   Configure git with your `username`_ and `email`_.

    .. code-block:: text

        $ git config --global user.name 'your name'
        $ git config --global user.email 'your email'

-   Make sure you have a `GitHub account`_.
-   Fork Flask to your GitHub account by clicking the `Fork`_ button.
-   `Clone`_ the main repository locally.

    .. code-block:: text

        $ git clone https://github.com/opendatacube/datacube-explorer.git
        $ cd datacube-explorer

-   Add your fork as a remote to push your work to. Replace
    ``{username}`` with your username. This names the remote "fork", the
    default Pallets remote is "origin".

    .. code-block:: text

        $ git remote add fork https://github.com/{username}/datacube-explorer

-   Create a virtualenv.


    - Linux/macOS

      .. code-block:: text

         $ python3 -m venv env
         $ . env/bin/activate

    - Windows

      .. code-block:: text

         > py -3 -m venv env
         > env\Scripts\activate

-   Upgrade pip and setuptools.

    .. code-block:: text

        $ python -m pip install --upgrade pip setuptools

-   Install the development dependencies, then install Flask in editable
    mode.

    .. code-block:: text

        $ pip install -r requirements/dev.txt && pip install -e .

-   Install the pre-commit hooks.

    .. code-block:: text

        $ pre-commit install

.. _latest version of git: https://git-scm.com/downloads
.. _username: https://docs.github.com/en/github/using-git/setting-your-username-in-git
.. _email: https://docs.github.com/en/github/setting-up-and-managing-your-github-user-account/setting-your-commit-email-address
.. _GitHub account: https://github.com/join
.. _Fork: https://github.com/opendatacube/datacube-explorer.git
.. _Clone: https://docs.github.com/en/github/getting-started-with-github/fork-a-repo#step-2-create-a-local-clone-of-your-fork


Start coding
~~~~~~~~~~~~

-   Create a branch to identify the issue you would like to work on. If
    you're submitting a bug or documentation fix, branch off of the
    latest ".x" branch.

    .. code-block:: text

        $ git fetch origin
        $ git checkout -b your-branch-name origin/2.0.x

    If you're submitting a feature addition or change, branch off of the
    "main" branch.

    .. code-block:: text

        $ git fetch origin
        $ git checkout -b your-branch-name origin/main

-   Using your favorite editor, make your changes,
    `committing as you go`_.
-   Include tests that cover any code changes you make. Make sure the
    test fails without your patch. Run the tests as described below.
-   Push your commits to your fork on GitHub and
    `create a pull request`_. Link to the issue being addressed with
    ``fixes #123`` in the pull request.

    .. code-block:: text

        $ git push --set-upstream fork your-branch-name

.. _committing as you go: https://dont-be-afraid-to-commit.readthedocs.io/en/latest/git/commandlinegit.html#commit-your-changes
.. _create a pull request: https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request


Pre-commit setup
~~~~~~~~~~~~~~~~

A `pre-commit <https://pre-commit.com/>`__ config is provided to automatically format
and check your code changes. This allows you to immediately catch and fix
issues before you raise a failing pull request (which run the same checks under
Travis).

Install pre-commit from pip, and initialise it in your repo:

.. code-block:: text

    pip install pre-commit
    pre-commit install

Your code will now be formatted and validated before each commit. You can also
invoke it manually by running ``pre-commit run``


Running the tests
~~~~~~~~~~~~~~~~~

Run the basic test suite with pytest.

.. code-block:: text

    $ pytest

This runs the tests for the current environment, which is usually
sufficient. CI will run the full suite when you submit your pull
request. You can run the full test suite with tox if you don't want to
wait.

.. code-block:: text

    $ tox


How do I modify the css/javascript?
---------------------------------------

The CSS is compiled from `Sass <https://sass-lang.com/>`__ , and the Javascript is compiled from
`Typescript <https://www.typescriptlang.org/>`__

Install `npm <https://www.npmjs.com/get-npm>`__, and then install them both:

.. code-block:: text

    npm install -g sass typescript

You can now run ``make static`` to rebuild all the static files, or
individually with ``make style`` or ``make js``.

Alternatively, if using `PyCharm <https://www.jetbrains.com/pycharm>`__, open a
Sass file and you will be prompted to enable a `File Watcher` to
compile automatically.

PyCharm will also compile the Typescript automatically by ticking
the "Recompile on changes" option in ``Languages & Frameworks ->
Typescript``.


Integration tests
-----------------

The integration tests run against a real postgres database, which is dropped and
recreated between each test method:

Install the test dependencies: ``pip install -e .[test]``

Simple test setup
~~~~~~~~~~~~~~~~~~~

Set up a database on localhost that doesn't prompt for a password locally (eg. add credentials to ``~/.pgpass``)

Then: ``createdb dea_integration``

And the tests should be runnable with no configuration: ``pytest integration_tests``

Setting up product and dataset for new tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inside https://github.com/opendatacube/datacube-explorer/tree/develop/integration_tests/data there are three folders, `ingestions`, `metadata` and `products`. For integration test to include a new metadata yaml, product yaml or ingestion yaml place the yaml files in the corresponding folders.

Then, to add sample datasets required for the test case, create a `.yaml` file with the product name and place all the sample datasets split by `---` in the yaml. Then at the beginning of the new `test_xyz.py` file place

.. code-block:: python

    from pathlib import Path

    import pytest
    from datacube.index.hl import Doc2Dataset
    from datacube.utils import read_documents

    TEST_DATA_DIR = Path(__file__).parent / "data"


    @pytest.fixture(scope="module", autouse=True)
    def populate_index(dataset_loader, module_dea_index):
        """
        Index populated with example datasets. Assumes our tests wont modify the data!

        It's module-scoped as it's expensive to populate.
        """
        dataset_count = 0
        create_dataset = Doc2Dataset(module_dea_index)
        for _, s2_dataset_doc in read_documents(TEST_DATA_DIR / "s2_l2a-sample.yaml"):
            try:
                dataset, err = create_dataset(
                    s2_dataset_doc, "file://example.com/test_dataset/"
                )
                assert dataset is not None, err
                created = module_dea_index.datasets.add(dataset)
                assert created.type.name == "s2_l2a"
                dataset_count += 1
            except AttributeError as ae:
                assert dataset_count == 5
                print(ae)
            assert dataset_count == 5
        return module_dea_index


if the sample dataset yaml file is too big, run `gzip **yaml**` and append the required `yaml.gz` to `conftest.py` `populated_index` fixture

.. code-block:: python

    from pathlib import Path

    import pytest

    TEST_DATA_DIR = Path(__file__).parent / "data"


    @pytest.fixture(scope="module")
    def populated_index(dataset_loader, module_dea_index):
        loaded = dataset_loader(
            "pq_count_summary", TEST_DATA_DIR / "pq_count_summary.yaml.gz"
        )
        assert loaded == 20
        return module_dea_index


Custom test configuration (using other hosts, postgres servers)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a ``.datacube_integration.conf`` file to your home directory in the same format as
`datacube config files <https://datacube-core.readthedocs.io/en/latest/user/config.html#runtime-config>`__

(You might already have one if you run datacube's integration tests)

Then run pytest: ``pytest integration_tests``

__Warning__ All data in this database will be dropped while running tests. Use a separate one from your normal development db.

Docker for Development and running tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You need to have Docker and Docker Compose installed on your system.

To create your environment, run `make up` or `docker-compose up`.

You need an ODC database, so you'll need to refer to the `ODC docs <https://datacube-core.readthedocs.io/en/latest/>`__ for help on indexing, but you can create the database by running ``make initdb`` or ``docker-compose exec explorer datacube system init``. (This is not enough, you still need to add a product and index datasets.)

When you have some ODC data indexed, you can run ``make index`` to create the Explorer indexes.

Once Explorer indexes have been created, you can browse the running application at `http://localhost:5000 <http://localhost:5000>`__

You can run tests by first creating a test database ``make create-test-db-docker`` and then running tests with ``make test-docker``.

And you can run a single test in Docker using a command like this: ``docker-compose --file docker-compose.yml run explorer pytest integration_tests/test_dataset_listing.py``


Docker-compose for Development and running tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

edit `.docker/settings_docker.py` and setup application config.
Then `docker-compose -f docker-compose.yml -f docker-compose.override.yml up` to bring up explorer docker with database, explorer with settings



Building the docs
~~~~~~~~~~~~~~~~~

Build the docs in the ``docs`` directory using Sphinx.

.. code-block:: text

    $ python3 -m pip install sphinx datacube-explorer
    $ cd docs
    $ make html

Open ``_build/html/index.html`` in your browser to view the docs.

Read more about `Sphinx <https://www.sphinx-doc.org/en/stable/>`__.



Generating database relationship diagram
----------------------------------------

.. code-block:: console

    docker run -it --rm -v "$PWD:/output" --network="host" schemaspy/schemaspy:snapshot -u $DB_USERNAME -host localhost -port $DB_PORT -db $DB_DATABASE -t pgsql11 -schemas cubedash -norows -noviews -pfp -imageformat svg

Merge relationship diagram and orphan diagram

.. code-block:: console

    python3 svg_stack.py --direction=h --margin=100 ../cubedash/diagrams/summary/relationships.real.large.svg ../cubedash/diagrams/orphans/orphans.svg > explorer.merged.large.svg

    cp svg_stack/explorer.merged.large.svg ../datacube-explorer/docs/diagrams/db-relationship-diagram.svg
