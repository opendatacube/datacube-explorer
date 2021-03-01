#!/usr/bin/env python3
"""
A tool to generate and update Explorer's caches of datacube data.

Explorer's view of ODC data is too expensive to calculate on
every page load (or API call), so Explorer uses its own tables
to calculate this summary information ahead-of-time.

The cubedash-gen command creates these summaries in a schema called
`cubedash`, separate from datacube’s own schema.

This can be re-run each time changes are made to an ODC product
to show the updates in Explorer.

The first run will be slow as it will scan all data, but later
runs will be faster as it scans only modified data.
(unless `--force-refresh` is specified)

---

Datacube config

To choose which datacube to point to, it takes identical datacube
config (-C) and environment (-E) options as the `datacube` command,
and reads identical datacube config files and environment variables.

ie. It will use the datacube that is shown by running the command
`datacube system check`

See datacube’s own docs for this configuration handling.

---

Examples

Create Explorer's schemas, then update (or create) all product summaries:

    cubedash-gen --init --all

Update (or create) all product summaries:

    cubedash-gen --all

Recreate all information for two products:

    cubedash-gen --force-refresh ls8_nbart_scene ls8_level1_scene


Drop all of Explorer’s additions to the database:

    cubedash-gen --drop


"""
import collections
import multiprocessing
import sys
from functools import partial
from textwrap import dedent
from typing import List, Sequence, Tuple, Optional

import click
import structlog
from click import secho as click_secho
from click import style

from cubedash.logs import init_logging
from cubedash.summary import SummaryStore, TimePeriodOverview
from cubedash.summary._stores import GenerateResult
from datacube.config import LocalConfig
from datacube.index import Index, index_connect
from datacube.model import DatasetType
from datacube.ui.click import config_option, environment_option, pass_config

# Machine (json) logging.
_LOG = structlog.get_logger()

# Interactive messages for a human go to stderr.
user_message = partial(click_secho, err=True)


# pylint: disable=broad-except
def generate_report(
    item: Tuple[LocalConfig, str, bool, bool]
) -> Tuple[str, GenerateResult, Optional[TimePeriodOverview]]:
    config, product_name, force_refresh, recreate_dataset_extents = item
    log = _LOG.bind(
        product=product_name, force=force_refresh, extents=recreate_dataset_extents
    )

    started_years = set()

    def print_status(product_name=None, year=None, month=None, day=None, summary=None):
        """Print status each time we start a year."""
        if year:
            if (product_name, year) not in started_years:
                user_message(f"\t  {product_name} {year}")
                started_years.add((product_name, year))

    store = SummaryStore.create(_get_index(config, product_name), log=log)
    store.add_change_listener(print_status)

    try:
        product = store.index.products.get_by_name(product_name)
        if product is None:
            raise ValueError(f"Unknown product: {product_name}")
        user_message(f"{product_name} refresh")
        result, updated_summary = store.refresh(
            product_name,
            force=force_refresh,
            recreate_dataset_extents=recreate_dataset_extents,
        )
        return product_name, result, updated_summary
    except Exception:
        log.exception("generate.product.error", exc_info=True)
        return product_name, GenerateResult.ERROR, None
    finally:
        store.index.close()


def _get_index(config: LocalConfig, variant: str) -> Index:
    # Avoid long names as they will print warnings all the time.
    short_name = variant.replace("_", "")[:20]
    index: Index = index_connect(
        config, application_name=f"gen.{short_name}", validate_connection=False
    )
    return index


def run_generation(
    config: LocalConfig,
    products: Sequence[DatasetType],
    workers=3,
    force_refresh: bool = False,
    recreate_dataset_extents: bool = False,
) -> Tuple[int, int]:
    user_message(
        f"Updating {len(products)} products for " f"{style(str(config), bold=True)}",
    )

    counts = collections.Counter()

    user_message("Generating product summaries...")

    def on_complete(
        product_name: str, result: GenerateResult, summary: TimePeriodOverview
    ):
        counts[result] += 1
        result_color = {
            GenerateResult.ERROR: "red",
            GenerateResult.CREATED: "blue",
            GenerateResult.UPDATED: "green",
        }.get(result)
        extra = ""
        if summary is not None:
            extra = f" (contains {summary.dataset_count} total datasets)"

        user_message(
            f"{style(product_name, fg=result_color)} {result.name.lower()}{extra}"
        )

    # If one worker, avoid any subprocesses/forking.
    # This makes test tracing far easier.
    if workers == 1:
        for p in products:
            on_complete(
                *generate_report(
                    (config, p.name, force_refresh, recreate_dataset_extents)
                )
            )
    else:
        with multiprocessing.Pool(workers) as pool:
            product: DatasetType
            summary: TimePeriodOverview
            for product_name, result, summary in pool.imap_unordered(
                generate_report,
                (
                    (config, p.name, force_refresh, recreate_dataset_extents)
                    for p in products
                ),
                chunksize=1,
            ):
                on_complete(product_name, result, summary)

        pool.close()
        pool.join()

    status_messages = ", ".join(
        f"{count_} {status.name.lower()}" for status, count_ in counts.items()
    )
    failure_count = counts[GenerateResult.ERROR]
    creation_count = counts[GenerateResult.CREATED] + counts[GenerateResult.UPDATED]

    user_message(
        f"finished. {status_messages}",
        fg="red" if failure_count else "green" if creation_count else None,
    )
    _LOG.info(
        "completed",
        count=len(products),
        **{f"was_{k.name.lower()}": count for k, count in counts.items()},
    )
    return creation_count, failure_count


def _load_products(index: Index, product_names) -> List[DatasetType]:
    for product_name in product_names:
        product = index.products.get_by_name(product_name)
        if product:
            yield product
        else:
            possible_product_names = "\n\t".join(
                p.name for p in index.products.get_all()
            )
            raise click.BadParameter(
                f"Unknown product {repr(product_name)}.\n\n"
                f"Possibilities:\n\t{possible_product_names}",
                param_hint="product_names",
            )


@click.command(help=__doc__)
@environment_option
@config_option
@pass_config
@click.option(
    "--all",
    "generate_all_products",
    is_flag=True,
    default=False,
    help="Refresh all products in the datacube, rather than the specified list.",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help=dedent(
        """\
        Enable all log messages, instead of just errors.

        Logging goes to stdout unless `--event-log-file` is specified.

        Logging is coloured plain-text if going to a tty, and jsonl format otherwise.

        Use twice to enable debug logging too.
        """
    ),
)
@click.option(
    "-j",
    "--jobs",
    type=int,
    default=3,
    help=dedent(
        """\
        Number of concurrent worker subprocesses to use (default: 3)

        This should match how many io-and-cpu-heavy queries your DB would
        like to handle concurrently.
    """
    ),
)
@click.option(
    "-l",
    "--event-log-file",
    help="Output log messages to file, in jsonl format",
    type=click.Path(writable=True, dir_okay=True),
)
@click.option(
    "--refresh-stats/--no-refresh-stats",
    is_flag=True,
    default=True,
    help=dedent(
        """\
        Refresh general statistics tables that cover all products (default: true)

        This can be slow, and only needs to be done once (at the end) if calling
        cubedash-gen repeatedly
        """
    ),
)
@click.option(
    "--force-refresh/--no-force-refresh",
    is_flag=True,
    default=False,
    help=dedent(
        """\
        Force all time periods to be regenerated, rather than just applying updates
        to existing ones.

        (default: false)
        """
    ),
)
@click.option(
    "--recreate-dataset-extents/--append-dataset-extents",
    is_flag=True,
    default=False,
    help=dedent(
        """\
        Rebuild Explorer's existing dataset extents even if they don't seem to be updated.
        (default: false)

        This is useful if you've patched datasets or products in-place with new geometry
        or regions.
        """
    ),
)
@click.option(
    "--force-concurrently",
    is_flag=True,
    default=False,
    help=dedent(
        """\
        Refresh materialised views concurrently in Postgres. (default: false)

        This will avoid taking any locks when updating statistics, but has some caveats,
        see https://www.postgresql.org/docs/10/sql-refreshmaterializedview.html
        """
    ),
)
@click.option(
    "--init-database/--no-init-database",
    "--init",
    default=False,
    help=dedent(
        """\
        Check the Explorer schema and create it (or apply updates to it) if needed,
        before doing anything else.
        """
    ),
)
@click.option(
    "--drop-database",
    "--drop",
    is_flag=True,
    default=False,
    help=dedent(
        """\
        Drop all of Explorer's database schema+caches and exit.
        """
    ),
)
@click.argument("product_names", nargs=-1)
def cli(
    config: LocalConfig,
    generate_all_products: bool,
    jobs: int,
    product_names: List[str],
    event_log_file: str,
    refresh_stats: bool,
    force_concurrently: bool,
    verbose: int,
    init_database: bool,
    drop_database: bool,
    force_refresh: bool,
    recreate_dataset_extents: bool,
):
    init_logging(
        open(event_log_file, "a") if event_log_file else None, verbosity=verbose
    )

    index = _get_index(config, "setup")
    store = SummaryStore.create(index)

    if drop_database:
        user_message("Dropping all Explorer additions to the database")
        store.drop_all()
        user_message("Done. Goodbye.")
        sys.exit(0)

    if init_database:
        user_message("Initialising schema")
        store.init()
    elif not store.is_initialised():
        user_message(
            style("No cubedash schema exists. ", fg="red")
            + "Please rerun with --init to create one",
        )
        sys.exit(-1)
    elif not store.is_schema_compatible(for_writing_operations_too=True):
        user_message(
            style("Cubedash schema is out of date. ", fg="red")
            + "Please rerun with --init to apply updates.",
        )
        sys.exit(-2)

    if generate_all_products:
        products = sorted(store.all_dataset_types(), key=lambda p: p.name)
    else:
        products = list(_load_products(store.index, product_names))

    updated, failures = run_generation(
        config,
        products,
        workers=jobs,
        force_refresh=force_refresh,
        recreate_dataset_extents=recreate_dataset_extents,
    )
    if updated > 0 and refresh_stats:
        user_message("Refreshing statistics...", nl=False)
        store.refresh_stats(concurrently=force_concurrently)
        user_message("done", color="green")
        _LOG.info("stats.refresh")
    sys.exit(failures)


if __name__ == "__main__":
    cli()
