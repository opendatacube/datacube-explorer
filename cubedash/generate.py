#!/usr/bin/env python3

import multiprocessing
import sys
from datetime import timedelta
from functools import partial
from textwrap import dedent
from typing import List, Sequence, Tuple, Optional

import click
import structlog
from click import secho as click_secho
from click import style

from cubedash.logs import init_logging
from cubedash.summary import SummaryStore, TimePeriodOverview
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
) -> Tuple[str, Optional[TimePeriodOverview]]:
    config, product_name, force_refresh, recreate_dataset_extents = item
    log = _LOG.bind(
        product=product_name, force=force_refresh, extents=recreate_dataset_extents
    )

    store = SummaryStore.create(_get_index(config, product_name), log=log)
    try:
        product = store.index.products.get_by_name(product_name)
        if product is None:
            raise ValueError(f"Unknown product: {product_name}")

        log.info("generate.product.refresh")
        store.refresh_product(
            product,
            refresh_older_than=(
                timedelta(minutes=-1) if force_refresh else timedelta(days=1)
            ),
            force_dataset_extent_recompute=recreate_dataset_extents,
        )
        log.info("generate.product.refresh.done")

        log.info("generate.product")
        updated = store.get_or_update(product.name, force_refresh=force_refresh)
        log.info("generate.product.done")

        return product_name, updated
    except Exception:
        log.exception("generate.product.error", exc_info=True)
        return product_name, None
    finally:
        store.index.close()


def _get_index(config: LocalConfig, variant: str) -> Index:
    index: Index = index_connect(
        config, application_name=f"dashgen.{variant}", validate_connection=False
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

    counts = {"complete": 0, "failure": 0}

    user_message("Generating product summaries...")

    def on_complete(product_name: str, summary: TimePeriodOverview):
        if summary is None:
            user_message(f"{style(product_name, fg='yellow')} error (see log)")
            counts["failure"] += 1
        else:
            user_message(
                f"{style(product_name, fg='green')} done: "
                f"({summary.dataset_count} datasets)",
            )
            counts["complete"] += 1

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
            for product_name, summary in pool.imap_unordered(
                generate_report,
                (
                    (config, p.name, force_refresh, recreate_dataset_extents)
                    for p in products
                ),
                chunksize=1,
            ):
                on_complete(product_name, summary)

        pool.close()
        pool.join()

    # if completed > 0:
    #     echo("\tregenerating totals....", nl=False, err=True)
    #     store.update(None, None, None, None, generate_missing_children=False)

    completed, failures = counts["complete"], counts["failure"]
    user_message(
        f"done. " f"{completed}/{len(products)} generated, " f"{failures} failures",
        fg="red" if failures else "green",
    )
    _LOG.info("completed", count=len(products), generated=completed, failures=failures)
    return completed, failures


def _load_products(index: Index, product_names) -> List[DatasetType]:
    for product_name in product_names:
        product = index.products.get_by_name(product_name)
        if product:
            yield product
        else:
            raise click.BadParameter(
                f"Unknown product {repr(product_name)}", param_hint="product_names"
            )


@click.command()
@environment_option
@config_option
@pass_config
@click.option("--all", "generate_all_products", is_flag=True, default=False)
@click.option("-v", "--verbose", is_flag=True)
@click.option(
    "-j", "--jobs", type=int, default=3, help="Number of worker processes to use"
)
@click.option(
    "-l",
    "--event-log-file",
    help="Output jsonl logs to file",
    type=click.Path(writable=True, dir_okay=True),
)
@click.option("--refresh-stats/--no-refresh-stats", is_flag=True, default=True)
@click.option("--force-refresh/--no-force-refresh", is_flag=True, default=False)
@click.option(
    "--recreate-dataset-extents/--append-dataset-extents",
    is_flag=True,
    default=False,
    help=dedent(
        """\
        Rebuild Explorer's existing dataset extents rather than appending new datasets.

        This is useful if you've patched datasets or products in-place with new geometry
        or regions.
        """
    ),
)
@click.option("--force-concurrently", is_flag=True, default=False)
@click.option(
    "--init-database/--no-init-database",
    "--init",
    default=False,
    help="Prepare the database for use by datacube explorer",
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
    verbose: bool,
    init_database: bool,
    force_refresh: bool,
    recreate_dataset_extents: bool,
):
    """
    Generate summary files for the given products
    """
    init_logging(open(event_log_file, "a") if event_log_file else None, verbose=verbose)

    index = _get_index(config, "setup")
    store = SummaryStore.create(index)

    if init_database:
        user_message("Initialising schema")
        store.init()
    elif not store.is_initialised():
        user_message(
            style("No cubedash schema exists. ", fg="red")
            + "Please rerun with --init to create one",
        )
        sys.exit(-1)
    elif not store.is_schema_compatible():
        user_message(
            style("Cubedash schema is out of date. ", fg="red")
            + "Please rerun with --init to apply updates.",
        )
        sys.exit(-2)

    if generate_all_products:
        products = sorted(store.all_dataset_types(), key=lambda p: p.name)
    else:
        products = list(_load_products(store.index, product_names))

    completed, failures = run_generation(
        config,
        products,
        workers=jobs,
        force_refresh=force_refresh,
        recreate_dataset_extents=recreate_dataset_extents,
    )
    if refresh_stats:
        user_message("Refreshing statistics...", nl=False)
        store.refresh_stats(concurrently=force_concurrently)
        user_message("done", color="green")
        _LOG.info("stats.refresh")
    sys.exit(failures)


if __name__ == "__main__":
    cli()
