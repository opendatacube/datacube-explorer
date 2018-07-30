import multiprocessing
import sys
from typing import List, Sequence, Tuple

import click
import structlog
from click import echo, secho, style

from cubedash.logs import init_logging
from cubedash.summary import SummaryStore, TimePeriodOverview
from datacube.config import LocalConfig
from datacube.index import index_connect, Index
from datacube.model import DatasetType
from datacube.ui.click import pass_config, environment_option

_LOG = structlog.get_logger()


# pylint: disable=broad-except
def generate_report(item):
    config: LocalConfig
    product_name: str
    config, product_name = item
    log = _LOG.bind(product=product_name)

    store = _get_store(config, product_name, log=log)
    try:
        product = store.index.products.get_by_name(product_name)
        if product is None:
            raise ValueError(f"Unknown product: {product_name}")

        log.info('generate.product.init')
        store.init_product(product)
        log.info('generate.product.init.done')

        log.info('generate.product')
        updated = store.get_or_update(product.name, None, None, None)
        log.info('generate.product.done')

        return product_name, updated
    except Exception:
        log.exception('generate.product.error', exc_info=True)
        return product_name, None
    finally:
        store.index.close()


def _get_store(config: LocalConfig, variant: str, log=_LOG) -> SummaryStore:
    index: Index = index_connect(
        config,
        application_name=f'cubedash.generate.{variant}',
        validate_connection=False
    )
    store = SummaryStore(index, log=log)
    return store


def run_generation(
        config: LocalConfig,
        products: Sequence[DatasetType],
        store: SummaryStore,
        workers=3) -> Tuple[int, int]:
    echo(f"Updating {len(products)} products for "
         f"{style(str(config), bold=True)}", err=True)

    completed = 0
    failures = 0

    echo("Initialising store...", err=True, nl=False)
    # We don't init products, as we'll do them individually in the workers below.
    store.init(init_products=False)
    secho("done", fg='green', err=True)

    echo("Generating product summaries...", err=True)
    with multiprocessing.Pool(workers) as pool:
        product: DatasetType
        summary: TimePeriodOverview

        for product_name, summary in pool.imap_unordered(
                generate_report,
                ((config, p.name) for p in products),
                chunksize=1
        ):
            if summary is None:
                echo(f"{style(product_name, fg='yellow')} error (see log)", err=True)
                failures += 1
            else:
                echo(f"{style(product_name, fg='green')} done: "
                     f"({summary.dataset_count} datasets)", err=True)
                completed += 1

        pool.close()
        pool.join()

    # if completed > 0:
    #     echo("\tregenerating totals....", nl=False, err=True)
    #     store.update(None, None, None, None, generate_missing_children=False)

    secho(f'done. '
          f'{completed}/{len(products)} generated, '
          f'{failures} failures',
          fg='red' if failures else 'green', err=True)
    _LOG.info('completed', count=len(products), generated=completed, failures=failures)
    return completed, failures


def _load_products(index: Index, product_names) -> List[DatasetType]:
    for product_name in product_names:
        product = index.products.get_by_name(product_name)
        if product:
            yield product
        else:
            echo(f'Unknown product {style(repr(product_name), bold=True)}', err=True)


@click.command()
@environment_option
@pass_config
@click.option('--all',
              'generate_all_products',
              is_flag=True,
              default=False)
@click.option('-v', '--verbose', is_flag=True)
@click.option('-j', '--jobs',
              type=int,
              default=3,
              help="Number of worker processes to use")
@click.option('-l', '--event-log-file',
              help="Output jsonl logs to file",
              type=click.Path(writable=True, dir_okay=True))
@click.argument('product_names',
                nargs=-1)
def cli(config: LocalConfig,
        generate_all_products: bool,
        jobs: int,
        product_names: List[str],
        event_log_file: str,
        verbose: bool):
    """
    Generate summary files for the given products
    """
    init_logging(
        open(event_log_file, 'a') if event_log_file else None,
        verbose=verbose
    )

    store = _get_store(config, 'setup')

    if generate_all_products:
        products = sorted(store.index.products.get_all(), key=lambda p: p.name)
    else:
        products = list(_load_products(store.index, product_names))

    completed, failures = run_generation(
        config,
        products,
        store=store,
        workers=jobs,
    )
    sys.exit(failures)


if __name__ == '__main__':
    cli()
