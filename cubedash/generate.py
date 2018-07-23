import sys
from pathlib import Path
from typing import List, Sequence, Tuple

import click
import structlog
from click import echo, secho, style

import cubedash._model as dash
from cubedash.logs import init_logging
from cubedash.summary import SummaryStore
from cubedash.summary._stores import PgSummaryStore
from datacube.model import DatasetType

_LOG = structlog.get_logger()


# pylint: disable=broad-except
def generate_reports(products: Sequence[DatasetType],
                     store: SummaryStore) -> Tuple[int, int]:
    echo(f"Updating {len(products)} products in "
         f"{style(repr(store), bold=True)}", err=True)

    completed = 0
    failures = 0

    for product in products:
        echo(f'\t{product.name}....', nl=False, err=True)
        if store.has(product.name, None, None, None):
            echo('exists', err=True)
            continue

        try:
            store.get_or_update(product.name, None, None, None)
            secho('done', fg='green', err=True)
            completed += 1
        except Exception:
            _LOG.exception('report.generate', product=product.name, exc_info=True)
            secho('error', fg='yellow', err=True)
            failures += 1

    echo("\tregenerating total....", nl=False, err=True)
    store.update(None, None, None, None)

    secho(f'done. '
          f'{completed}/{len(products)} generated, '
          f'{failures} failures',
          fg='red' if failures else 'green', err=True)
    _LOG.info('completed', count=len(products), generated=completed, failures=failures)

    return completed, failures


def _load_products(product_names) -> List[DatasetType]:
    products = dash.index.products.get_all()
    for product_name in product_names:
        product = dash.index.products.get_by_name(product_name)
        if product:
            yield product
        else:
            echo(f'Unknown product {style(repr(product_name), bold=True)}', err=True)


@click.command()
@click.option('--all',
              'generate_all_products',
              is_flag=True,
              default=False)
@click.option('--summaries-dir',
              type=click.Path(exists=True, file_okay=False),
              default=None)
@click.option('-v', '--verbose', is_flag=True)
@click.option('-l', '--event-log-file',
              help="Output jsonl logs to file",
              type=click.Path(writable=True, dir_okay=True))
@click.argument('product_names',
                nargs=-1)
def cli(generate_all_products: bool,
        summaries_dir: str,
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

    if generate_all_products:
        products = sorted(dash.index.products.get_all(), key=lambda p: p.name)
    else:
        products = list(_load_products(product_names))

    if summaries_dir:
        store = PgSummaryStore(dash.index)
    else:
        store = dash.DEFAULT_STORE

    completed, failures = generate_reports(products, store=store)
    sys.exit(failures)


if __name__ == '__main__':
    cli()
