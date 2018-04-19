import shutil
import sys
import tempfile
from pathlib import Path
from typing import List, Sequence, Tuple

import click
import structlog
from click import echo, secho, style

import cubedash._model as dash
from datacube.model import DatasetType

_LOG = structlog.get_logger()


# pylint: disable=broad-except
def generate_reports(products: Sequence[DatasetType],
                     summaries_dir: Path = None) -> Tuple[int, int]:
    echo(f"Updating {len(products)} products in "
         f"{style(str(dash.get_summary_path()), bold=True)}", err=True)

    completed = 0
    failures = 0

    for product in products:
        echo(f'\t{product.name}....', nl=False, err=True)
        final_path = dash.get_summary_path(product.name, summaries_dir=summaries_dir)
        if final_path.exists():
            echo('exists', err=True)
            continue

        tmp_dir = Path(
            tempfile.mkdtemp(prefix='.dash-report-', dir=str(final_path.parent))
        )
        try:
            dash.write_product_summary(
                product,
                tmp_dir
            )
            tmp_dir.rename(final_path)
            secho('done', fg='green', err=True)
            completed += 1
        except Exception:
            _LOG.exception('report.generate', product=product.name, exc_info=True)
            secho('error', fg='yellow', err=True)
            failures += 1
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)

    echo("\tregenerating total....", nl=False, err=True)
    dash.generate_summary()

    secho(f'done. '
          f'{completed}/{len(products)} generated, '
          f'{failures} failures',
          fg='red' if failures else 'green', err=True)
    _LOG.info('completed', count=len(products), generated=completed, failures=failures)

    return completed, failures


def _load_products(product_names) -> List[DatasetType]:
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
@click.argument('product_names',
                nargs=-1)
def cli(generate_all_products: bool, summaries_dir: str, product_names: List[str]):
    if generate_all_products:
        products = sorted(dash.index.products.get_all(), key=lambda p: p.name)
    else:
        products = list(_load_products(product_names))

    completed, failures = generate_reports(
        products,
        summaries_dir=Path(summaries_dir) if summaries_dir else None
    )
    sys.exit(failures)


if __name__ == '__main__':
    cli()
