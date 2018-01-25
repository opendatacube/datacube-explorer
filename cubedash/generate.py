import sys
from typing import List

from click import echo, secho, style

from cubedash._model import index, write_product_summary, get_summary_path
from datacube.model import DatasetType


def generate_reports(product_names):
    products = _load_products(product_names)

    echo(f"Updating {len(products)} products in {style(str(get_summary_path()), bold=True)}", err=True)
    for product in products:
        echo(f'\t{product.name}....', nl=False, err=True)
        write_product_summary(
            product,
            get_summary_path(product.name)
        )
        secho('done', fg='green', err=True)


def _load_products(product_names) -> List[DatasetType]:
    products = []
    if len(product_names) == 1 and product_names[0] == '--all':
        products.extend(sorted(index.products.get_all(), key=lambda p: p.name))
    else:
        for product_name in product_names:
            product = index.products.get_by_name(product_name)
            if product:
                products.append(product)
            else:
                echo(f'Unknown product {style(repr(product_name), bold=True)}', err=True)

    return products


if __name__ == '__main__':
    generate_reports(sys.argv[1:])
