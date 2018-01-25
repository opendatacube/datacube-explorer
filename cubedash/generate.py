import shutil
import sys
import tempfile
from pathlib import Path
from typing import List

from click import echo, secho, style

from cubedash._model import get_summary_path, index, write_product_summary
from datacube.model import DatasetType


def generate_reports(product_names):
    products = _load_products(product_names)

    echo(
        f"Updating {len(products)} products in {style(str(get_summary_path()), bold=True)}",
        err=True,
    )
    for product in products:
        echo(f"\t{product.name}....", nl=False, err=True)
        final_path = get_summary_path(product.name)
        if final_path.exists():
            echo("exists", err=True)
            continue

        tmp_dir = Path(
            tempfile.mkdtemp(prefix=".dash-report-", dir=str(final_path.parent))
        )
        try:
            write_product_summary(product, tmp_dir)
            tmp_dir.rename(final_path)
            secho("done", fg="green", err=True)
        except Exception:
            secho("error", fg="yellow", err=True)
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)


def _load_products(product_names) -> List[DatasetType]:
    products = []
    if len(product_names) == 1 and product_names[0] == "--all":
        products.extend(sorted(index.products.get_all(), key=lambda p: p.name))
    else:
        for product_name in product_names:
            product = index.products.get_by_name(product_name)
            if product:
                products.append(product)
            else:
                echo(
                    f"Unknown product {style(repr(product_name), bold=True)}", err=True
                )

    return products


if __name__ == "__main__":
    generate_reports(sys.argv[1:])
