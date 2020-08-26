import sys
import urllib.request
from textwrap import indent
from urllib.error import HTTPError
from urllib.parse import urljoin

import click
from click import secho

from cubedash.summary import RegionInfo
from datacube.index import Index
from datacube.ui.click import config_option, environment_option, pass_index


def find_examples_of_all_public_urls(index: Index):

    yield "/"
    yield "/about"
    yield "/products.txt"
    yield "/product-audit/"
    yield "/product-audit/day-times.txt"

    for mdt in index.metadata_types.get_all():
        name = mdt.name
        yield f"/metadata-type/{name}"
        yield f"/metadata-type/{name}.odc-type.yaml"

    for dt in index.products.get_all():
        name = dt.name
        yield f"/{name}"
        yield f"/datasets/{name}"
        yield f"/product/{name}"
        yield f"/product/{name}.odc-product.yaml"

        has_datasets = index.datasets.search_eager(product=name, limit=1)
        if has_datasets:
            dataset = has_datasets[0]
            time = dataset.center_time
            yield f"/{name}/{time:%Y}"
            yield f"/{name}/{time:%Y/%m}"
            yield f"/{name}/{time:%Y/%m/%d}"
            yield f"/datasets/{name}/{time:%Y}"
            yield f"/datasets/{name}/{time:%Y/%m}"
            yield f"/datasets/{name}/{time:%Y/%m/%d}"

            yield f"/api/datasets/{name}"
            yield f"/api/footprint/{name}/{time:%Y/%m/%d}"

            region_info = RegionInfo.for_product(dt)
            if region_info is not None:
                region_code = region_info.dataset_region_code(dataset)
                if region_code is not None:
                    yield f"/api/regions/{name}/{time:%Y/%m/%d}"

                    yield f"/region/{name}/{region_code}"
                    yield f"/region/{name}/{region_code}/{time:%Y/%m/%d}"

    for [dataset_id] in index.datasets.search_returning(("id",), limit=10):
        yield f"/dataset/{dataset_id}"
        yield f"/dataset/{dataset_id}.odc-metadata.yaml"


@click.command()
@environment_option
@config_option
@pass_index(app_name="explorer-warmup")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show entire contents of failures instead of the header",
)
@click.option(
    "--url",
    "explorer_url",
    default="http://localhost:8080",
    help="URL of Explorer to call",
)
@click.option(
    "-x",
    "--max-failures",
    "max_failures",
    type=int,
    default=1,
    help="Exit immediately when reaching this many failures (-1 for never)",
)
def cli(index: Index, verbose: bool, max_failures: bool, explorer_url: str):
    """
    A tool to load an example of each Explorer page, reporting if any
    return errors.

    It uses an underlying datacube to get lists of things to try.

    Returns error count.
    """
    failures = 0
    max_failure_line_count = sys.maxsize if verbose else 5

    for url_offset in find_examples_of_all_public_urls(index):
        url = urljoin(explorer_url, url_offset)
        secho(f"get {url_offset} ", bold=True, nl=False, err=True)
        try:
            with urllib.request.urlopen(url) as _:
                secho("ok", fg="green", err=True)
        except HTTPError as e:
            secho(f"fail {e.code}", fg="red", err=True)
            page_sample = "\n".join(
                s.decode("utf-8") for s in e.readlines()[:max_failure_line_count]
            )
            secho(indent(page_sample, " " * 4))
            failures += 1
            if failures == max_failures:
                secho(f"(hit max failures {max_failures})", fg="yellow")
                break

    sys.exit(failures)


if __name__ == "__main__":
    cli()
