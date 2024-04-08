import socket
import sys
import time
import urllib.request
from textwrap import indent
from typing import List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin

import click
from click import echo, secho, style
from datacube.index import Index
from datacube.ui.click import config_option, environment_option, pass_index

from cubedash.summary import RegionInfo


def find_examples_of_all_public_urls(index: Index):
    yield "/"
    yield "/arrivals"
    yield "/arrivals.csv"

    yield "/products.txt"
    yield "/products.odc-product.yaml"
    yield "/metadata-types.txt"
    yield "/metadata-types.odc-type.yaml"

    yield "/audit/datasets-metadata"
    yield "/audit/product-overview"
    yield "/audit/dataset-counts"
    yield "/audit/dataset-counts.csv"
    yield "/audit/storage"
    yield "/audit/storage.csv"
    yield "/audit/day-query-times.txt"

    yield "/stac"
    yield "/stac/collections"
    yield "/stac/catalogs/arrivals"
    yield "/stac/catalogs/arrivals/items"

    for mdt in index.metadata_types.get_all():
        name = mdt.name
        yield f"/metadata-types/{name}"
        yield f"/metadata-types/{name}.odc-type.yaml"

    for dt in index.products.get_all():
        name = dt.name
        yield f"/{name}"
        yield f"/datasets/{name}"
        yield f"/products/{name}"
        yield f"/products/{name}.odc-product.yaml"

        yield f"/stac/collections/{name}"
        yield f"/stac/collections/{name}/items"
        yield f"/stac/search?collection={name}&limit=1"
        yield f"/stac/search?collection={name}&limit=1&_full=true"

        has_datasets = index.datasets.search_eager(product=name, limit=1)
        if has_datasets:
            dataset = has_datasets[0]
            time = dataset.center_time

            yield f"/products/{name}/extents/{time:%Y}"
            yield f"/products/{name}/extents/{time:%Y/%m}"
            yield f"/products/{name}/extents/{time:%Y/%m/%d}"

            yield f"/products/{name}/datasets/{time:%Y}"
            yield f"/products/{name}/datasets/{time:%Y/%m}"
            yield f"/products/{name}/datasets/{time:%Y/%m/%d}"

            yield f"/api/datasets/{name}"
            yield f"/api/footprint/{name}/{time:%Y/%m/%d}"

            region_info = RegionInfo.for_product(dt)
            if region_info is not None:
                region_code = region_info.dataset_region_code(dataset)
                if region_code is not None:
                    yield f"/api/regions/{name}/{time:%Y/%m/%d}"

                    yield f"/product/{name}/regions/{region_code}"
                    yield f"/product/{name}/regions/{region_code}.geojson"
                    yield f"/product/{name}/regions/{region_code}/{time:%Y/%m/%d}"

            yield f"/dataset/{dataset.id}"
            yield f"/dataset/{dataset.id}.odc-metadata.yaml"
            yield f"/stac/collections/{name}/items/{dataset.id}"


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
    "--timeout",
    "timeout_seconds",
    default=120,
    type=int,
    help="Query timeout (seconds)",
)
@click.option(
    "-s",
    "--throttle-seconds",
    default=0,
    type=float,
    help="Sleep for this long between requests (seconds)",
)
@click.option(
    "-x",
    "--max-failures",
    "max_failures",
    type=int,
    default=3,
    help="Exit immediately when reaching this many consecutive failures (-1 for never)",
)
def cli(
    index: Index,
    verbose: bool,
    max_failures: int,
    timeout_seconds: int,
    throttle_seconds: float,
    explorer_url: str,
    show_timings: int = 5,
):
    """
    A tool to load an example of each Explorer page, reporting if any
    return errors.

    It uses an underlying datacube to get lists of things to try.

    Returns error count.
    """
    max_failure_line_count = sys.maxsize if verbose else 5
    response_times: List[Tuple[float, str]] = []
    failures = []
    consecutive_failures = 0

    for url_offset in find_examples_of_all_public_urls(index):
        url = urljoin(explorer_url, url_offset)
        secho(f"get {url_offset} ", bold=True, nl=False, err=True)

        def handle_failure():
            nonlocal consecutive_failures
            consecutive_failures += 1
            failures.append(url)
            # Back off slightly for network hiccups.
            time.sleep(max(throttle_seconds, 1) * (consecutive_failures + 1))

        try:
            start_time = time.time()
            with urllib.request.urlopen(url, timeout=timeout_seconds) as _:
                finished_in = time.time() - start_time
                echo(
                    f"{style('ok', fg='green')} ({_format_time(finished_in)})", err=True
                )
            consecutive_failures = 0
            response_times.append((finished_in, url))
            time.sleep(throttle_seconds)
        except socket.timeout:
            secho(f"timeout (> {timeout_seconds}s)", fg="magenta", err=True)
            handle_failure()
        except HTTPError as e:
            secho(f"fail {e.code}", fg="red", err=True)
            page_sample = "\n".join(
                s.decode("utf-8") for s in e.readlines()[:max_failure_line_count]
            )
            secho(indent(page_sample, " " * 4))
            handle_failure()
        except URLError as e:
            secho(f"connection error {e.reason}", fg="magenta", err=True)
            handle_failure()

        if consecutive_failures == max_failures:
            secho(f"(hit max consecutive failures {max_failures})", fg="yellow")
            break

    if response_times:
        secho()
        secho("Slowest responses:")
        for i, (response_secs, url) in enumerate(sorted(response_times, reverse=True)):
            if i > show_timings:
                break
            secho(f"\t{_format_time(response_secs)}\t{url}")

    if len(failures):
        secho()
        secho(f"{len(failures)} failures", fg="red")

    sys.exit(len(failures))


def _format_time(t: float):
    """
    >>> _format_time(0.31)
    '310ms'
    >>> _format_time(3.0)
    '3.0s'
    >>> # A bit unix-specific? Show with orange
    >>> _format_time(30.3234234)
    '\\x1b[33m30.3s\\x1b[0m'
    """
    # More than a minute? red
    if t > 60:
        return style(f"{t:.1f}s", fg="red")
    # More than five seconds? orange
    if t > 5:
        return style(f"{t:.1f}s", fg="yellow")
    if t > 1:
        return f"{t:.1f}s"
    else:
        return f"{int(t*1000)}ms"


if __name__ == "__main__":
    cli()
