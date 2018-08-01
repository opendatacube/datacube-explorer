import time
from typing import Counter

import click
import structlog
from click import echo, secho

from cubedash import _utils
from cubedash._filters import sizeof_fmt
from cubedash.logs import init_logging
from cubedash.summary import SummaryStore
from cubedash.summary._summarise import GridCell
from datacube.config import LocalConfig
from datacube.index import Index, index_connect
from datacube.ui.click import environment_option, pass_config

_LOG = structlog.get_logger()


def _get_store(config: LocalConfig, variant: str, log=_LOG) -> SummaryStore:
    index: Index = index_connect(
        config, application_name=f"cubedash.show.{variant}", validate_connection=False
    )
    store = SummaryStore(index, log=log)
    return store


@click.command()
@environment_option
@pass_config
@click.option("-v", "--verbose", is_flag=True)
@click.option(
    "-l",
    "--event-log-file",
    help="Output jsonl logs to file",
    type=click.Path(writable=True, dir_okay=True),
)
@click.option("--allow-cache/--no-cache", is_flag=True, default=True)
@click.argument("product_name")
@click.argument("year", type=int, required=False)
@click.argument("month", type=int, required=False)
@click.argument("day", type=int, required=False)
def cli(
    config: LocalConfig,
    allow_cache: bool,
    product_name: str,
    year: int,
    month: int,
    day: int,
    event_log_file: str,
    verbose: bool,
):
    """
    Print the recorded summary information for the given product
    """
    init_logging(open(event_log_file, "a") if event_log_file else None, verbose=verbose)

    store = _get_store(config, "setup")
    store.init(init_products=False)

    t = time.time()
    if allow_cache:
        summary = store.get_or_update(product_name, year, month, day)
    else:
        summary = store.calculate_summary(
            product_name, _utils.as_time_range(year, month, day)
        )
    t_end = time.time()

    echo(f"{summary.dataset_count} ", nl=False)
    secho(product_name, nl=False, bold=True)
    echo(f" datasets for ", nl=False)
    secho(f"{year or 'all'} {month or 'all'} {day or 'all'}", fg="blue")
    if summary.size_bytes is not None:
        echo(sizeof_fmt(summary.size_bytes))
    echo(f"{round(t_end - t, 2)} seconds")
    echo()
    print_count_table(summary.grid_dataset_counts)


def print_count_table(cs: Counter[GridCell]):
    xs = [c.x for c, count in cs.items()]
    ys = [c.y for c, count in cs.items()]

    count_range = min(cs.values()), max(cs.values())
    x_range = min(xs), max(xs) + 1
    y_range = min(ys), max(ys) + 1

    # Find the "widest" number to print.
    # (it could be the smallest number if there's a minus sign)
    count_width = max(len(str(i)) for i in x_range + y_range + count_range)

    def echo_head(s):
        secho(f"%{count_width}d " % s, nl=False, bold=True)

    def echo_cell(number):
        if number:
            secho(f"%{count_width}d " % number, nl=False)
        else:
            # Print empty space for zeroes
            echo(" " * (count_width + 1), nl=False)

    # Header of X values

    # corner gap
    echo_cell(None)
    for x in range(*x_range):
        echo_head(x)
    echo()

    # Rows
    for y in range(*y_range):
        echo_head(y)
        for x in range(*x_range):
            cell = GridCell(x, y)
            count = cs.get(cell) or 0
            echo_cell(count)
        echo()


if __name__ == "__main__":
    cli()
