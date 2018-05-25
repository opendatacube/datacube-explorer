"""
Util script to dump datasets from a datacube for use as test data.
"""
import click
from datetime import datetime

from pathlib import Path

from datacube import Datacube
import gzip
import yaml

from datacube.model import Range


def dump_datasets(dc: Datacube, path: Path, **query):
    total_count = dc.index.datasets.count(**query)

    if path.exists():
        raise ValueError(f"Path exists: {path}")

    product_name = query.get('product') or 'datasets'
    msg = f'Dumping {total_count} {product_name} (with their sources)'
    with click.progressbar(dc.index.datasets.search(**query),
                           length=total_count,
                           label=msg) as progress:
        with gzip.open(path, 'w') as f:
            yaml.dump_all(
                (dc.index.datasets.get(d.id, include_sources=True).metadata_doc
                 for d in progress),
                stream=f,
                encoding='utf-8',
                indent=4,
                Dumper=yaml.CDumper
            )


if __name__ == '__main__':
    with Datacube() as dc:
        dump_datasets(
            dc,
            Path('ls8-nbar-albers-recent.yaml.gz'),
            product='ls8_nbar_albers',
            time=Range(datetime(2017, 4, 15), datetime(2017, 5, 15))
        )
        dump_datasets(
            dc,
            Path('ls8-nbar-scene-2017.yaml.gz'),
            product='ls8_nbar_scene',
            time=Range(datetime(2017, 1, 1), datetime(2018, 1, 1))

        )
        dump_datasets(
            dc,
            Path('low-tide-comp-20p.yaml.gz'),
            product='low_tide_comp_20p'
        )
