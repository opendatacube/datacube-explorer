"""
Util script to dump datasets from a datacube for use as test data.
"""
import click
from datetime import datetime

from pathlib import Path

from datacube import Datacube
import gzip
import yaml

from datacube.model import Range, Dataset
import random


def _sample(iterable, sample_count):
    """
    Choose a random sampling of items from an iterator.

    (you will get Nones if sample_count is less than iterable length)
    """
    result = [None] * sample_count
    for i, item in enumerate(iterable):
        if i < sample_count:
            result[i] = item
        else:
            j = int(random.random() * (i + 1))
            if j < sample_count:
                result[j] = item
    return result


def dump_datasets(dc: Datacube, path: Path, dataset_sample_fraction=0.3, include_sources=True, **query):
    total_count = dc.index.datasets.count(**query)

    if path.exists():
        raise ValueError(f"Path exists: {path}")

    product_name = query.get('product') or 'datasets'
    sample_count = int(total_count * dataset_sample_fraction)
    msg = f'Dumping {sample_count} of {total_count} {product_name} (with their sources)'

    with click.progressbar(
            _sample(dc.index.datasets.search(**query), sample_count),
            length=sample_count,
            label=msg) as progress:
        with gzip.open(path, 'w') as f:
            yaml.dump_all(
                (_get_dumpable_doc(dc, d, include_sources) for d in progress),
                stream=f,
                encoding='utf-8',
                indent=4,
                Dumper=yaml.CDumper
            )


def _get_dumpable_doc(dc: Datacube, d: Dataset, include_sources=True):
    if include_sources:
        return dc.index.datasets.get(d.id, include_sources=include_sources).metadata_doc
    else:
        # Empty doc means "there are no sources", so we can load it easily.
        d.metadata.sources = {}
        return d.metadata_doc


TEST_DATA_DIR = Path(__file__).parent / 'data'

if __name__ == '__main__':
    with Datacube(env='clone') as dc:

        dump_datasets(
            dc,
            TEST_DATA_DIR / 'high_tide_comp_20p.yaml.gz',
            dataset_sample_fraction=1,
            include_sources=False,
            product='high_tide_comp_20p',
            time=Range(datetime(1980, 4, 15), datetime(2018, 5, 15)),
        )
        # dump_datasets(
        #     dc,
        #     TEST_DATA_DIR / 'wofs-albers-sample2.yaml.gz',
        #     dataset_sample_fraction=0.01,
        #     product='wofs_albers',
        #     time=Range(datetime(2017, 4, 15), datetime(2017, 5, 15)),
        # )
        # dump_datasets(
        #     dc,
        #     TEST_DATA_DIR / 'ls8-nbar-scene-sample-2017.yaml.gz',
        #     dataset_sample_fraction=0.1,
        #     product='ls8_nbar_scene',
        #     time=Range(datetime(2016, 1, 1), datetime(2018, 1, 1)),
        # )
        # # Huuge amount of lineage.
        # dump_datasets(
        #     dc,
        #     TEST_DATA_DIR / 'low-tide-comp-20p.yaml.gz',
        #     dataset_sample_fraction=0.1,
        #     product='low_tide_comp_20p',
        # )
