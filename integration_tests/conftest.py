from pathlib import Path

import pytest

from cubedash import logs
from datacube.scripts.dataset import create_dataset, load_rules_from_types
from datacube.utils import read_documents

pytest_plugins = "digitalearthau.testing"

TEST_DATA_DIR = Path(__file__).parent / "data"


# TODO: scope="session", as index is expensive to populate
# Needs the upstream dea_index to allow it.
@pytest.fixture
def populated_scene_index(dea_index):
    _populate_from_dump(
        dea_index,
        "ls8_nbar_scene",
        TEST_DATA_DIR / "ls8-nbar-scene-sample-2017.yaml.gz",
    )
    return dea_index


@pytest.fixture
def populated_albers_index(dea_index):
    _populate_from_dump(
        dea_index, "ls8_nbar_albers", TEST_DATA_DIR / "ls8-nbar-albers-sample.yaml.gz"
    )
    return dea_index


def _populate_from_dump(dea_index, expected_type: str, dump_path: Path):
    ls8_nbar_scene = dea_index.products.get_by_name(expected_type)
    dataset_count = 0
    rules = load_rules_from_types(dea_index)
    for _, doc in read_documents(dump_path):
        created = dea_index.datasets.add(create_dataset(doc, None, rules))

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    print(f"Populated {dataset_count} of {expected_type}")
    return dataset_count


@pytest.fixture(autouse=True, scope="session")
def init_logs(pytestconfig):
    logs.init_logging(verbose=pytestconfig.getoption("verbose") > 0)
