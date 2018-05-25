import pytest
from pathlib import Path

from datacube.scripts.dataset import create_dataset, load_rules_from_types
from datacube.utils import read_documents

pytest_plugins = "digitalearthau.testing"

TEST_DATA_DIR = Path(__file__).parent / 'data'

_NBAR_SCENE_DUMP_PATH = TEST_DATA_DIR / 'ls8-nbar-scene-sample-2017.yaml.gz'


# TODO: scope="session", as index is expensive to populate
# Needs the upstream dea_index to allow it.
@pytest.fixture
def populated_index(dea_index):
    ls8_nbar_scene = dea_index.products.get_by_name('ls8_nbar_scene')

    dataset_count = 0

    rules = load_rules_from_types(dea_index)
    for _, doc in read_documents(_NBAR_SCENE_DUMP_PATH):
        created = dea_index.datasets.add(
            create_dataset(doc, None, rules),
        )

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    return dea_index
