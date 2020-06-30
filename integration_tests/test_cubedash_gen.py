from pathlib import Path

import pytest
from click.testing import CliRunner
from dateutil import tz

from cubedash.generate import cli
from datacube.index.hl import Doc2Dataset
from datacube.utils import read_documents

TEST_DATA_DIR = Path(__file__).parent / "data"
DEFAULT_TZ = tz.gettz("Australia/Darwin")


def _populate_from_dump(session_dea_index, expected_type: str, dump_path: Path):
    ls8_nbar_scene = session_dea_index.products.get_by_name(expected_type)
    dataset_count = 0

    create_dataset = Doc2Dataset(session_dea_index)

    for _, doc in read_documents(dump_path):
        label = doc["ga_label"] if ("ga_label" in doc) else doc["id"]
        dataset, err = create_dataset(doc, f"file://example.com/test_dataset/{label}")
        assert dataset is not None, err
        created = session_dea_index.datasets.add(dataset)

        assert created.type.name == ls8_nbar_scene.name
        dataset_count += 1

    print(f"Populated {dataset_count} of {expected_type}")
    return dataset_count


@pytest.fixture(scope="module", autouse=True)
def populate_index(module_dea_index):
    """
    Index populated with example datasets. Assumes our tests wont modify the data!

    It's module-scoped as it's expensive to populate.
    """
    _populate_from_dump(
        module_dea_index,
        "ls8_nbar_scene",
        TEST_DATA_DIR / "ls8-nbar-scene-sample-2017.yaml.gz",
    )
    _populate_from_dump(
        module_dea_index,
        "ls8_nbar_albers",
        TEST_DATA_DIR / "ls8-nbar-albers-sample.yaml.gz",
    )
    return module_dea_index


def test_cubedash_gen_refresh(module_db):
    """
    Test cubedash get with refresh
    """
    runner = CliRunner()
    res = runner.invoke(cli, ["--init"])
    assert res
    last_val = module_db.execute("select last_value from cubedash.product_id_seq;")
    res = runner.invoke(
        cli, ["--no-init-database", "--refresh-stats", "--force-refresh", "--all"]
    )
    assert res
    new_last_val = module_db.execute("select last_value from cubedash.product_id_seq;")
    assert new_last_val == last_val
