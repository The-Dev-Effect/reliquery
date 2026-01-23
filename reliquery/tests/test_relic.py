import json
import pytest

from .. import Relic
from ..storage import FileStorage

import numpy as np


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test-relic")


@pytest.fixture
def test_config():
    return {
        "default": {"storage": {"type": "File", "args": {"root": "~/reliquery"}}},
    }


def test_relic(test_storage):
    Relic("test", "test", storage=test_storage)
    assert Relic.relic_exists("test", "test", storage=test_storage)


def test_relic_given_reliquery_config_root(tmp_path, test_config) -> None:
    custom_root = tmp_path / "custom_reliquery_root"
    custom_root.mkdir(parents=True, exist_ok=True)
    config_path = custom_root / "config"
    with open(config_path, "w") as f:
        json.dump(test_config, f, indent=2)

    relic_name = "test-config"
    relic_type = "test-config"

    assert (
        Relic(
            name=relic_name,
            relic_type=relic_type,
            reliquery_config_root=str(custom_root),
        )
        is not None
    )


def test_array(test_storage):
    e = Relic("test", "test", storage=test_storage)

    orig = np.ones((10, 10))

    e.add_array("test", orig)

    np.testing.assert_array_equal(e.get_array("test"), orig)

    assert Relic.relic_exists("test", "test", storage=test_storage)
