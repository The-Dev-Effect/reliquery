import pytest

from .. import Relic
from ..storage import FileStorage

import numpy as np


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path))


def test_relic(test_storage):
    e = Relic("test", "test", storage=test_storage, meta_in_memory=True)
    assert Relic.relic_exists("test", "test", storage=test_storage)


def test_array(test_storage):
    e = Relic("test", "test", storage=test_storage, meta_in_memory=True)

    orig = np.ones((10, 10))

    e.add_array("test", orig)

    np.testing.assert_array_equal(e.get_array("test"), orig)

    assert Relic.relic_exists("test", "test", storage=test_storage)
