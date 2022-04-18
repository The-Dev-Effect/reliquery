import numpy as np
import pytest
from .. import Relic
from ..storage import FileStorage, StorageItemDoesNotExist


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_arrays")


def test_add_array_by_name(test_storage):
    arr = np.ones((10, 10))

    relic = Relic(name="array", relic_type="test", storage=test_storage)

    assert len(relic.list_arrays()) == 0

    relic.add_array("arr", arr)

    assert len(relic.list_arrays()) == 1


def test_get_array_by_name(test_storage):
    arr = np.ones((10, 10))

    relic = Relic(name="array", relic_type="test", storage=test_storage)

    relic.add_array("arr", arr)

    assert np.array_equal(relic.get_array("arr"), arr)


def test_list_arrays(test_storage):
    relic = Relic(name="array", relic_type="test", storage=test_storage)

    assert len(relic.list_arrays()) == 0

    relic.add_array("ones", np.ones((10, 10)))
    relic.add_array("zeros", np.zeros((10, 10)))

    assert len(relic.list_arrays()) == 2


def test_remove_array_by_name(test_storage):
    arr = np.ones((10, 10))

    relic = Relic(name="array", relic_type="test", storage=test_storage)

    relic.add_array("arr", arr)
    assert len(relic.list_arrays()) == 1

    relic.remove_array("arr")
    print(len(relic.list_arrays()))
    assert len(relic.list_arrays()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        relic.get_array("arr")
