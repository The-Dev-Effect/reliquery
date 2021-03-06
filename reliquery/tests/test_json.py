import pytest
from .. import Relic
from reliquery.storage import FileStorage, StorageItemDoesNotExist


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_json")


def test_list_json_file_when_add_json(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_json("test-json", {"One": 1, "Two": 2, "Three": 3})
    json_text_list = rq.list_json()
    assert len(json_text_list) > 0


def test_json_file_given_file_name(test_storage):

    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_json("test-json", {"One": 1, "Two": 2, "Three": 3})

    json_text = rq.get_json("test-json")
    assert json_text is not None
    assert json_text == ({"One": 1, "Two": 2, "Three": 3})


def test_remove_json_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_json("test-json", {"One": 1, "Two": 2, "Three": 3})
    assert len(rq.list_json()) == 1

    rq.remove_json("test-json")
    assert len(rq.list_json()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_json("test-json")
