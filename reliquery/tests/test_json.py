import pytest
from .. import Relic
from reliquery.storage import FileStorage


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_json")


def test_list_json_file_when_add_json(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_json("test-json", {"One": 1, "Two": 2, "Three": 3})
    json_text_list = rq.list_jsons()
    assert len(json_text_list) > 0


def test_json_file_given_file_name(test_storage):

    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_json("test-json", {"One": 1, "Two": 2, "Three": 3})

    json_text = rq.get_json("test-json")
    assert json_text is not None
    assert json_text == ({"One": 1, "Two": 2, "Three": 3})
