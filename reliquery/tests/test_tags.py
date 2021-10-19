import pytest
from .. import Relic
from reliquery.storage import FileStorage


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), name="test-tag-storage")


def test_list_tags_given_key_value_pair(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_tag({"color": "red"})

    tag_list = rq.list_tags()
    assert len(tag_list) > 0
    assert tag_list["color"] == "red"


def test_list_text_files(test_storage):

    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_tag({"color": "red", "day": "monday"})
    assert len(rq.list_tags()) == 2
    rq.add_tag({"reseacher": "newton"})

    assert len(rq.list_tags()) == 3
