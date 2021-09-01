import pytest
import os
import reliquery.relic as relic
from reliquery.storage import FileStorage


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path))


def test_list_text_file_when_add_text(test_storage):

    rq = relic.Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")
    text_list = rq.list_text()
    assert len(text_list) > 0


def test_text_file_given_file_name(test_storage):

    rq = relic.Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")

    text = rq.get_text("test-text")
    assert text != None
    assert text == ("Test String!!!\n\tWhat do you think?")


def test_list_text_files(test_storage):

    rq = relic.Relic(name="element-bucket", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")
    rq.add_text("test-text2", "Another String!!!\n\tWant More of them?")
    text_list = rq.list_text()
    assert len(text_list) == 2
