import pytest
from .. import Relic
from reliquery.storage import FileStorage, StorageItemDoesNotExist


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test-text")


def test_list_text_file_when_add_text(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")
    text_list = rq.list_text()
    assert len(text_list) > 0


def test_text_file_given_file_name(test_storage):

    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")

    text = rq.get_text("test-text")
    assert text is not None
    assert text == ("Test String!!!\n\tWhat do you think?")


def test_list_text_files(test_storage):

    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")
    rq.add_text("test-text2", "Another String!!!\n\tWant More of them?")
    text_list = rq.list_text()
    assert len(text_list) == 2


def test_remove_text_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_text("test-text", "Test String!!!\n\tWhat do you think?")
    assert len(rq.list_text()) == 1

    rq.remove_text("test-text")

    assert len(rq.list_text()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_text("test-text")
