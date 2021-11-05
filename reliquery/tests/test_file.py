import pytest
from .. import Relic
from reliquery.storage import FileStorage
import os


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_file")

def test_list_file_when_add_file(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    assert len(rq.list_files()) == 0
    test_file = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_file_from_path("TestFile", test_file)

    file_list = rq.list_files()
    assert len(file_list) == 1

def test_save_file_to_path(test_storage, tmp_path):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_file = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_file_from_path("Image", test_file)
    path_to_save = os.path.join(tmp_path, "testsaved.png")

    assert(os.path.exists(path_to_save) == False)
    rq.save_file_to_path("Image", path_to_save)

    assert(os.path.exists(path_to_save))

def test_get_file_given_file_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_file = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_file_from_path("TestFile", test_file)

    stream = rq.get_file("TestFile")
    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "TestFile"


