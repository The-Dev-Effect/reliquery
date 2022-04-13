import pytest
from .. import Relic
from reliquery.storage import FileStorage, StorageItemDoesNotExist
import os


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_notebook")


def test_list_noteboks_when_add_notebook(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    assert len(rq.list_notebooks()) == 0
    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebook_from_path("TestNotebook", test_notebook)

    notebook_list = rq.list_notebooks()
    assert len(notebook_list) == 1


def test_save_notebook_to_path(test_storage, tmp_path):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebook_from_path("TestNotebook", test_notebook)
    path_to_save = os.path.join(tmp_path, "testnotebook.ipynb")

    assert os.path.exists(path_to_save) is False
    rq.save_notebook_to_path("TestNotebook", path_to_save)

    assert os.path.exists(path_to_save)


def test_get_notebook_given_notebook_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebook_from_path("TestNotebook", test_notebook)

    stream = rq.get_notebook("TestNotebook")
    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "TestNotebook"


def test_get_notebook_html(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebook_from_path("TestNotebook.ipynb", test_notebook)

    assert (
        rq.get_notebook_html("TestNotebook.ipynb").lower().startswith("<!doctype html>")
    )


def test_remove_notebook_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebook_from_path("TestNotebook", test_notebook)
    assert len(rq.list_notebooks()) == 1

    rq.remove_notebook("TestNotebook")

    assert len(rq.list_notebooks()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_notebook("TestNotebook")

    with pytest.raises(StorageItemDoesNotExist):
        rq.get_notebook_html("TestNotebook")
