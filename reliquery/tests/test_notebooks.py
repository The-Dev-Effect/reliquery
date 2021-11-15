import pytest
from .. import Relic
from reliquery.storage import FileStorage
import os


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_notebook")


def test_list_noteboks_when_add_notebooks(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    assert len(rq.list_notebooks()) == 0
    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebooks_from_path("TestNotebook", test_notebook)

    notebook_list = rq.list_notebooks()
    assert len(notebook_list) == 1
    assert rq.get_notebooks_html(test_notebook) is not None
    print(rq.get_notebooks_html(test_notebook))
    assert False

def test_save_notebooks_to_path(test_storage, tmp_path):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
    rq.add_notebooks_from_path("TestNotebook", test_notebook)
    path_to_save = os.path.join(tmp_path, "testnotebook.ipynb")

    assert os.path.exists(path_to_save) is False
    rq.save_notebooks_to_path("TestNotebook", path_to_save)

    assert os.path.exists(path_to_save)


def test_get_notebooks_given_notebook_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_notebook = os.path.join(os.path.dirname(__file__),"notebook_test.ipynb")
    rq.add_notebooks_from_path("TestNotebook", test_notebook)

    stream = rq.get_notebooks("TestNotebook")
    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "TestNotebook"
