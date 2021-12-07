import pytest
import os
import reliquery.relic as relic
from reliquery.storage import FileStorage, StorageItemDoesNotExist


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test-html")


def test_list_html_file_when_add_html(test_storage):

    rq = relic.Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_html_from_path(
        "test-html.html", os.path.join(os.path.dirname(__file__), "test.html")
    )
    html_list = rq.list_html()
    assert len(html_list) > 0


def test_html_file_given_file_name(test_storage):

    rq = relic.Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_html_from_path(
        "test-html.html", os.path.join(os.path.dirname(__file__), "test.html")
    )

    html = rq.get_html("test-html.html")
    assert html is not None
    assert html.startswith("<div>")


def test_list_html_files(test_storage):

    rq = relic.Relic(
        name="element-bucket",
        relic_type="test",
        storage=test_storage,
    )

    rq.add_html_from_path(
        "test-html.html", os.path.join(os.path.dirname(__file__), "test.html")
    )
    rq.add_html_from_path(
        "test-html2.html", os.path.join(os.path.dirname(__file__), "test.html")
    )
    html_list = rq.list_html()
    assert len(html_list) == 2


def test_remove_html_given_name(test_storage):
    rq = relic.Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_html_from_path(
        "test-html.html", os.path.join(os.path.dirname(__file__), "test.html")
    )
    assert len(rq.list_html()) == 1

    rq.remove_html("test-html.html")
    assert len(rq.list_html()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_html("test-html.html")
