import pytest
import os
import reliquery.relic as relic
from reliquery.storage import FileStorage


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path))


def test_list_html_file_when_add_html(test_storage):

    rq = relic.Relic(
        name="test", relic_type="test", storage=test_storage, meta_in_memory=True
    )

    rq.add_html("test-html.html", os.path.join(os.path.dirname(__file__), "test.html"))
    html_list = rq.list_html()
    assert len(html_list) > 0


def test_html_file_given_file_name(test_storage):

    rq = relic.Relic(
        name="test", relic_type="test", storage=test_storage, meta_in_memory=True
    )

    rq.add_html("test-html.html", os.path.join(os.path.dirname(__file__), "test.html"))

    html = rq.get_html("test-html.html")
    assert html != None
    assert html.startswith("<div>")


def test_list_html_files(test_storage):

    rq = relic.Relic(
        name="element-bucket",
        relic_type="test",
        storage=test_storage,
        meta_in_memory=True,
    )

    rq.add_html("test-html.html", os.path.join(os.path.dirname(__file__), "test.html"))
    rq.add_html("test-html2.html", os.path.join(os.path.dirname(__file__), "test.html"))
    html_list = rq.list_html()
    assert len(html_list) == 2
