import pytest
import os
from .. import Relic
from ..storage import FileStorage

import numpy as np

"""
NOTE:
Tests metadata created implicitly and as a product of creating other 
relics of differing types

Pass meta_in_memory=True into any relics to create a sqlite database 
in memory. This is needed in tests to clean up after test that dirty 
the database.
"""


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path))


def test_metadata_when_adding_arrays(test_storage):
    rq = Relic(
        name="test", relic_type="test", storage=test_storage, meta_in_memory=True
    )

    rq.add_array("test-array", np.zeros((100, 128, 128)))

    metadata = rq.describe()

    assert metadata["test"]["arrays"][0]["name"] == "test-array"
    assert metadata["test"]["arrays"][0]["data_type"] == "arrays"
    assert metadata["test"]["arrays"][0]["size_mb"] > 0
    assert metadata["test"]["arrays"][0]["shape"] == "(100, 128, 128)"


def test_metadata_when_adding_text(test_storage):
    rq = Relic(
        name="test", relic_type="test", storage=test_storage, meta_in_memory=True
    )
    test_text = "This is the test arena"
    rq.add_text("test-text", test_text)

    metadata = rq.describe()

    assert metadata["test"]["text"][0]["name"] == "test-text"
    assert metadata["test"]["text"][0]["data_type"] == "text"
    assert metadata["test"]["text"][0]["size_mb"] > 0
    assert metadata["test"]["text"][0]["shape"] == len(test_text)


def test_metadata_when_adding_html(test_storage):
    rq = Relic(
        name="test", relic_type="test", storage=test_storage, meta_in_memory=True
    )

    rq.add_html("test-html.html", os.path.join(os.path.dirname(__file__), "test.html"))

    metadata = rq.describe()

    assert metadata["test"]["html"][0]["name"] == "test-html.html"
    assert metadata["test"]["html"][0]["data_type"] == "html"
