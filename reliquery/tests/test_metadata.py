import pytest
import os
from .. import Relic
from ..metadata import Metadata
from ..storage import FileStorage
from unittest.mock import patch
import datetime as dt

import numpy as np

raw_config = """
    {
        "s3": {
            "storage": {
            "type": "S3",
                "args": {
                    "s3_bucket": "Scratch-bucket",
                    "prefix": "scratch"
                }
            }
        }
    }"""


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test-metadata")


def test_metadata_when_adding_arrays(test_storage):
    rq = Relic(
        name="test", relic_type="test", storage_name="tests", storage=test_storage
    )

    rq.add_array("test-array", np.zeros((100, 128, 128)))

    metadata = rq.describe()

    assert metadata["test"]["arrays"][0]["name"] == "test-array"
    assert metadata["test"]["arrays"][0]["data_type"] == "arrays"
    assert metadata["test"]["arrays"][0]["size"] > 0
    assert metadata["test"]["arrays"][0]["shape"] == "(100, 128, 128)"


def test_metadata_when_adding_text(test_storage):
    rq = Relic(
        name="test", relic_type="test", storage_name="tests", storage=test_storage
    )
    test_text = "This is the test arena"
    rq.add_text("test-text", test_text)

    metadata = rq.describe()

    assert metadata["test"]["text"][0]["name"] == "test-text"
    assert metadata["test"]["text"][0]["data_type"] == "text"
    assert metadata["test"]["text"][0]["size"] > 0
    assert metadata["test"]["text"][0]["shape"] == len(test_text)


def test_metadata_when_adding_html(test_storage):
    rq = Relic(
        name="test", relic_type="test", storage_name="tests", storage=test_storage
    )

    rq.add_html("test-html.html", os.path.join(os.path.dirname(__file__), "test.html"))

    metadata = rq.describe()

    assert metadata["test"]["html"][0]["name"] == "test-html.html"
    assert metadata["test"]["html"][0]["data_type"] == "html"


@patch.dict(os.environ, {"RELIQUERY_CONFIG": raw_config})
@patch("reliquery.storage.S3Storage")
def test_relic_s3_storage_syncs_on_init(storage):
    storage().put_text.return_value = "exists"
    storage().get_metadata.return_value = {
        "test": {
            "arrays": [
                {
                    "id": 1,
                    "name": "test-array",
                    "data_type": "arrays",
                    "relic_type": "test",
                    "size": 80.0,
                    "shape": "(100,100,100)",
                    "last_modified": dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S"),
                }
            ],
            "text": [],
            "html": [],
        }
    }
    storage().list_keys.return_value = ["test-array"]

    rq = Relic(name="test", relic_type="test", storage_name="s3")

    assert len(rq.describe()["test"]["arrays"]) == 1


@patch.dict(os.environ, {"RELIQUERY_CONFIG": raw_config})
@patch("reliquery.storage.S3Storage.put_text")
@patch("reliquery.storage.S3Storage.list_keys")
@patch("reliquery.storage.S3Storage.get_metadata")
def test_db_connection(put_text, list_keys, get_metadata):
    put_text.return_value = "exists"
    list_keys.return_value = []
    get_metadata.return_value = {}

    rq = Relic(
        name="test",
        relic_type="test",
        storage_name="s3",
        check_exists=False,
    )

    db = rq.metadata_db

    assert len(list(iter(db.get_all_metadata()))) == 0

    db.add_metadata(
        Metadata(
            "name",
            "data type",
            "relic type",
            "storage1",
            last_modified=dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S"),
        )
    )

    meta = [i for i in db.get_all_metadata()]

    assert len(meta) == 1
    meta = [i for i in db.get_all_metadata()]
    assert meta[0].name == "name"
    assert meta[0].data_type == "data type"
    assert meta[0].relic_type == "relic type"
    assert meta[0].storage_name == "storage1"
    assert meta[0].last_modified is not None
