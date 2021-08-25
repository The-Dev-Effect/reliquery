import json
import os

import pytest
from unittest import mock

from reliquery.storage import S3Storage, get_default_storage, FileStorage

raw_config = """
    {
        "storage": {
          "type": "S3",
          "args": {
            "s3_bucket": "Scratch-bucket",
            "prefix": "scratch"
          }
        }
    }"""


def test_use_s3_when_getting_storage_with_config_having_s3_type(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {
            "storage": {
                "type": "S3",
                "args": {"s3_bucket": "somewhere", "prefix": "rel"},
            }
        }
        config_file.write(json.dumps(config, indent=4))
    storage = get_default_storage(tmpdir)
    assert type(storage) == S3Storage
    assert storage.signed == True
    assert "somewhere" == storage.s3_bucket
    assert "rel" == storage.prefix


def test_use_demo_s3_storage_when_getting_storage_with_config_having_demo_type(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {
            "storage": {
                "type": "Demo",
                "args": {"s3_bucket": "somewhere", "prefix": "rel"},
            }
        }
        config_file.write(json.dumps(config, indent=4))
    storage = get_default_storage(tmpdir)
    assert type(storage) == S3Storage
    assert storage.signed == False
    assert "somewhere" == storage.s3_bucket
    assert "rel" == storage.prefix


def test_use_demo_s3_storage_when_getting_default_storage_without_config(tmpdir):
    storage = get_default_storage(tmpdir)
    assert type(storage) == S3Storage
    assert storage.signed == False


def test_use_file_storage_when_getting_storage_with_config_having_file_type(tmpdir,):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {"storage": {"type": "File"}}
        config_file.write(json.dumps(config, indent=4))
    storage = get_default_storage(tmpdir)
    assert type(storage) == FileStorage


@mock.patch.dict(os.environ, {"RELIQUERY_CONFIG": raw_config})
def test_use_s3_storage_when_passing_s3_config_in_environment_as_variable(tmpdir):
    storage = get_default_storage(tmpdir)
    assert type(storage) == S3Storage


def test_error_when_getting_storage_with_config_having_unknown_type(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {"storage": {"type": "None"}}
        config_file.write(json.dumps(config, indent=4))
    with pytest.raises(ValueError):
        get_default_storage(tmpdir)
