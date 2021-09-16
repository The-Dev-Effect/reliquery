import json
import os

import pytest
from unittest import mock

from reliquery.storage import (
    S3Storage,
    get_storage_by_name,
    FileStorage,
    get_all_available_storages,
)

raw_config = """
    {
        "s3":{
            "storage": {
                "type": "S3",
                "args": {
                    "s3_signed": true,
                    "s3_bucket": "Scratch-bucket",
                    "prefix": "scratch"
                }
            }
        },
        "file": {
            "storage": {
                "type": "File",
                "args": {
                }
            }
        }
    }"""


def test_use_s3_when_getting_storage_with_config_having_s3_type(tmpdir):

    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {
            "s3": {
                "storage": {
                    "type": "S3",
                    "args": {
                        "s3_signed": True,
                        "s3_bucket": "somewhere",
                        "prefix": "rel",
                    },
                }
            }
        }

        config_file.write(json.dumps(config, indent=4))
    storage = get_storage_by_name("s3", tmpdir)
    assert type(storage) == S3Storage
    assert storage.signed == True
    assert "somewhere" == storage.s3_bucket
    assert "rel" == storage.prefix


def test_use_s3_when_getting_storage_with_config_missing_s3_signed(tmpdir):

    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {
            "s3": {
                "storage": {
                    "type": "S3",
                    "args": {
                        "s3_bucket": "somewhere",
                        "prefix": "rel",
                    },
                }
            }
        }

        config_file.write(json.dumps(config, indent=4))
    storage = get_storage_by_name("s3", tmpdir)
    assert type(storage) == S3Storage
    assert storage.signed == True
    assert "somewhere" == storage.s3_bucket
    assert "rel" == storage.prefix


def test_use_demo_s3_storage_when_getting_storage_with_config_having_demo_name(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {
            "demo": {
                "storage": {
                    "type": "S3",
                    "args": {
                        "s3_signed": False,
                        "s3_bucket": "somewhere",
                        "prefix": "rel",
                    },
                }
            }
        }
        config_file.write(json.dumps(config, indent=4))
    storage = get_storage_by_name("demo", tmpdir)
    assert type(storage) == S3Storage
    assert storage.signed == False
    assert "somewhere" == storage.s3_bucket
    assert "rel" == storage.prefix


def test_use_file_storage_when_getting_default_storage_without_config(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    storage = get_storage_by_name("default", tmpdir)
    assert type(storage) == FileStorage


def test_use_file_storage_when_getting_storage_with_config_having_file_type(
    tmpdir,
):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {"file": {"storage": {"type": "File", "args": {}}}}
        config_file.write(json.dumps(config, indent=4))
    storage = get_storage_by_name("file", tmpdir)
    assert type(storage) == FileStorage


@mock.patch.dict(os.environ, {"RELIQUERY_CONFIG": raw_config})
def test_use_s3_storage_when_passing_s3_config_in_environment_as_variable(tmpdir):
    storage = get_storage_by_name("s3", tmpdir)
    assert type(storage) == S3Storage


def test_error_when_getting_default_storage_with_config_having_unknown_type(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    config_path = os.path.join(reliquery_dir, "config")
    with open(config_path, mode="w+") as config_file:
        config = {"none": {"storage": {"type": "None"}}}
        config_file.write(json.dumps(config, indent=4))
    with pytest.raises(ValueError):
        get_storage_by_name("none", tmpdir)


def test_availible_storages_returns_default_storage(tmpdir):
    storage = get_all_available_storages(tmpdir)

    assert len(storage) == 2
    for stor in storage:
        if type(stor) == FileStorage:
            assert stor.name == "default"
        elif type(stor) == S3Storage:
            assert stor.name == "demo"


@mock.patch.dict(os.environ, {"RELIQUERY_CONFIG": raw_config})
def test_different_storage_types_given_env_config(tmpdir):
    storages = get_all_available_storages(tmpdir)

    assert len(storages) == 2
    for stor in storages:
        if type(stor) == FileStorage:
            assert stor.name == "file"
        elif type(stor) == S3Storage:
            assert stor.name == "s3"
