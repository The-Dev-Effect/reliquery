import json
from reliquery.metadata import MetadataDB
from typing import List, Dict
from sys import getsizeof
import os
from io import BytesIO

import numpy as np

from .storage import StorageItemDoesNotExist, get_default_storage, Storage

import pprint


StoragePath = List[str]


class InvalidRelicId(Exception):
    pass


class Relic:
    name: str
    relic_type: str
    storage: Storage

    def __init__(
        self,
        name: str,
        relic_type: str,
        storage: Storage = None,
        check_exists: bool = True,
        meta_in_memory: bool = False,
    ):
        self.name = name
        self.relic_type = relic_type
        self.reliquery_dir = os.path.join(os.path.expanduser("~"), "reliquery")
        if meta_in_memory:
            self.metadata_db = MetadataDB(":memory:")
        else:
            self.metadata_db = MetadataDB(
                os.path.join(self.reliquery_dir, "metadata.db")
            )

        if storage is None:
            self.storage = get_default_storage()
        else:
            self.storage = storage

        if check_exists:
            self._ensure_exists()

    @classmethod
    def assert_valid_id(cls, id: str):
        if id == "":
            raise InvalidRelicId()

    def _ensure_exists(self):
        self.storage.put_text([self.relic_type, self.name, "exists"], "exists")

    @classmethod
    def relic_exists(cls, name: str, relic_type: str, storage: Storage = None) -> bool:
        if storage is None:
            storage = get_default_storage()

        try:
            storage.get_text([relic_type, name, "exists"])
        except StorageItemDoesNotExist:
            return False

        return True

    def add_array(self, name: str, array: np.ndarray):
        self.assert_valid_id(name)

        size = getsizeof(array) * 1e-6
        shape = str(np.array(array).shape)
        metadata = {
            "name": name,
            "data_type": "arrays",
            "size_mb": size,
            "shape": shape,
        }

        buffer = BytesIO()
        np.save(buffer, array, allow_pickle=False)
        buffer.seek(0)

        self.storage.put_binary_obj(
            [self.relic_type, self.name, "arrays", name], buffer
        )
        self._add_metadata(metadata, "arrays", name)

    def get_array(self, name: str) -> np.ndarray:
        self.assert_valid_id(name)

        with self.storage.get_binary_obj(
            [self.relic_type, self.name, "arrays", name]
        ) as f:
            return np.load(f, allow_pickle=False)

    def list_arrays(self) -> List[np.ndarray]:
        return self.storage.list_keys([self.relic_type, self.name, "arrays"])

    def add_html(self, name: str, html_path: str) -> None:
        self.assert_valid_id(name)

        metadata = {
            "name": name,
            "data_type": "html",
        }

        self.storage.put_file([self.relic_type, self.name, "html", name], html_path)
        self._add_metadata(metadata=metadata, data_type="html", name=name)

    def list_html(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "html"])

    def get_html(self, name: str) -> str:
        self.assert_valid_id(name)

        with self.storage.get_binary_obj(
            [self.relic_type, self.name, "html", name]
        ) as f:
            return f.read().decode("utf-8")

    def add_text(self, name: str, text: str) -> None:
        self.assert_valid_id(name)

        size = getsizeof(text) * 1e-6
        metadata = {
            "name": name,
            "data_type": "text",
            "size_mb": size,
            "shape": len(text),
        }

        self.storage.put_text([self.relic_type, self.name, "text", name], text)
        self._add_metadata(metadata, "text", name)

    def list_text(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "text"])

    def get_text(self, name: str) -> str:
        self.assert_valid_id(name)

        return self.storage.get_text([self.relic_type, self.name, "text", name])

    def _add_metadata(
        self, metadata: Dict, data_type: str, name: str, save_local: bool = True
    ) -> None:
        self.assert_valid_id(name)

        self.storage.put_metadata(
            [self.relic_type, self.name, "metadata", data_type, name], metadata
        )

        if save_local:
            self.metadata_db.add_metadata(metadata, self.relic_type)

    def describe(self) -> Dict:
        return self.storage.get_metadata(
            [self.relic_type, self.name, "metadata"], self.name
        )
