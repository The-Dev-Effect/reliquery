from genericpath import samefile
import json
from reliquery.metadata import Metadata, MetadataDB
from typing import List, Dict
from sys import getsizeof
import os
from io import BytesIO

import numpy as np

from .storage import (
    get_available_storages,
    StorageItemDoesNotExist,
    get_default_storage,
    Storage,
)

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
        storage_type: str = "default",
        check_exists: bool = True,
    ):
        self.name = name
        self.relic_type = relic_type
        self.storage_type = storage_type

        if storage is None:
            self.storage = get_default_storage(self.storage_type)
        else:
            self.storage = storage

        if check_exists:
            self._ensure_exists()

        self.metadata_db = MetadataDB()

    @classmethod
    def assert_valid_id(cls, id: str):
        if id == "":
            raise InvalidRelicId()

    def _ensure_exists(self):
        try:
            self.storage.get_text([self.relic_type, self.name, "exists"])
        except StorageItemDoesNotExist as e:
            print("Creating a Relic")
            self.storage.put_text([self.relic_type, self.name, "exists"], "exists")

    # TODO: needs test coverage
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

        size = getsizeof(array)
        shape = str(np.array(array).shape)
        metadata = Metadata(
            name=name,
            data_type="arrays",
            relic_type=self.relic_type,
            storage_name=self.storage.name,
            size=size,
            shape=shape,
        )

        buffer = BytesIO()
        np.save(buffer, array, allow_pickle=False)
        buffer.seek(0)

        self.storage.put_binary_obj(
            [self.relic_type, self.name, "arrays", name], buffer
        )
        self._add_metadata(metadata)

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

        metadata = metadata = Metadata(
            name=name,
            data_type="html",
            relic_type=self.relic_type,
            storage_name=self.storage.name,
        )

        self.storage.put_file([self.relic_type, self.name, "html", name], html_path)
        self._add_metadata(metadata)

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

        size = getsizeof(text)
        metadata = Metadata(
            name=name,
            data_type="text",
            relic_type=self.relic_type,
            storage_name=self.storage.name,
            size=size,
            shape=len(text),
        )

        self.storage.put_text([self.relic_type, self.name, "text", name], text)
        self._add_metadata(metadata)

    def list_text(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "text"])

    def get_text(self, name: str) -> str:
        self.assert_valid_id(name)

        return self.storage.get_text([self.relic_type, self.name, "text", name])

    def _add_metadata(self, metadata: Metadata) -> None:
        self.assert_valid_id(metadata.name)

        self.storage.put_metadata(
            [self.relic_type, self.name, "metadata", metadata.data_type, metadata.name],
            metadata.get_metadata(),
        )

    def describe(self) -> Dict:
        return self.storage.get_metadata(
            [self.relic_type, self.name, "metadata"], self.name
        )

    """
    Query metadata over all relics

    pass SQL expression as a string
    """

    def query(self, statement: str) -> List:
        metadata = self.storage.get_all_relic_metadata()

        for data in metadata:
            self.metadata_db.sync(Metadata.parse_dict(data))

        return self.metadata_db.query(statement)


class Reliquery:
    """
    Class used to query over available and accessible storage locations and Relics

    Attributes
    ----------
    storages : List[Storage]
        list of availably accessible storages

    metadata_db : MetadataDB
        in memory sqlite db used for querying Relics

    """

    def __init__(self, storages: List[Storage] = []) -> None:
        if len(storages) > 0:
            self.storages = storages
        else:
            self.storages = get_available_storages()
        self.metadata_db = MetadataDB()

    def query(self, statement: str) -> List:
        if self.storages:
            self._sync_relics()

        return self.metadata_db.query(statement)

    def _sync_relics(self) -> None:
        for stor in self.storages:
            metadata = stor.get_all_relic_metadata()

            for data in metadata:
                self.metadata_db.sync(Metadata.parse_dict(data))
