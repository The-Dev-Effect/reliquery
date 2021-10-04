from genericpath import samefile
import json
import logging
from reliquery.metadata import Metadata, MetadataDB, RelicTag
from typing import List, Dict
from sys import getsizeof
import os
from io import BytesIO

import numpy as np
import datetime as dt


from .storage import (
    get_all_available_storages,
    StorageItemDoesNotExist,
    get_storage_by_name,
    Storage,
)


StoragePath = List[str]

dt_format = "%m/%d/%Y %H:%M:%S"


class InvalidRelicId(Exception):
    pass


class EmptyTagDict(Exception):
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
        storage_name: str = "default",
        check_exists: bool = True,
    ):
        self.name = name
        self.relic_type = relic_type
        self.storage_name = storage_name

        if storage is None:
            self.storage = get_storage_by_name(self.storage_name)

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
            logging.info("Creating a Relic")
            self.storage.put_text([self.relic_type, self.name, "exists"], "exists")

    # TODO: needs test coverage
    @classmethod
    def relic_exists(
        cls,
        name: str,
        relic_type: str,
        storage: Storage = None,
        storage_name: str = "default",
    ) -> bool:
        if storage is None:
            storage = get_storage_by_name(storage_name)
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

    def add_html_from_path(self, name: str, html_path: str) -> None:

        self.assert_valid_id(name)

        metadata = metadata = Metadata(
            name=name,
            data_type="html",
            relic_type=self.relic_type,
            storage_name=self.storage.name,
        )

        self.storage.put_file([self.relic_type, self.name, "html", name], html_path)
        self._add_metadata(metadata)

    # TODO needs test coverage
    def add_html_string(self, name: str, html_str: str):
        self.assert_valid_id(name)

        metadata = Metadata(
            name=name,
            data_type="html",
            relic_type=self.relic_type,
            storage_name=self.storage.name,
        )

        self.storage.put_text([self.relic_type, self.name, "html", name], html_str)

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
            metadata.get_dict(),
        )

    def describe(self) -> Dict:
        return self.storage.get_metadata(
            [self.relic_type, self.name, "metadata"], self.name
        )

    def query(self, statement: str) -> List:
        """
        Query metadata over all relics

        pass SQL expression as a string
        """
        metadata = self.storage.get_all_relic_metadata()

        for data in metadata:
            self.metadata_db.sync_metadata(Metadata.parse_dict(data))

        return self.metadata_db.query(statement)

    def add_tag(self, tags: Dict) -> List[Dict]:

        tag = RelicTag(self.name, self.relic_type, self.storage.name, tags=tags)
        tag.date_created = dt.datetime.utcnow().strftime(dt_format)

        try:
            all_tags = self.metadata_db.get_all_tags_from_relic(
                self.name, self.relic_type, self.storage.name
            )

            saved = self.metadata_db.add_relic_tag(tag)

            all_tags.extend(saved)

            self.storage.put_tags([self.relic_type, self.name, "tags"], all_tags)

            return saved
        except Exception as e:
            logging.warning(f"Error adding tags: {tags} | {e} ")

    def list_tags(self) -> List[str]:
        return self.storage.get_tags([self.relic_type, self.name, "tags"])

    def add_image(self, name: str, image_bin: bytes):
        self.assert_valid_id(name)

        buffer = BytesIO(image_bin)
        size = buffer.getbuffer().nbytes
        buffer.seek(0)

        metadata = Metadata(
            name=name,
            data_type="images",
            relic_type=self.relic_type,
            storage_name=self.storage.name,
            size=size,
        )

        self.storage.put_binary_obj(
            [self.relic_type, self.name, "images", name], buffer
        )
        self._add_metadata(metadata)

    def get_image(self, name: str) -> BytesIO:
        self.assert_valid_id(name)

        return self.storage.get_binary_obj([self.relic_type, self.name, "images", name])

    def list_images(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "images"])


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
            self.storages = get_all_available_storages()
        self.metadata_db = MetadataDB()

    def query(self, statement: str) -> List:
        if self.storages:
            self._sync_relics()

        return self.metadata_db.query(statement)

    def _sync_relics(self) -> None:
        for stor in self.storages:
            metadata = stor.get_all_relic_metadata()
            tags = stor.get_all_relic_tags()

            for data in metadata:
                self.metadata_db.sync_metadata(Metadata.parse_dict(data))

            for tag in tags:

                self.metadata_db.sync_tags(RelicTag.parse_dict(tag))
