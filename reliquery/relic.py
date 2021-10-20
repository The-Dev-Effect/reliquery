import logging
from reliquery.metadata import Metadata, MetadataDB, RelicData, RelicTag
from typing import List, Dict
from sys import getsizeof
from io import BytesIO

import numpy as np
import json


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
        self.data = self.metadata_db.sync_relic_data(
            RelicData(self.name, self.relic_type, self.storage_name)
        )

    @classmethod
    def assert_valid_id(cls, id: str):
        if id == "":
            raise InvalidRelicId()

    def _ensure_exists(self):
        try:
            self.storage.get_text([self.relic_type, self.name, "exists"])
        except StorageItemDoesNotExist:
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

    def _relic_data(self):
        return self.metadata_db.get_relic_data_by_name(
            self.name, self.relic_type, self.storage_name
        )

    def add_array(self, name: str, array: np.ndarray):

        self.assert_valid_id(name)

        size = getsizeof(array)
        shape = str(np.array(array).shape)
        metadata = Metadata(
            name=name,
            data_type="arrays",
            relic=self.data,
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

        metadata = Metadata(name=name, data_type="html", relic=self.data)

        self.storage.put_file([self.relic_type, self.name, "html", name], html_path)
        self._add_metadata(metadata)

    # TODO needs test coverage
    def add_html_string(self, name: str, html_str: str):
        self.assert_valid_id(name)

        metadata = Metadata(name=name, data_type="html", relic=self.data)

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
            relic=self.data,
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

    def add_tag(self, tags: Dict) -> None:
        tags_path = [self.relic_type, self.name, "tags"]

        curr_tags = self.storage.get_tags(tags_path)
        curr_tags.update(tags)

        self.storage.put_tags(tags_path, curr_tags)

    def list_tags(self) -> Dict:
        return self.storage.get_tags([self.relic_type, self.name, "tags"])

    def add_image(self, name: str, image_bin: bytes):
        self.assert_valid_id(name)

        buffer = BytesIO(image_bin)
        size = buffer.getbuffer().nbytes
        buffer.seek(0)

        metadata = Metadata(
            name=name,
            data_type="images",
            relic=self.data,
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

    def add_json(self, name: str, json_data: Dict) -> None:
        self.assert_valid_id(name)

        json_text = json.dumps(json_data)
        size = getsizeof(json_data)
        metadata = Metadata(
            name=name,
            data_type="json",
            relic=self._relic_data(),
            size=size,
        )

        self.storage.put_text([self.relic_type, self.name, "json", name], json_text)
        self._add_metadata(metadata)

    def list_json(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "json"])

    def get_json(self, name: str) -> dict:
        self.assert_valid_id(name)

        json_text = self.storage.get_text([self.relic_type, self.name, "json", name])
        my_json = json.loads(json_text)
        return my_json


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

        self.storage_map = {s.name: s for s in self.storages}
        self.metadata_db = MetadataDB()
        self._sync_relics()

    def query(self, statement: str) -> List:
        return self.metadata_db.query(statement)

    def get_relics_by_tag(self, key: str, value: str) -> List[Relic]:
        """
        Query relics for user defined tags added to Relics.

        Parameters
        ----------

        key : string
        value : string

        Returns
        -------
        List of Relic objects
            Reclics associated with the key, values given
        """
        return [
            Relic(name=data[0], relic_type=data[1], storage=self.storage_map[data[2]])
            for data in self.metadata_db.get_relics_by_tag(key, value)
        ]

    def _sync_relics(self) -> None:
        """
        Syncs Relics from all available storages with the in-memory database.
        This is done on the initial creation of a Reliquery object and prior
        to any queries over Relics.
        """
        for stor in self.storages:
            relic_datas = [
                self.metadata_db.sync_relic_data(RelicData.parse_dict(data))
                for data in stor.get_all_relic_data()
            ]

            if relic_datas:
                for relic_data in relic_datas:
                    self.metadata_db.sync_tags(
                        RelicTag.parse_dict(
                            stor.get_tags(
                                [relic_data.relic_type, relic_data.relic_name, "tags"]
                            ),
                            relic_data,
                        )
                    )
