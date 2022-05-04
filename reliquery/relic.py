import logging
from tkinter import N
from reliquery.metadata import Metadata, MetadataDB, RelicData, RelicTag
from typing import List, Dict
from sys import getsizeof
from io import BytesIO

import numpy as np
import json
import pandas as pd

import nbconvert
import nbformat

from .storage import (
    get_all_available_storages,
    StorageItemDoesNotExist,
    get_storage_by_name,
    Storage,
)

from PIL import Image


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
            self.storage_name = storage.name

        if check_exists:
            self._ensure_exists()

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
        return RelicData(self.name, self.relic_type, self.storage_name)

    def add_array(self, name: str, array: np.ndarray):

        self.assert_valid_id(name)

        size = getsizeof(array)
        shape = str(np.array(array).shape)
        metadata = Metadata(
            name=name,
            data_type="arrays",
            relic=self._relic_data(),
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

    def remove_array(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "arrays", name])
        self._remove_metadata("arrays", name)

    def add_html_from_path(self, name: str, html_path: str) -> None:

        self.assert_valid_id(name)

        metadata = Metadata(name=name, data_type="html", relic=self._relic_data())

        self.storage.put_file([self.relic_type, self.name, "html", name], html_path)
        self._add_metadata(metadata)

    # TODO needs test coverage
    def add_html_string(self, name: str, html_str: str):
        self.assert_valid_id(name)

        metadata = Metadata(name=name, data_type="html", relic=self._relic_data())

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

    def remove_html(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "html", name])
        self._remove_metadata("html", name)

    def add_text(self, name: str, text: str) -> None:

        self.assert_valid_id(name)

        size = getsizeof(text)
        metadata = Metadata(
            name=name,
            data_type="text",
            relic=self._relic_data(),
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

    def remove_text(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "text", name])
        self._remove_metadata("text", name)

    def _add_metadata(self, metadata: Metadata) -> None:
        self.assert_valid_id(metadata.name)

        self.storage.put_metadata(
            [self.relic_type, self.name, "metadata", metadata.data_type, metadata.name],
            metadata.get_dict(),
        )

    def _remove_metadata(self, data_type, name) -> None:
        self.assert_valid_id(self.name)

        self.storage.remove_metadata(
            [self.relic_type, self.name, "metadata", f"{data_type}", f"{name}"]
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

    def add_image(self, name: str, image_bytes: BytesIO):
        self.assert_valid_id(name)

        size = image_bytes.getbuffer().nbytes
        metadata = Metadata(
            name=name,
            data_type="images",
            relic=self._relic_data(),
            size=size,
        )

        self.storage.put_binary_obj(
            [self.relic_type, self.name, "images", name], image_bytes
        )
        self._add_metadata(metadata)

    def add_image_from_path(self, name: str, image_path: str):
        self.assert_valid_id(name)
        # TODO: Make use of stream like capabilities instead of full read()s
        with open(image_path, "rb") as input_file:
            buffer = BytesIO(input_file.read())
            fileSize = buffer.getbuffer().nbytes
            metadata = Metadata(
                name=name,
                data_type="images",
                relic=self._relic_data(),
                size=fileSize,
            )
            self.storage.put_binary_obj(
                [self.relic_type, self.name, "images", name], buffer
            )
            self._add_metadata(metadata)

    def get_image(self, name: str) -> BytesIO:
        self.assert_valid_id(name)
        return self.storage.get_binary_obj([self.relic_type, self.name, "images", name])

    def get_pil_image(self, name: str) -> Image:
        self.assert_valid_id(name)
        img_data = self.storage.get_binary_obj(
            [self.relic_type, self.name, "images", name]
        )
        image = Image.open(img_data)
        return image

    def save_image_to_path(self, name: str, path: str) -> None:
        self.assert_valid_id(name)
        buffer = self.storage.get_binary_obj(
            [self.relic_type, self.name, "images", name]
        )
        content = buffer.read()
        with open(path, "wb") as new_file:
            new_file.write(content)

    def list_images(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "images"])

    def remove_image(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "images", name])
        self._remove_metadata("images", name)

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

    def get_json(self, name: str) -> Dict:
        self.assert_valid_id(name)

        json_text = self.storage.get_text([self.relic_type, self.name, "json", name])
        my_json = json.loads(json_text)
        return my_json

    def remove_json(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "json", name])
        self._remove_metadata("json", name)

    def add_pandasdf(self, name: str, pandas_data: pd.DataFrame) -> None:
        """
        Note that json is used to serialize which
        comes with other caveats that can be found here:
        https://pandas.pydata.org/pandas-docs/version/0.23/generated/pandas.DataFrame.to_json.html
        """
        self.assert_valid_id(name)

        json_pandasdf = pandas_data.to_json()
        pandasdf_size = getsizeof(pandas_data)

        metadata = Metadata(
            name=name,
            data_type="pandasdf",
            relic=self._relic_data(),
            size=pandasdf_size,
        )

        self.storage.put_text(
            [self.relic_type, self.name, "pandasdf", name], json_pandasdf
        )
        self._add_metadata(metadata)

    def list_pandasdf(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "pandasdf"])

    def get_pandasdf(self, name: str) -> pd.DataFrame:
        self.assert_valid_id(name)

        pandas_json = self.storage.get_text(
            [self.relic_type, self.name, "pandasdf", name]
        )
        pandas_dataframe = pd.read_json(pandas_json)
        return pandas_dataframe

    def remove_pandasdf(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "pandasdf", name])
        self._remove_metadata("pandasdf", name)

    def add_files_from_path(self, name: str, path: str) -> None:
        self.assert_valid_id(name)
        # TODO: Make use of stream like capabilities instead of full read()s
        with open(path, "rb") as input_file:
            buffer = BytesIO(input_file.read())
            fileSize = buffer.getbuffer().nbytes
            metadata = Metadata(
                name=name,
                data_type="files",
                relic=self._relic_data(),
                size=fileSize,
            )
            self.storage.put_binary_obj(
                [self.relic_type, self.name, "files", name], buffer
            )
            self._add_metadata(metadata)

    def save_files_to_path(self, name: str, path: str) -> None:
        buffer = self.storage.get_binary_obj(
            [self.relic_type, self.name, "files", name]
        )
        content = buffer.read()
        with open(path, "wb") as new_file:
            new_file.write(content)

    # TODO Add file like object

    def list_files(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "files"])

    def get_file(self, name: str) -> BytesIO:
        self.assert_valid_id(name)

        return self.storage.get_binary_obj([self.relic_type, self.name, "files", name])

    def remove_file(self, name: str) -> None:
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "files", name])
        self._remove_metadata("files", name)

    def add_notebook_from_path(self, name: str, path: str) -> None:
        self.assert_valid_id(name)
        # TODO: Make use of stream like capabilities instead of full read()s
        with open(path, "rb") as input_file:
            fileString = input_file.read()
            buffer = BytesIO(fileString)
            fileSize = buffer.getbuffer().nbytes
            metadata = Metadata(
                name=name,
                data_type="notebooks",
                relic=self._relic_data(),
                size=fileSize,
            )
            self.storage.put_binary_obj(
                [self.relic_type, self.name, "notebooks", name], buffer
            )
            self._add_metadata(metadata)

            exporter = nbconvert.HTMLExporter()
            exporter.template_name = "classic"
            note = nbformat.reads(fileString, as_version=4)
            (body, resources) = exporter.from_notebook_node(note)

            self.storage.put_text(
                [self.relic_type, self.name, "notebooks-html", name], body
            )

    def list_notebooks(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "notebooks"])

    def get_notebook(self, name: str) -> BytesIO:
        self.assert_valid_id(name)

        return self.storage.get_binary_obj(
            [self.relic_type, self.name, "notebooks", name]
        )

    def save_notebook_to_path(self, name: str, path: str) -> None:
        buffer = self.storage.get_binary_obj(
            [self.relic_type, self.name, "notebooks", name]
        )
        content = buffer.read()
        with open(path, "wb") as new_file:
            new_file.write(content)

    def get_notebook_html(self, name: str) -> str:
        self.assert_valid_id(name)

        return self.storage.get_text(
            [self.relic_type, self.name, "notebooks-html", name]
        )

    def remove_notebook(self, name: str) -> None:
        self.assert_valid_id(name)
        self.storage.remove_obj([self.relic_type, self.name, "notebooks", name])
        self.storage.remove_obj([self.relic_type, self.name, "notebooks-html", name])
        self._remove_metadata("notebooks", name)

    def add_video_from_path(self, name: str, video_path: str):
        self.assert_valid_id(name)
        with open(video_path, "rb") as input_file:
            buffer = BytesIO(input_file.read())
            fileSize = buffer.getbuffer().nbytes
            metadata = Metadata(
                name=name,
                data_type="videos",
                relic=self._relic_data(),
                size=fileSize,
            )
            self.storage.put_binary_obj(
                [self.relic_type, self.name, "videos", name], buffer
            )
            self._add_metadata(metadata)

    def get_video(self, name: str, ext:str) -> BytesIO:
        self.assert_valid_id(name)
        return self.storage.get_binary_obj([self.relic_type, self.name, "videos", name])

    def save_video_to_path(self, name: str, path: str) -> None:
        self.assert_valid_id(name)
        buffer = self.storage.get_binary_obj(
            [self.relic_type, self.name, "videos", name]
        )
        content = buffer.read()
        with open(path, "wb") as new_file:
            new_file.write(content)

    def list_videos(self) -> List[str]:
        return self.storage.list_keys([self.relic_type, self.name, "videos"])

    def remove_video(self, name: str) -> None:
        self.assert_valid_id(name)
        self.storage.remove_obj([self.relic_type, self.name, "videos", name])
        self._remove_metadata("videos", name)


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
        list_ids = []

        for stor in self.storages:
            relic_datas = [
                self.metadata_db.sync_relic_data(RelicData.parse_dict(data))
                for data in stor.get_all_relic_data()
            ]
            list_ids.extend([i.id for i in relic_datas])

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
        self.metadata_db.remove_old_relic_data(list_ids)

    def get_relic_types_by_storage(self, storage: str) -> List[str]:
        return self.metadata_db.get_relic_types_by_storage(storage)

    def get_relic_names_by_storage_and_type(
        self, storage: str, relic_type: str
    ) -> List[Dict]:
        return self.metadata_db.get_relic_names_by_storage_and_type(storage, relic_type)

    def get_relic_names(self) -> List[Dict]:
        return self.metadata_db.get_all_relic_names()

    def get_unique_relic_types_and_storages(self) -> List[Dict]:
        return self.metadata_db.get_unique_relic_types_and_storages()

    def get_relic_names_by_storage(self, storage: str) -> List[Dict]:
        return self.metadata_db.get_relic_names_by_storage(storage)

    def sync_reliquery(self) -> None:
        self._sync_relics()

    def remove_relic(self, relic: Relic) -> int:
        relic.storage.remove_relic([relic.relic_type, relic.name])
        return self.metadata_db.delete_relic(
            relic.name, relic.relic_type, relic.storage_name
        )
