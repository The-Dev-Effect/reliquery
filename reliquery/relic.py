import logging
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


StoragePath = List[str]


class InvalidRelicId(Exception):
    pass


class Relic:
    """A Relic represents a collection of storage results from scientific exploration.

    This class is used to store and results, notations and, scienticic materials used in
    research. 

    Attributes:
        name(str): Name of relic.
        relic_type(str): Type of relic.
        storage(Storage): Storage object defaults to 'None'.
        storage_name:(str): Name of storage defaults to 'default'.
        check_exists(boolean): If True checks if relic aleady exists.

    Methods:
        add_array(name, array) -> None:
        list_arrays() -> List[str]:
        get_array(name) -> numpy.ndarray:
        remove_array() -> None:
        add_html_from_path(name, html_path):
        add_html_string(name, html_string)
        list_html() -> List[str]:
        get_html(name) -> str:
        remove_html() -> None:
        add_text(name, text) -> None:
        list_text() -> List[str]:
        get_text(name) -> str:
        remove_text() -> None:
        describe() -> Dict:
        add_tag(tags) -> None:
        list_tags() -> Dict:
        add_image(name, image_bin) -> None:
        get_image(name) -> BytesIO:
        list_images() -> List[str]:
        remove_image(name) -> None:
        add_json(name, json_data) -> None:
        list_json() -> List[str]:
        get_json(name) -> Dict:
        remove_json(name) -> None:
        add_pandasdf(name, pandas_data):
        list_pandasdf() -> List[str]
        get_pandasdf(name) -> pandas.DataFrame:
        remove_pandasdf(name) -> None:
        list_files() -> List[str]:
        get_file(name) -> BytesIO:
        remove_file(name) -> None:
        add_notebook_from_path(name, path) -> None:
        list_notebooks() -> List[str]:
        save_notebook_to_path(name, path) -> None:
        remove_notebook(name) -> None:

    """

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

        """
        since querying relics is not currently supproted there is
        no need to create database object to persist relic data.
        """
        # self.metadata_db = MetadataDB()
        # self.data = self.metadata_db.sync_relic_data(
        #     RelicData(self.name, self.relic_type, self.storage_name)

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
        """Adds an array by name

        Args:
            name: id to save array by
            array: numpy.ndarray to be stored on the Relic

         Raises:
            InvalidRelicId: Name of array is in an incorrect format

        """
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
        """Fetches an array by a given name

        Args:
            name (str): Name of array to fetch
        
        Returns:
            A numpy ndimensional array

        Raises:
            StorageItemDoesNotExist: When an array is not found by the given name
            InvalidRelicId: Name of array is in an incorrect format

        """
        self.assert_valid_id(name)

        with self.storage.get_binary_obj(
            [self.relic_type, self.name, "arrays", name]
        ) as f:
            return np.load(f, allow_pickle=False)

    def list_arrays(self) -> List[np.ndarray]:
        """List of names of arrays stored
        
        Returns:
            List of all array names stored on Relic
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "arrays"])

    def remove_array(self, name: str) -> None:
        """Remove array by name
        
        Args:
            name: name of array to be removed

        Raises:
            StorageItemDoesNotExist: When there is no array found by the given name
            InvalidRelicId: Name of array is in an incorrect format

        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "arrays", name])

    def add_html_from_path(self, name: str, html_path: str) -> None:
        """Adds html from a path
        
        Args:
            name: id to add html as
            html_path: path where html is located
        
        Raises:
            InvalidRelicId: Name of html is in an incorrect format
        
        """
        self.assert_valid_id(name)

        metadata = Metadata(name=name, data_type="html", relic=self._relic_data())

        self.storage.put_file([self.relic_type, self.name, "html", name], html_path)
        self._add_metadata(metadata)

    # TODO needs test coverage
    def add_html_string(self, name: str, html_str: str):
        """Adds html as a string to Relic
        
        Args:
            name: id to store html as
            html_str: html as a string

        Raises:
            InvalidRelicId: Name of html is in an incorrect format
        
        """
        self.assert_valid_id(name)

        metadata = Metadata(name=name, data_type="html", relic=self._relic_data())

        self.storage.put_text([self.relic_type, self.name, "html", name], html_str)

        self._add_metadata(metadata)

    def list_html(self) -> List[str]:
        """List names of html
        
        Returns:
            List of html names stored on the Relic
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "html"])

    def get_html(self, name: str) -> str:
        """Fetches html by name
        
        Args:
            name: name of html stored

        Returns:
            Html as a string

        Raises:
            StorageItemDoesNotExist: When there is no html found by the given name
            InvalidRelicId: Name of html is in an incorrect format
        
        """
        self.assert_valid_id(name)

        with self.storage.get_binary_obj(
            [self.relic_type, self.name, "html", name]
        ) as f:
            return f.read().decode("utf-8")

    def remove_html(self, name: str) -> None:
        """Removes html by name
        
        Args:
            name: name of html to be removed
        
        Raises:
            StorageItemDoesNotExist: When there is no html found by the given name
            InvalidRelicId: Name of html is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "html", name])

    def add_text(self, name: str, text: str) -> None:
        """Adds text by name
        
        Args:
            name: id to store text by
            text: string of text to be stored

        Raises:
            InvalidRelicId: Name of text is in an incorrect format
        
        """
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
        """Lists names of text stored
        
        Returns:
            List of text names stored on the Relic
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "text"])

    def get_text(self, name: str) -> str:
        """Fetches text by name
        
        Args:
            name: name of text stored

        Returns:
            The text stored as a string

        Raises:
            StorageItemDoesNotExist: When there is no text found by the given name
            InvalidRelicId: Name of text is in an incorrect format
        
        """
        self.assert_valid_id(name)

        return self.storage.get_text([self.relic_type, self.name, "text", name])

    def remove_text(self, name: str) -> None:
        """Remove text by name
        
        Args:
            name: name of text to be removed

        Raises:
            StorageItemDoesNotExist: When there is no text found by the given name
            InvalidRelicId: Name of text is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "text", name])

    def _add_metadata(self, metadata: Metadata) -> None:
        self.assert_valid_id(metadata.name)

        self.storage.put_metadata(
            [self.relic_type, self.name, "metadata", metadata.data_type, metadata.name],
            metadata.get_dict(),
        )

    def describe(self) -> Dict:
        """Describes the contents stored on a Relic
        
        Returns:
            Dictionary of Relic content metadata

            Example:
                {'relic': {'arrays': [],
                    'text': [],
                    'html': [],
                    'images': [{'id': None,
                        'name': 'draft-logo.png',
                        'data_type': 'images',
                        'relic_name': 'relic',
                        'relic_type': 'dev-team',
                        'size': 23211,
                        'shape': None,
                        'last_modified': '11/24/2021 15:27:08'},
                    'json': [],
                    'pandasdf': [],
                    'files': [],
                    'notebooks': []}}
        
        """
        return self.storage.get_metadata(
            [self.relic_type, self.name, "metadata"], self.name
        )

    def add_tag(self, tags: Dict) -> None:
        """Adds tags to a Relic
        
        Args:
            tags: dictionary of key value pairs

            Example:
                {
                    "author": "tesla",
                    "pre-process": "PCA",
                    ...
                }
        
        """
        tags_path = [self.relic_type, self.name, "tags"]

        curr_tags = self.storage.get_tags(tags_path)
        curr_tags.update(tags)

        self.storage.put_tags(tags_path, curr_tags)

    def list_tags(self) -> Dict:
        """Lists tags on a Relic
        
        Returns:
            A dictionary containing all the key value pairs that make up tags
        
        """
        return self.storage.get_tags([self.relic_type, self.name, "tags"])

    def add_image(self, name: str, image_bin: bytes):
        """Adds image to Relic
        
        Args:
            name: id to store image by
            image_bin: image as binary 

        Raises:
            InvalidRelicId: Name of image is in an incorrect format
        
        """
        self.assert_valid_id(name)

        buffer = BytesIO(image_bin)
        size = buffer.getbuffer().nbytes
        buffer.seek(0)

        metadata = Metadata(
            name=name,
            data_type="images",
            relic=self._relic_data(),
            size=size,
        )

        self.storage.put_binary_obj(
            [self.relic_type, self.name, "images", name], buffer
        )
        self._add_metadata(metadata)

    def get_image(self, name: str) -> BytesIO:
        """Fetches an image by name
        
        Args:
            name: name of the image stored

        Returns:
            The image as a file-object. BytesIO

        Raises:
            StorageItemDoesNotExist: When there is no image found by the given name
            InvalidRelicId: Name of image is in an incorrect format
        
        """
        self.assert_valid_id(name)

        return self.storage.get_binary_obj([self.relic_type, self.name, "images", name])

    def list_images(self) -> List[str]:
        """Lists all the names of images stored
        
        Returns:
            list of image names that are stored on the Relic
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "images"])

    def remove_image(self, name: str) -> None:
        """Removes an image by name
        
        Args:
            name: name of image to be removed

        Raises:
            StorageItemDoesNotExist: When there is no image found by the given name
            InvalidRelicId: Name of image is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "images", name])

    def add_json(self, name: str, json_data: Dict) -> None:
        """Adds json to Relic
        
        Args:
            name: id to store json to Relic with.
            json_data: Python dictionary object to be converted to json

        Raises:
            InvalidRelicId: Name of json is in an incorrect format
        
        """
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
        """Lists names of all json stored
        
        Returns:
            List of json names that are stored on the Relic
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "json"])

    def get_json(self, name: str) -> Dict:
        """Fetches json from Relic by name
        
        Args:
            name: name of json stored on Relic

        Returns:
            A python dictionary object formatted from the json stored

        Raises:
            StorageItemDoesNotExist: When there is no json found by the given name
            InvalidRelicId: Name of json is in an incorrect format
        
        """
        self.assert_valid_id(name)

        json_text = self.storage.get_text([self.relic_type, self.name, "json", name])
        my_json = json.loads(json_text)
        return my_json

    def remove_json(self, name: str) -> None:
        """Removes json stored by name
        
        Args:
            name: name of json to be removed from Relic

        Raises:
            StorageItemDoesNotExist: When there is no json found by the given name
            InvalidRelicId: Name of json is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "json", name])

    def add_pandasdf(self, name: str, pandas_data: pd.DataFrame) -> None:
        """Adds a dataframe to Relic by name

        Args:
            name: 
                id given to the dataframe 
            pandas_data: 
                Pandas DataFrame to stored as JSON on Relic. 
                Note: 
                    that json is used to serialize which comes with other caveats that can be found here:
                    https://pandas.pydata.org/pandas-docs/version/0.23/generated/pandas.DataFrame.to_json.html

        Raises:
            InvalidRelicId: Name of dataframe is in an incorrect format


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
        """Lists all dataframe names stored
        
        Returns:
            List of dataframe names that are stored on Relic
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "pandasdf"])

    def get_pandasdf(self, name: str) -> pd.DataFrame:
        """Fetches dataframe by name
        
        Args:
            name: name of dataframe

        Raises:
            StorageItemDoesNotExist: When there is no dataframe found by the given name
            InvalidRelicId: Name of dataframe is in an incorrect format
        
        """
        self.assert_valid_id(name)

        pandas_json = self.storage.get_text(
            [self.relic_type, self.name, "pandasdf", name]
        )
        pandas_dataframe = pd.read_json(pandas_json)
        return pandas_dataframe

    def remove_pandasdf(self, name: str) -> None:
        """Removes a dataframe by name
        
        Args:
            name: 
                name of the dataframe to be removed.

        Raises:
            StorageItemDoesNotExist: When there is no dataframe found by the given name
            InvalidRelicId: Name of dataframe is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "pandasdf", name])

    def add_files_from_path(self, name: str, path: str) -> None:
        """Adds file from the path given
        
        Args:
            name: id given to file to be stored
            path: path where the file is to be read from

        Raises:
            InvalidRelicId: Name of file is in an incorrect format
        
        """
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
        """Writes a file to the given path
        
        Args:
            name: name of the file stored on the Relic
            path: path to where the file is written

        Raises:
            StorageItemDoesNotExist: When there is no file found by the given name
            InvalidRelicId: Name of file is in an incorrect format
        
        """
        buffer = self.storage.get_binary_obj(
            [self.relic_type, self.name, "files", name]
        )
        content = buffer.read()
        with open(path, "wb") as new_file:
            new_file.write(content)

    # TODO Add file like object

    def list_files(self) -> List[str]:
        """Lists all the files stored on the Relic
        
        Returns:
            A list of file names stored
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "files"])

    def get_file(self, name: str) -> BytesIO:
        """Fetches a file by given name
        
        Args:
            name: Name of the file to be fetched

        Raises:
            StorageItemDoesNotExist: When there is no file found by the given name
            InvalidRelicId: Name of file is in an incorrect format
        
        """
        self.assert_valid_id(name)

        return self.storage.get_binary_obj([self.relic_type, self.name, "files", name])

    def remove_file(self, name: str) -> None:
        """Removes file by name
        
        Args:
            name: Name of the file to be removed.
        
        Raises:
            StorageItemDoesNotExist: When there is no file found by the given name
            InvalidRelicId: Name of file is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "files", name])

    def add_notebook_from_path(self, name: str, path: str) -> None:
        """Adds notebook from a given path
        
        Args:
            name: id to store notebook by.
            path: path where to find the notebook to be stored.

        Raises:
            InvalidRelicId: Name of notebook is in an incorrect format
        
        """
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
        """Lists stored notebooks
        
        Returns:
            List of notebook names stored on the Relic.
        
        """
        return self.storage.list_keys([self.relic_type, self.name, "notebooks"])

    def get_notebook(self, name: str) -> BytesIO:
        """Fetches notebook by name
        
        Args:
            name: Name of the notebook

        Returns:
            Returns a the notebook as a file-like object. BytesIO
        
        Raises:
            StorageItemDoesNotExist: When there is no notebook found by the given name
            InvalidRelicId: Name of notebook is in an incorrect format
        
        """
        self.assert_valid_id(name)

        return self.storage.get_binary_obj(
            [self.relic_type, self.name, "notebooks", name]
        )

    def save_notebook_to_path(self, name: str, path: str) -> None:
        """Writes a notebook file to the given path
        
        Args:
            name: Name of notebook
            path: Path to where notebook file is to be written to.

        Raises:
            StorageItemDoesNotExist: When there is no notebook found by the given name
            InvalidRelicId: Name of notebook is in an incorrect format
        
        """
        buffer = self.storage.get_binary_obj(
            [self.relic_type, self.name, "notebooks", name]
        )
        content = buffer.read()
        with open(path, "wb") as new_file:
            new_file.write(content)

    def get_notebook_html(self, name: str) -> str:
        """Fetches rendered notebook html
        
        Args:
            name: Name of notebook to fetch html for

        Returns:
            HTML of the notebook as a string.
        
        Raises:
            StorageItemDoesNotExist: When there is no notebook found by the given name
            InvalidRelicId: Name of notebook is in an incorrect format
        """
        self.assert_valid_id(name)

        return self.storage.get_text(
            [self.relic_type, self.name, "notebooks-html", name]
        )

    def remove_notebook(self, name: str) -> None:
        """Removes notebook by name
        
        Args:
            name (str): name of the notebook to be removed

        Raises:
            StorageItemDoesNotExist: When there is no notebook found by the given name
            InvalidRelicId: Name of notebook is in an incorrect format
        
        """
        self.assert_valid_id(name)

        self.storage.remove_obj([self.relic_type, self.name, "notebooks", name])

        self.storage.remove_obj([self.relic_type, self.name, "notebooks-html", name])


class Reliquery:
    """ Class used to query over available and accessible storages and Relics.
    
    Attributes:
        storages: List of Storage objects availble to Reliquery
        storage_map: Dictionary of storages mapped to storage names as keys

    Methods:
        query(statement) -> List:
        get_relics_by_tag(key, value) -> List[Relic]:
        sync_reliquery() -> None:
        get_relic_types_by_storage(storage) -> List[str]:
        get_relic_names_by_storage_and_type(storage, relic_type) -> List[Dict]:
        get_relic_names() -> List[Dict]:
        get_unique_relic_types_and_storages() -> List[Dict]:
        get_relic_names_by_storage(storage) -> List[Dict]:
        remove_relic(relic) -> int:

    """


    def __init__(self, storages: List[Storage] = []) -> None:
        """ Initializes Reliquery class.

        Args:
            storages: List of Storage objects given to query over defaults to none.
        """
        if len(storages) > 0:
            self.storages = storages
        else:
            self.storages = get_all_available_storages()

        self.storage_map = {s.name: s for s in self.storages}
        self._metadata_db = MetadataDB()
        self._sync_relics()

    def query(self, statement: str) -> List:
        """Queries Reliquery using SQL Statements.
        
        Args:
            statement: SQL like string used to query Reliquery.
            
        Returns:
            List of rows returned from SQL query.

        """
        return self._metadata_db.query(statement)

    def get_relics_by_tag(self, key: str, value: str) -> List[Relic]:
        """
        Query relics for user defined tags added to Relics.
        
        Args:
            key: string.
            value: string.

        Returns:
            A list of Relic objects associated with the key, values.

        """
        return [
            Relic(name=data[0], relic_type=data[1], storage=self.storage_map[data[2]])
            for data in self._metadata_db.get_relics_by_tag(key, value)
        ]

    def _sync_relics(self) -> None:
        """Syncs Relics from all available storages with the in-memory database.
        
        This is done on the initial creation of a Reliquery object and prior
        to any queries over Relics.

        """
        list_ids = []

        for stor in self.storages:
            relic_datas = [
                self._metadata_db.sync_relic_data(RelicData.parse_dict(data))
                for data in stor.get_all_relic_data()
            ]
            list_ids.extend([i.id for i in relic_datas])

            if relic_datas:
                for relic_data in relic_datas:
                    self._metadata_db.sync_tags(
                        RelicTag.parse_dict(
                            stor.get_tags(
                                [relic_data.relic_type, relic_data.relic_name, "tags"]
                            ),
                            relic_data,
                        )
                    )

        self._metadata_db.remove_old_relic_data(list_ids)

    def get_relic_types_by_storage(self, storage: str) -> List[str]:
        """Fetches all relic types from given storage name
        
        Args:
            storage (str): name of the storage where to fetch relic types from

        Returns:
            A list of strings that are the all the relic types in a storage.
        
        """
        return self._metadata_db.get_relic_types_by_storage(storage)

    def get_relic_names_by_storage_and_type(
        self, storage: str, relic_type: str
    ) -> List[Dict]:
        """Fetches all Relic names by storage and type
        
        Args:
            storage (str): Name of which storage to fetch names from.
            relic_type (str): Name of which relic types to fetch from

        Returns:
            Dictionary list of mapped Relic names that are in of the given type
            and are in the given storage.

            Example:
            [
                {
                    "name": "trial-1",
                    "type": "calcium-imaging",
                    "storage": "local"
                },
                {
                    "name": "trial-2",
                    "type": "emg-data",
                    "storage": "local"
                },
                ...
            ]
        
        """
        return self._metadata_db.get_relic_names_by_storage_and_type(storage, relic_type)

    def get_relic_names(self) -> List[Dict]:
        """Fetchs all avaiable Relic names
        
        Returns:
            Dictionary list of mapped Relic names

            Example:
            [
                {
                    "name": "trial-1",
                    "type": "calcium-imaging",
                    "storage": "local"
                },
                {
                    "name": "trial-2",
                    "type": "emg-data",
                    "storage": "local"
                },
                ...
            ]

        """
        return self._metadata_db.get_all_relic_names()

    def get_unique_relic_types_and_storages(self) -> List[Dict]:
        """Fetchs unique Relic types and storages

        Returns:
            Dictionary list of mapped unique pairs of types and storages

            Example:
            [
                {
                    "type": "calcium-imaging",
                    "storage": "local"
                },
                {
                    "type": "emg-data",
                    "storage": "local"
                },
                ...
            ]
        
        """
        return self._metadata_db.get_unique_relic_types_and_storages()

    def get_relic_names_by_storage(self, storage: str) -> List[Dict]:
        """Fetchs Relic names from a given storage
        
        Args:
            storage (str): Name of the storage to get relics from

        """
        return self._metadata_db.get_relic_names_by_storage(storage)

    def sync_reliquery(self) -> None:
        """Syncs Reliquery with available storages"""
        self._sync_relics()

    def remove_relic(self, relic: Relic) -> int:
        """Removes a Relic from Reliquery

        Args:
            relic (Relic): The relic to be removed

        Returns:
            The number of relics removed
        
        """
        relic.storage.remove_relic([relic.relic_type, relic.name])
        return self._metadata_db.delete_relic(
            relic.name, relic.relic_type, relic.storage_name
        )
