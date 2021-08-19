from typing import List

from io import BytesIO

import numpy as np

from .storage import StorageItemDoesNotExist, get_default_storage, Storage


StoragePath = List[str]


class InvalidRelicId(Exception):
    pass


class Relic:
    name: str
    relic_type: str
    storage: Storage

    def __init__(self, name: str, relic_type: str, storage: Storage = None):
        self.name = name
        self.relic_type = relic_type

        if storage is None:
            self.storage = get_default_storage()
        else:
            self.storage = storage

        self._ensure_exists()

    @classmethod
    def assert_valid_id(cls, id: str):
        if id == "":
            raise InvalidRelicId()

    def _ensure_exists(self):
        self.storage.put_text([self.relic_type, self.name, "exists"], "exists")

    @classmethod
    def relic_exists(
        cls, name: str, relic_type: str, storage: Storage = None
    ) -> bool:
        if storage is None:
            storage = get_default_storage()

        try:
            storage.get_text([relic_type, name, "exists"])
        except StorageItemDoesNotExist:
            return False

        return True

    def add_array(self, name: str, array: np.ndarray):
        self.assert_valid_id(name)

        buffer = BytesIO()
        np.save(buffer, array, allow_pickle=False)
        buffer.seek(0)

        self.storage.put_binary_obj(
            [self.relic_type, self.name, "arrays", name], buffer
        )

    def get_array(self, name: str) -> np.ndarray:
        self.assert_valid_id(name)

        with self.storage.get_binary_obj(
            [self.relic_type, self.name, "arrays", name]
        ) as f:
            return np.load(f, allow_pickle=False)

    def list_arrays(self) -> List[np.ndarray]:
        return self.storage.list_keys([self.relic_type, self.name, "arrays"])
