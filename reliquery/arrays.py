from typing import Set

from io import BytesIO
from .property import Serializer

try:
    import numpy as np

    has_numpy = True
except:
    has_numpy = False

if has_numpy:

    class Arrays(Serializer[np.ndarray]):
        programmatic_name = "arrays"

        def serialize(self, item: np.ndarray):
            buffer = BytesIO()
            raw_data = np.save(buffer, item, allow_pickle=False)

            buffer.seek(0)

            return buffer

        def deserialize(self, buffer: BytesIO) -> np.ndarray:
            return np.load(buffer, buffer, allow_pickle=False)

        def __setitem__(self, key: str, item: np.ndarray):
            # Serialize the data.
            buffer = BytesIO()
            raw_data = np.save(buffer, item, allow_pickle=False)

            buffer.seek(0)

            # Push the data.
            self.relic.upload_data("arrays", key)

        def __getitem__(self, key: str) -> np.ndarray:
            pass

        def keys(self) -> Set[str]:
            return set()
