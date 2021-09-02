import os
from io import BytesIO, BufferedIOBase
from typing import Any, List
from shutil import copyfile

import boto3
from botocore import UNSIGNED
from botocore.client import Config


from . import settings

StoragePath = List[str]


class StorageItemDoesNotExist(Exception):
    pass


class Storage:
    def put_file(self, path: StoragePath, file_path: str) -> None:
        raise NotImplementedError

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO):
        raise NotImplementedError

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO):
        raise NotImplementedError

    def get_binary_obj(self, path: StoragePath) -> BytesIO:
        raise NotImplementedError

    def put_text(self, path: StoragePath, buffer: BytesIO) -> None:
        raise NotImplementedError

    def get_text(self, path: StoragePath) -> BytesIO:
        raise NotImplementedError

    def list_keys(self, path: StoragePath) -> List[str]:
        raise NotImplementedError

    def put_metadata(self, path: StoragePath, metadata: Dict):
        raise NotImplementedError

    def list_metadata(self, path: StoragePath) -> List[str]:
        raise NotImplementedError


class FileStorage:
    def __init__(self, root: str):
        self.root = root

    def _ensure_path(self, path: StoragePath):
        if not os.path.exists(self._join_path(path[:-1])):
            os.makedirs(self._join_path(path[:-1]), exist_ok=True)

    def _join_path(self, path: StoragePath) -> str:
        return os.path.join(*[self.root] + path)

    def put_file(self, path: StoragePath, file_path: str) -> None:
        self._ensure_path(path)
        copyfile(file_path, self._join_path(path))

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO) -> None:
        self._ensure_path(path)
        with open(self._join_path(path), "wb") as f:
            f.write(buffer.getbuffer())

    def get_binary_obj(self, path: StoragePath) -> BytesIO:
        return open(self._join_path(path), "rb")

    def put_text(self, path: StoragePath, text: str) -> None:
        self._ensure_path(path)

        with open(self._join_path(path), "w") as f:
            return f.write(text)

    def get_text(self, path: StoragePath) -> str:
        try:
            with open(self._join_path(path), "r") as f:
                return f.read()
        except IOError:
            raise StorageItemDoesNotExist

    def list_keys(self, path: StoragePath) -> List[str]:
        joined_path = self._join_path(path)
        if not os.path.exists(joined_path):
            return []

        if not os.path.isdir(joined_path):
            return []

        return os.listdir(joined_path)


    def list_metadata(self, path: StoragePath) -> List[str]:
        joined_path = self._join_path(path)
        if not os.path.exists(joined_path):
            return []

        if not os.path.isdir(joined_path):
            return []
        f = []
        for root, dirs, files in os.walk(joined_path):
            [f.append(i) for i in files]

        return f


S3Client = Any


def _get_s3_client(signed) -> S3Client:
    if signed:
        return boto3.client("s3")
    else:
        return boto3.client("s3", config=Config(signature_version=UNSIGNED))


class S3Storage(Storage):
    def __init__(self, s3_bucket: str, prefix: str, *, s3_signed: bool = True):
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.signed = s3_signed

        self.s3 = _get_s3_client(self.signed)

    def _join_path(self, path: StoragePath) -> str:
        return "/".join([self.prefix] + path)

    def put_file(self, path: StoragePath, file_path: str) -> None:
        self.s3.upload_file(file_path, self.s3_bucket, self._join_path(path))

    def put_binary_obj(self, path: StoragePath, buffer: BufferedIOBase) -> None:
        print(buffer)

        self.s3.upload_fileobj(buffer, self.s3_bucket, self._join_path(path))

    def get_binary_obj(self, path: StoragePath) -> BufferedIOBase:
        buffer = BytesIO()

        self.s3.download_fileobj(self.s3_bucket, self._join_path(path), buffer)

        buffer.seek(0)

        return buffer

    def put_text(self, path: StoragePath, text: str, encoding: str = "utf-8") -> None:
        self.s3.put_object(
            Key=self._join_path(path), Bucket=self.s3_bucket, Body=text.encode(encoding)
        )

    def get_text(self, path: StoragePath, encoding: str = "utf-8") -> str:
        try:
            obj = self.s3.get_object(Key=self._join_path(path), Bucket=self.s3_bucket)
        except self.s3.exceptions.NoSuchKey:
            raise StorageItemDoesNotExist

        return obj["Body"].read().decode(encoding)

    def list_keys(self, path: StoragePath) -> List[str]:
        prefix = self._join_path(path)

        def process_key(k):
            return k[len(prefix) + 1 :]

        is_truncated = True
        keys = []
        kwargs = {}
        while is_truncated:
            response = self.s3.list_objects_v2(
                Bucket=self.s3_bucket, Prefix=prefix, **kwargs
            )

            is_truncated = response["IsTruncated"]
            if is_truncated:
                kwargs = dict(ContinuationToken=response["NextContinuationToken"])

            keys.extend([process_key(c["Key"]) for c in response["Contents"]])

        # we want to remove the prefx
        return keys

    def put_metadata(self, path: Storage, metadata: Dict):
        self.s3.put_object(
            Key=self._join_path(path), Bucket=self.s3_bucket, Body=json.dumps(metadata)
        )

    def list_metadata(self, path: StoragePath) -> List[str]:
        raise Exception


def get_default_storage(home_dir=os.path.expanduser("~")) -> Storage:
    reliquery_dir = os.path.join(home_dir, "reliquery")
    config = settings.get_config(reliquery_dir)
    storage_type = config["storage"]["type"]
    if storage_type == "S3":
        # return S3Storage("de-relic", "v0")
        return S3Storage(**config["storage"]["args"])
    elif storage_type == "File":
        return FileStorage(reliquery_dir)
    elif storage_type == "Demo":
        return S3Storage(**config["storage"]["args"], s3_signed=False)
    else:
        raise ValueError("Config storage type is not supported. Use S3 or File.")
