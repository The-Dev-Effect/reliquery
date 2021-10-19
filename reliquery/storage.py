import logging
import os
from io import BytesIO, BufferedIOBase
from typing import Any, List, Dict
from shutil import copyfile
import json

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

    def get_metadata(self, path: StoragePath, root_key: str) -> Dict:
        raise NotImplementedError

    def get_all_relic_metadata(self) -> List[Dict]:
        raise NotImplementedError

    def put_tags(self, path: StoragePath, tags: Dict) -> None:
        raise NotImplementedError

    def get_tags(self, path: StoragePath) -> Dict:
        raise NotImplementedError

    def get_all_relic_tags(self) -> List[Dict]:
        raise NotImplementedError

    def get_all_relic_data(self) -> List[Dict]:
        raise NotImplementedError


class FileStorage:
    def __init__(self, root: str, name: str):
        self.root = root
        self.name = name

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

    def put_metadata(self, path: StoragePath, metadata: Dict):
        self._ensure_path(path)

        with open(self._join_path(path), "w") as f:
            return f.write(json.dumps(metadata))

    def get_metadata(self, path: StoragePath, root_key: str) -> Dict:
        data = {
            root_key: {"arrays": [], "text": [], "html": [], "images": [], "json": []}
        }

        dirs = ["arrays", "html", "text", "images", "json"]

        def dict_from_path(path: StoragePath, dirname: str):
            dirpath = path.copy()
            dirpath.append(dirname)
            if os.path.exists(self._join_path(dirpath)):
                entries = os.listdir(self._join_path(dirpath))
                for i in entries:
                    entry_path = dirpath.copy()
                    entry_path.append(i)
                    with open(self._join_path(entry_path), "r") as f:
                        data[root_key][dirname].append(json.loads(f.read()))

        for d in dirs:
            dict_from_path(path, d)

        return data

    # TODO Test coverage needed
    def get_all_relic_metadata(self, encoding: str = "utf-8") -> List[Dict]:
        meta_keys = [
            key for key in self.list_key_paths([""]) if "metadata" in key.split("/")
        ]

        if len(meta_keys) > 0:
            for key in meta_keys:
                with open(key, "r") as f:
                    yield json.loads(f.read())

    def list_key_paths(self, path: StoragePath) -> List[str]:
        paths = []
        joined_path = self._join_path(path)
        subs = os.listdir(joined_path)

        if subs:
            for sub in subs:
                copy = path.copy()
                copy.append(sub)
                sub_path = self._join_path(copy)
                if os.path.isdir(sub_path):
                    paths.extend(self.list_key_paths(copy))
                else:
                    paths.append(sub_path)

        return paths

    def put_tags(self, path: StoragePath, tags: Dict) -> None:
        self._ensure_path(path)

        with open(self._join_path(path), "w") as f:
            return f.write(json.dumps(tags))

    def get_tags(self, path: StoragePath) -> Dict:
        try:
            with open(self._join_path(path), "r") as f:
                return json.loads(f.read())
        except FileNotFoundError:
            return {}

    def get_all_relic_tags(self) -> List[Dict]:
        tag_keys = [
            key for key in self.list_key_paths([""]) if "tags" in key.split("/")
        ]

        for key in tag_keys:
            return self.get_tags(key.split("/")[-3:])

    def get_all_relic_data(self) -> List[Dict]:
        relic_types = os.listdir(self._join_path([""]))

        if relic_types:
            for relic_type in relic_types:
                if os.path.isdir(self._join_path([relic_type])):
                    names = os.listdir(self._join_path([relic_type]))

                    if names:
                        for name in names:
                            yield {
                                "relic_name": name,
                                "relic_type": relic_type,
                                "storage_name": self.name,
                            }
        else:
            return []


S3Client = Any


def _get_s3_client(signed) -> S3Client:
    if signed:
        return boto3.client("s3")
    else:
        return boto3.client("s3", config=Config(signature_version=UNSIGNED))


class S3Storage(Storage):
    def __init__(
        self,
        s3_bucket: str,
        prefix: str,
        name: str,
        s3_signed: bool = True,
    ):
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.name = name if len(name) > 0 else s3_bucket
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

            try:
                keys.extend([process_key(c["Key"]) for c in response["Contents"]])
            except KeyError:
                logging.warning(f"No files found in directory {prefix}")
        # we want to remove the prefx
        return keys

    def put_metadata(self, path: Storage, metadata: Dict):
        self.s3.put_object(
            Key=self._join_path(path), Bucket=self.s3_bucket, Body=json.dumps(metadata)
        )

    def get_metadata(self, path: StoragePath, root_key: str) -> Dict:
        dirs = ["arrays", "html", "text", "images", "json"]

        data = {
            root_key: {"arrays": [], "text": [], "html": [], "images": [], "json": []}
        }

        def dict_from_path(path: StoragePath, dirname: str) -> None:
            dirpath = path.copy()
            dirpath.append(dirname)
            dir_keys = self.list_keys(dirpath.copy())

            if len(dir_keys) > 0:
                for key in dir_keys:
                    key_path = dirpath.copy()
                    key_path.append(key)

                    try:
                        obj = self.s3.get_object(
                            Key=self._join_path(key_path),
                            Bucket=self.s3_bucket,
                        )
                    except self.s3.exceptions.NoSuchKey:
                        raise StorageItemDoesNotExist

                    data[root_key][dirname].append(
                        json.loads(obj["Body"].read().decode("utf-8"))
                    )

        for d in dirs:
            dict_from_path(path, d)

        return data

    def get_all_relic_metadata(self, encoding: str = "utf-8") -> List[Dict]:
        meta_keys = [
            key for key in self.list_key_paths([""]) if "metadata" in key.split("/")
        ]

        if len(meta_keys) > 0:
            for key in meta_keys:
                yield json.loads(
                    self.s3.get_object(
                        Key=self._join_path(key.split("/")),
                        Bucket=self.s3_bucket,
                    )["Body"]
                    .read()
                    .decode(encoding)
                )

    def list_key_paths(self, path: StoragePath) -> List[str]:
        prefix = self._join_path(path)

        def process_key(k):
            return k[len(prefix) :]

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

            try:
                keys.extend([process_key(c["Key"]) for c in response["Contents"]])
            except KeyError:
                logging.warning(f"No files found in directory {prefix}")
        # we want to remove the prefx
        return keys

    def put_tags(self, path: StoragePath, tags: Dict) -> None:
        self.put_text(path, json.dumps(tags))

    def get_tags(self, path: StoragePath) -> Dict:
        try:
            return json.loads(self.get_text(path))
        except StorageItemDoesNotExist:
            return {}

    def get_all_relic_tags(self) -> List[Dict]:
        tag_keys = [
            key for key in self.list_key_paths([""]) if "tags" in key.split("/")
        ]
        tags = []
        for key in tag_keys:
            tags.extend(self.get_tags(key.split("/")))

        return tags

    def get_all_relic_data(self) -> List[Dict]:
        relic_types = {path.split("/")[0] for path in self.list_key_paths([""])}

        for relic_type in relic_types:
            names = {path.split("/")[1] for path in self.list_key_paths([relic_type])}
            for name in names:
                yield {
                    "relic_name": name,
                    "relic_type": relic_type,
                    "storage_name": self.name,
                }


def get_storage_by_name(name: str, root: str = os.path.expanduser("~")) -> Storage:
    reliquery_dir = os.path.join(root, "reliquery")
    config = settings.get_config(reliquery_dir)

    return get_storage(name, reliquery_dir, config[name])


def get_all_available_storages(root: str = os.path.expanduser("~")) -> List[Storage]:
    reliquery_dir = os.path.join(root, "reliquery")
    config = settings.get_config(reliquery_dir)

    storages = []

    for key in config.keys():
        storages.append(get_storage(key, reliquery_dir, config[key]))

    return storages


def get_storage(name: str, root: str, config: Dict) -> Storage:
    if config["storage"]["type"] == "S3":
        return S3Storage(
            **config["storage"]["args"],
            name=name,
        )

    elif config["storage"]["type"] == "File":
        if "root" in config["storage"]["args"]:
            return FileStorage(**config["storage"]["args"], name=name)
        else:
            return FileStorage(**config["storage"]["args"], root=root, name=name)

    else:
        raise ValueError(f"No storage found by name : {name}")
