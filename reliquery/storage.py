import logging
import os
import io
from io import BytesIO, BufferedIOBase
from typing import Any, List, Dict
from shutil import copyfile
import json

import boto3
from botocore import UNSIGNED
from botocore.client import Config


from . import settings

import dropbox
from dropbox.exceptions import ApiError

from google.oauth2 import service_account
from googleapiclient.discovery import build
from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseUpload
from google.cloud import storage
from google.cloud.exceptions import NotFound

StoragePath = List[str]

DATA_TYPES = [
    "arrays",
    "html",
    "text",
    "images",
    "json",
    "pandasdf",
    "files",
    "notebooks",
]


class StorageItemDoesNotExist(Exception):
    pass

class BucketDoesNotExist(Exception):
    pass


class Storage:
    def put_file(self, path: StoragePath, file_path: str) -> None:
        raise NotImplementedError

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO):
        raise NotImplementedError

    def get_binary_obj(self, path: StoragePath) -> BytesIO:
        raise NotImplementedError

    def put_text(self, path: StoragePath, text: str) -> None:
        raise NotImplementedError

    def get_text(self, path: StoragePath) -> str:
        raise NotImplementedError

    def list_keys(self, path: StoragePath) -> List[str]:
        raise NotImplementedError

    def put_metadata(self, path: StoragePath, metadata: Dict):
        raise NotImplementedError

    def get_metadata(self, path: StoragePath, root_key: str) -> Dict:
        raise NotImplementedError

    def put_tags(self, path: StoragePath, tags: Dict) -> None:
        raise NotImplementedError

    def get_tags(self, path: StoragePath) -> Dict:
        raise NotImplementedError

    def get_all_relic_tags(self) -> List[Dict]:
        raise NotImplementedError

    def get_all_relic_data(self) -> List[Dict]:
        raise NotImplementedError


class FileStorage(Storage):
    def __init__(self, root: str, name: str):
        self.root = os.path.expanduser(root)
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
        data = {root_key: {k: [] for k in DATA_TYPES}}

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

        for d in data[root_key]:
            dict_from_path(path, d)

        return data

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
        relic_data = []
        for relic_type in relic_types:
            if os.path.isdir(self._join_path([relic_type])):
                names = os.listdir(self._join_path([relic_type]))

                if names:
                    for name in names:
                        relic_data.append(
                            {
                                "relic_name": name,
                                "relic_type": relic_type,
                                "storage_name": self.name,
                            }
                        )
        return relic_data


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
        data = {root_key: {k: [] for k in DATA_TYPES}}

        def dict_from_path(path: StoragePath, dirname: str) -> None:
            dirpath = path.copy()
            dirpath.append(dirname)
            dir_keys = self.list_keys(dirpath.copy())

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

        for d in data[root_key]:
            dict_from_path(path, d)

        return data

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
        relic_data = []

        for relic_type in relic_types:
            names = {path.split("/")[1] for path in self.list_key_paths([relic_type])}
            if names:
                for name in names:
                    relic_data.append(
                        {
                            "relic_name": name,
                            "relic_type": relic_type,
                            "storage_name": self.name,
                        }
                    )
            return relic_data


class DropboxStorage(Storage):
    def __init__(
        self,
        access_token: str,
        prefix: str,
        name: str,
    ):
        self.dbx = dropbox.Dropbox(access_token)
        self.prefix = "/" + prefix
        self.name = name

    def _join_path(self, path: StoragePath) -> str:
        return "/".join([self.prefix] + path)

    def put_file(self, path: StoragePath, file_path: str) -> None:
        path = path.split()
        with open(file_path, "rb") as f:
            self.dbx.files_upload(f.read(), "/" + self._join_path(path), mute=True)

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO) -> None:
        self.dbx.files_upload(buffer.read(), self._join_path(path), mute=True)

    def get_binary_obj(self, path: StoragePath) -> BytesIO:
        return BytesIO(
            self.dbx.files_download(self._join_path(path), rev=None)[-1].content
        )

    def put_text(self, path: StoragePath, text: str, encoding: str = "utf-8") -> None:
        string_bytes = bytes(text, encoding)
        self.dbx.files_upload(string_bytes, self._join_path(path))

    def get_text(self, path: StoragePath) -> str:
        try:
            return self.dbx.files_download(self._join_path(path))[-1].text
        except ApiError:
            raise StorageItemDoesNotExist

    def list_keys(self, path: StoragePath) -> List[str]:
        try:
            return [
                i.name
                for i in self.dbx.files_list_folder(self._join_path(path)).entries
            ]
        except ApiError:
            return []

    def put_metadata(self, path: StoragePath, metadata: Dict):
        metadata_bytes = json.dumps(metadata).encode("utf-8")
        self.dbx.files_upload(
            metadata_bytes,
            self._join_path(path),
            mode=dropbox.files.WriteMode.overwrite,
        )

    def get_metadata(self, path: StoragePath, root_key: str) -> Dict:
        data = {root_key: {k: [] for k in DATA_TYPES}}

        def dict_from_path(path: StoragePath, dirname: str):
            dirpath = path.copy()
            dirpath.append(dirname)
            entries = self.list_keys(dirpath)

            for i in entries:
                entry_path = dirpath.copy()
                entry_path.append(i)
                data[root_key][dirname].append(
                    self.dbx.files_download(self._join_path(entry_path))[-1].json()
                )

        for d in data[root_key]:
            dict_from_path(path, d)

        return data

    def list_key_paths(self, path: StoragePath) -> List[str]:
        paths = []
        subs = self.list_keys(path)
        if subs:
            for sub in subs:
                copy = path.copy()
                copy.append(sub)
                sub_path = self._join_path(copy)
                # isinstace was found as the best way to check if a directory
                # is a file or folder
                if isinstance(
                    self.dbx.files_get_metadata(sub_path), dropbox.files.FolderMetadata
                ):
                    paths.extend(self.list_key_paths(copy))
                elif isinstance(
                    self.dbx.files_get_metadata(sub_path), dropbox.files.FileMetadata
                ):
                    paths.append(sub_path)
        return paths

    def put_tags(self, path: StoragePath, tags: Dict) -> None:
        self.put_text(path, json.dumps(tags))

    def get_tags(self, path: StoragePath) -> Dict:
        try:
            return json.loads(self.get_text(path))
        except StorageItemDoesNotExist:
            return {}

    def get_all_relic_tags(self) -> List[Dict]:
        tag_keys = [key for key in self.list_key_paths([]) if "tags" in key.split("/")]

        for key in tag_keys:
            return self.get_tags(key.split("/")[-3:])

    def get_all_relic_data(self) -> List[Dict]:
        relic_types = {path.split("/")[2] for path in self.list_key_paths([])}
        relic_data = []

        for relic_type in relic_types:
            names = {path.split("/")[3] for path in self.list_key_paths([relic_type])}
            if names:
                for name in names:
                    relic_data.append(
                        {
                            "relic_name": name,
                            "relic_type": relic_type,
                            "storage_name": self.name,
                        }
                    )
            return relic_data


class GoogleDriveStorage(Storage):
    def __init__(
        self,
        prefix: str,
        name: str,
        token_file: str,
        SCOPES: list,
        shared_folder_id: str,
    ):
        self.prefix = prefix
        self.name = name
        self.token_file = token_file
        self.SCOPES = SCOPES
        self.shared_folder_id = shared_folder_id

        creds = None
        if os.path.exists(token_file):
            creds = service_account.Credentials.from_service_account_file(
                token_file, scopes=SCOPES
            )
        try:
            self.service = build("drive", "v3", credentials=creds)
        except IOError:
            raise StorageItemDoesNotExist

        # Check for prefix folder
        results = (
            self.service.files()
            .list(
                q="parents in '{}'".format(self.shared_folder_id),
                pageSize=100,
                fields="nextPageToken, files(id, name)",
            )
            .execute()
        )
        items = results.get("files", [])

        self.root_id = ""
        prefix_found = False
        for item in items:
            if item["name"] == prefix:
                self.root_id = item["id"]
                prefix_found = True
                break

        if prefix_found is False:
            ids = self._create_path(self.shared_folder_id, [prefix])
            self.root_id = ids[-1]

    def _join_path(self, path: StoragePath) -> str:
        return "/".join([self.prefix] + path)

    def _create_folder(self, path_name, parent_id):
        # Create folder metadata
        folder1_metadata = {
            "name": path_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": "{}".format([parent_id]),
        }
        # Create the folder with the previous metadata
        folder1 = (
            self.service.files().create(body=folder1_metadata, fields="id").execute()
        )
        # Get folder id for the folder just created
        folder1_id = "{}".format(folder1.get("id"))

        # Retrieve existing parents of the folder
        folder = (
            self.service.files()
            .get(fileId="{}".format(folder1_id), fields="parents")
            .execute()
        )
        previous_parents = ",".join(folder.get("parents"))

        # Remove the previous parents and replace with the parent we want
        folder = (
            self.service.files()
            .update(
                fileId=folder1_id,
                addParents=parent_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )

        return folder

    def _list_items_in_folder(self, folder_id):
        # List files in parent folder
        results = (
            self.service.files()
            .list(
                q="parents in '{}'".format(folder_id),
                pageSize=100,
                fields="nextPageToken, files(id, name)",
            )
            .execute()
        )
        items = results.get("files", [])
        return items

    def _find_id_in_folder(self, folder_id, fileName):
        items = self._list_items_in_folder(folder_id)
        for item in items:
            i = item.get("name")
            if i == fileName:
                return item.get("id")
        raise StorageItemDoesNotExist

    def _find_deepest_folder_id(self, root, path):
        parents = [root]

        for p in path:
            # List files in curr_root
            results = (
                self.service.files()
                .list(
                    q="parents in '{}'".format(parents[-1]),
                    pageSize=100,
                    fields="nextPageToken, files(id, name)",
                )
                .execute()
            )
            items = results.get("files", [])
            if len(items) > 0:
                for item in items:
                    if item["name"] == p:
                        parents.append(item["id"])
            else:
                return ""

        return parents[-1]

    def _create_path(self, curr_root, path):
        parents = [curr_root]

        for p in path:
            # List files in curr_root
            results = (
                self.service.files()
                .list(
                    q="parents in '{}'".format(curr_root),
                    pageSize=100,
                    fields="nextPageToken, files(id, name)",
                )
                .execute()
            )
            items = results.get("files", [])

            # Check if results were found
            if len(items) > 0:
                # Check if this path name already exists
                # and change curr_root to it if so
                p_found = False
                for item in items:
                    if item["name"] == p:
                        parents.append(item["id"])
                        curr_root = item["id"]
                        p_found = True

                # If all items were checked and the path name did not exist
                # -> Create the path as a folder
                if p_found is False:
                    folder = self._create_folder(p, parents[-1])
                    curr_root = folder.get("id")
                    parents.append(folder.get("id"))

            # When no results are found/items list is empty
            else:
                folder = self._create_folder(p, parents[-1])
                curr_root = folder.get("id")
                parents.append(folder.get("id"))

        return parents

    def _check_file_exists(self, folder_id, file_name):
        items = self._list_items_in_folder(folder_id)
        for item in items:
            i = item.get("name")
            if i == file_name:
                return item.get("id")
        return None

    def _update_binary_file(self, file_id, content):
        # Retrive the tags file from the API
        file = self.service.files().get(fileId=file_id).execute()

        # Files new content
        media_body = MediaIoBaseUpload(content, mimetype="application/octet-stream")

        # Delete not writable items
        del file["kind"]
        del file["id"]

        # Send request to API
        (
            self.service.files()
            .update(fileId=file_id, body=file, media_body=media_body)
            .execute()
        )

    def _update_file(self, file_id, file_path):
        # Retrive the tags file from the API
        file = self.service.files().get(fileId=file_id).execute()

        # Files new content
        media_body = MediaFileUpload(file_path)
        # Delete not writable items
        del file["kind"]
        del file["id"]

        # Send request to API
        (
            self.service.files()
            .update(fileId=file_id, body=file, media_body=media_body)
            .execute()
        )

    def _create_binary_file(self, file_name, parent_id, buffer):
        file_metadata = {"name": file_name, "parents": parent_id}
        media = MediaIoBaseUpload(buffer, mimetype="application/octet-stream")
        file = (
            self.service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )

        # Get file id for the file just created
        file_id = "{}".format(file.get("id"))

        # Retrieve existing parents of the folder
        file = (
            self.service.files()
            .get(fileId="{}".format(file_id), fields="parents")
            .execute()
        )
        previous_parents = ",".join(file.get("parents"))

        # Remove the previous parents and replace with the parent we want
        file = (
            self.service.files()
            .update(
                fileId=file_id,
                addParents=parent_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )

    def _create_file(self, file_name, parent_id, file_path):
        file_metadata = {"name": file_name, "parents": parent_id}
        media = MediaFileUpload(file_path)
        file = (
            self.service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )

        # Get file id for the file just created
        file_id = "{}".format(file.get("id"))

        # Retrieve existing parents of the folder
        file = (
            self.service.files()
            .get(fileId="{}".format(file_id), fields="parents")
            .execute()
        )
        previous_parents = ",".join(file.get("parents"))

        # Remove the previous parents and replace with the parent we want
        file = (
            self.service.files()
            .update(
                fileId=file_id,
                addParents=parent_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )

    def put_file(self, path: StoragePath, file_path: str) -> None:
        curr_root = self.root_id
        parents = self._create_path(curr_root, path[:-1])
        file_id = self._check_file_exists(parents[-1], path[-1])
        if file_id is None:
            self._create_file(path[-1], parents[-1], file_path)
        else:
            self._update_file(file_id, file_path)

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO):
        curr_root = self.root_id
        parents = self._create_path(curr_root, path[:-1])
        file_id = self._check_file_exists(parents[-1], path[-1])
        if file_id is None:
            self._create_binary_file(path[-1], parents[-1], buffer)
        else:
            self._update_binary_file(file_id, buffer)

    def get_binary_obj(self, path: StoragePath) -> BytesIO:
        parents = self._create_path(self.root_id, path[:-1])
        file_id = self._find_id_in_folder(parents[-1], path[-1])

        request = self.service.files().get_media(fileId=file_id).execute()
        buffer = io.BytesIO(request)
        return buffer

    def put_text(self, path: StoragePath, text: str, encoding="utf-8") -> None:
        # Implementation with uploading BytesIO
        parents = self._create_path(self.root_id, path[:-1])
        file_id = self._check_file_exists(parents[-1], path[-1])
        content = io.BytesIO(bytes(text, encoding))
        if file_id is None:
            self._create_binary_file(path[-1], parents[-1], content)
        else:
            self._update_binary_file(file_id, content)

    def get_text(self, path: StoragePath, encoding: str = "utf-8") -> str:
        folder_id = self._find_deepest_folder_id(self.root_id, path[:-1])
        file_id = self._find_id_in_folder(folder_id, path[-1])

        if file_id != "":
            return (
                self.service.files()
                .get_media(fileId=file_id)
                .execute()
                .decode(encoding)
            )

    def list_keys(self, path: StoragePath) -> List[str]:
        # Go to the deepest part of the path and save the id of that folder
        curr_root = self.root_id
        parents = self._create_path(curr_root, path)
        key_list = []
        # List all the files in that folder
        items = self._list_items_in_folder(parents[-1])
        # Return a list of all the file names in that folder
        for item in items:
            key_list.append(item.get("name"))
        return key_list

    def list_key_paths(self, path: StoragePath) -> List[str]:
        paths = []
        subs = self.list_keys(path)
        if subs:
            for sub in subs:
                copy = path.copy()
                copy.append(sub)
                sub_path = self._join_path(copy)
                # is a file or folder
                parent = self._find_deepest_folder_id(self.root_id, sub_path[:-1])
                items = self._list_items_in_folder(parent)

                for item in items:
                    if item.get("name") == sub_path[-1]:
                        file_id = item.get("id")
                        type = (
                            self.service.files()
                            .get(fileId=file_id, fields="mimeType")
                            .execute()
                        )

                        if type["mimeType"] == "application/vnd.google-apps.folder":
                            paths.extend(self.list_key_paths(copy))
                    else:
                        paths.append(sub_path)
        return paths

    def put_metadata(self, path: StoragePath, metadata: Dict):
        # get the folder id of the relic
        folder_id_list = self._create_path(self.root_id, path[:-3])

        path_list = path[2:]
        parents = self._create_path(folder_id_list[-1], path_list[:-1])

        metadata_bytes = json.dumps(metadata).encode("utf-8")
        buffer = io.BytesIO(metadata_bytes)

        file_id = self._check_file_exists(parents[-1], path[-1])
        if file_id is None:
            self._create_binary_file(path[-1], parents[-1], buffer)
        else:
            self._update_binary_file(file_id, buffer)

    def get_metadata(
        self, path: StoragePath, root_key: str, encoding: str = "utf-8"
    ) -> Dict:
        data = {root_key: {k: [] for k in DATA_TYPES}}

        def dict_from_path(path: StoragePath, dirname: str):
            dirpath = path.copy()
            dirpath.append(dirname)
            entries = self.list_keys(dirpath)

            for i in entries:
                entry_path = dirpath.copy()
                entry_path.append(i)

                file_id = ""
                curr_root = self.root_id
                parents = self._create_path(curr_root, entry_path[:-1])
                try:
                    file_id = self._find_id_in_folder(parents[-1], entry_path[-1])
                except StorageItemDoesNotExist:
                    return []

                data[root_key][dirname].append(
                    json.loads(
                        self.service.files()
                        .get_media(fileId=file_id)
                        .execute()
                        .decode(encoding)
                    )
                )

        for d in data[root_key]:
            dict_from_path(path, d)

        return data

    def put_tags(self, path: StoragePath, tags: Dict, encoding="utf-8") -> None:
        self._create_path(self.root_id, path[:-1])
        self.put_text(path, json.dumps(tags))

    def get_tags(self, path: StoragePath) -> Dict:
        try:
            tags = self.get_text(path)
        except StorageItemDoesNotExist:
            return {}

        return json.loads(tags)

    def get_all_relic_data(self) -> List[Dict]:
        relic_types = [path.split("/")[1] for path in self.list_key_paths([])]
        relic_data = []

        for relic_type in relic_types:
            names = {path.split("/")[2] for path in self.list_key_paths([relic_type])}
            if names:
                for name in names:
                    relic_data.append(
                        {
                            "relic_name": name,
                            "relic_type": relic_type,
                            "storage_name": self.name,
                        }
                    )
        return relic_data


class GoogleCloudStorage(Storage):
    def __init__(
        self,
        prefix: str,
        name: str,
        token_file: str,
        root: str
    ):
        self.prefix = prefix
        self.name = name
        self.token_file = token_file
        self.root = root

        self.storage_client = storage.Client.from_service_account_json(self.token_file)
        # Check for root bucket
        buckets_list = list(self.storage_client.list_buckets())
        root_bucket = False
        for bucket in buckets_list:
            if bucket.id == root:
                self.bucket_id = bucket.id
                root_bucket = True
        if root_bucket == False:
            raise BucketDoesNotExist

    def _join_path(self, path: StoragePath) -> str:
        return "/".join([self.prefix] + path)

    def put_file(self, path: StoragePath, file_path: str) -> None:
        path = self._join_path(path)
        bucket = self.storage_client.bucket(self.bucket_id)
        blob = bucket.blob(path)
        blob.upload_from_filename(file_path)

    def put_binary_obj(self, path: StoragePath, buffer: BytesIO):
        path = self._join_path(path)

        buffer_bytes = buffer.read()

        bucket = self.storage_client.bucket(self.bucket_id)
        blob = bucket.blob(path)
        blob.upload_from_string(buffer_bytes)

    def get_binary_obj(self, path: StoragePath) -> BytesIO:
        path = self._join_path(path)
        bucket = self.storage_client.bucket(self.bucket_id)
        blob = bucket.blob(path)
        bytes = blob.download_as_string()
        bytesio = BytesIO(bytes)
        return bytesio

    def put_text(self, path: StoragePath, text: str) -> None:
        path = self._join_path(path)

        bucket = self.storage_client.bucket(self.bucket_id)
        blob = bucket.blob(path)
        blob.upload_from_string(text)

    def get_text(self, path: StoragePath) -> str:
        path = self._join_path(path)
        bucket = self.storage_client.bucket(self.bucket_id)
        blob = bucket.blob(path)
        try:
            return blob.download_as_text()
        except NotFound:
            raise StorageItemDoesNotExist

    def list_keys(self, path: StoragePath) -> List[str]:
        key_list = []
        bucket = self.storage_client.get_bucket(self.bucket_id)
        # List all the files in that folder with the given prefix
        items = self.storage_client.list_blobs(bucket, prefix=self._join_path(path))
        # Return a list of all the file names in that folder
        for item in items:
            key_list.append(item.id.split("/")[-2])
        return key_list

    def list_key_paths(self, path: StoragePath) -> List[str]:
        paths = []
        subs = self.list_keys(path)
        if subs:
            for sub in subs:
                copy = path.copy()
                copy.append(sub)
                sub_path = self._join_path(copy)

                # is a file or folder
                if str(sub_path)[-1] == "/":
                    paths.extend(self.list_key_paths(copy))
                else:
                    paths.append(sub_path)
        return paths

    def put_metadata(self, path: StoragePath, metadata: Dict):
        metadata_bytes = json.dumps(metadata).encode("utf-8")
        path = self._join_path(path)

        bucket = self.storage_client.bucket(self.bucket_id)
        blob = bucket.blob(path)
        blob.upload_from_string(metadata_bytes)

    def get_metadata(self, path: StoragePath, root_key: str) -> Dict:
        data = {root_key: {k: [] for k in DATA_TYPES}}

        def dict_from_path(path: StoragePath, dirname: str):
            dirpath = path.copy()
            dirpath.append(dirname)
            entries = self.list_keys(dirpath)

            for i in entries:
                entry_path = dirpath.copy()
                entry_path.append(i)
                bucket = self.storage_client.bucket(self.bucket_id)
                blob = bucket.blob(self._join_path(entry_path))
                data[root_key][dirname].append(json.loads(blob.download_as_text()))

        for d in data[root_key]:
            dict_from_path(path, d)

        return data

    def put_tags(self, path: StoragePath, tags: Dict) -> None:
        self.put_text(path, json.dumps(tags))

    def get_tags(self, path: StoragePath) -> Dict:
        try:
            tags = self.get_text(path)
        except StorageItemDoesNotExist:
            return {}

        return json.loads(tags)

    def get_all_relic_data(self) -> List[Dict]:
        bucket = self.storage_client.get_bucket(self.bucket_id)
        relic_types = {
            path.id.split("/")[1] for path in self.storage_client.list_blobs(bucket)
        }
        relic_data = []

        for relic_type in relic_types:
            prefix = self._join_path([self.prefix, relic_type])
            names = {path.id for path in self.storage_client.list_blobs(bucket)}
            for name in names:
                if prefix in name:
                    relic_data.append(
                        {
                            "relic_name": name.split("/")[2],
                            "relic_type": relic_type,
                            "storage_name": self.name,
                        }
                    )
        return relic_data


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

    elif config["storage"]["type"] == "Dropbox":
        return DropboxStorage(**config["storage"]["args"], name=name)

    elif config["storage"]["type"] == "GoogleDrive":
        return GoogleDriveStorage(**config["storage"]["args"], name=name)

    elif config["storage"]["type"] == "GoogleCloud":
        return GoogleCloudStorage(**config["storage"]["args"], name=name)

    else:
        raise ValueError(f"No storage found by name : {name}")
