import sqlite3
from sqlite3 import Error
from sqlite3.dbapi2 import Connection
from typing import Dict, List
import datetime as dt
import logging

dt_format = "%m/%d/%Y %H:%M:%S"


class Data:
    def get_dict(self) -> Dict:
        raise NotImplementedError

    @classmethod
    def parse_dict(self, dict: Dict):
        raise NotImplementedError


class RelicTag(Data):
    def __init__(
        self,
        relic_name: str,
        relic_type: str,
        storage_name: str,
        tags: Dict,
        id: int = None,
        date_created: str = None,
    ) -> None:
        self.id = id
        self.relic_name = relic_name
        self.relic_type = relic_type
        self.storage_name = storage_name
        self.tags = tags
        self.date_created = date_created

    def get_dict(self) -> Dict:
        return {
            "id": self.id,
            "relic_name": self.relic_name,
            "relic_type": self.relic_type,
            "storage_name": self.storage_name,
            "tags": self.tags,
            "date_created": self.date_created,
        }

    @classmethod
    def parse_dict(self, dict: Dict):
        return RelicTag(
            relic_name=dict["relic_name"],
            relic_type=dict["relic_type"],
            storage_name=dict["storage_name"],
            tags=dict["tags"],
            id=dict["id"] if "id" in dict else None,
            date_created=dict["date_created"],
        )

    @classmethod
    def parse_sql_result(self, tag: List):
        return {
            "id": tag[0],
            "relic_name": tag[1],
            "relic_type": tag[2],
            "storage_name": tag[3],
            "tags": {tag[4]: tag[5]},
            "date_created": tag[6],
        }


class Metadata(Data):
    def __init__(
        self,
        name: str,
        data_type: str,
        relic_type: str,
        storage_name: str,
        size: float = None,
        shape: str = None,
        id: int = None,
        last_modified: str = None,
    ) -> None:
        self.id = id
        self.name = name
        self.data_type = data_type
        self.relic_type = relic_type
        self.storage_name = storage_name
        self.size = size
        self.shape = shape
        self.last_modified = (
            last_modified
            if last_modified is not None
            else dt.datetime.utcnow().strftime(dt_format)
        )

    def get_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "data_type": self.data_type,
            "relic_type": self.relic_type,
            "storage_name": self.storage_name,
            "size": self.size,
            "shape": self.shape,
            "last_modified": self.last_modified,
        }

    @classmethod
    def parse_dict(self, dict: Dict):
        metadata = Metadata(
            name=dict["name"],
            data_type=dict["data_type"],
            relic_type=dict["relic_type"],
            storage_name=dict["storage_name"]
            if "storage_name" in dict
            else "None",  # FIXME
            last_modified=dict["last_modified"],
        )

        if "size" in dict:
            metadata.size = dict["size"]

        if "shape" in dict:
            metadata.shape = dict["shape"]

        return metadata

    @classmethod  # FIXME need correct keys and value indexs
    def parse_sql_results(self, data: List):
        return {
            "id": data[0],
            "relic_name": data[1],
            "relic_type": data[2],
            "storage_name": data[3],
            "tags": data[4],
            "date_created": data[5],
        }


class MetadataDB:

    metadata_table = """
    CREATE TABLE IF NOT EXISTS metadata (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        name text NOT NULL,
        data_type text NOT NULL,
        relic_type text NOT NULL,
        storage_name text NOT NULL,
        size real,
        shape text,
        last_modified text NOT NULL
    )
    """

    relic_tag_table = """
    CREATE TABLE IF NOT EXISTS relic_tags (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        relic_name text NOT NULL,
        relic_type text NOT NULL,
        storage_name text NOT NULL,
        key text NOT NULL,
        value text NOT NULL,
        date_created text NOT NULL
    )
    """

    def __init__(self) -> None:
        self.conn = None

        try:
            self.conn = self.connect_db()
        except Error as e:
            logging.warning(f"Error connecting to database: {e}")
        finally:
            if self.conn:
                try:
                    cur = self.conn.cursor()
                    cur.execute(self.metadata_table)
                    cur.execute(self.relic_tag_table)
                    self.conn.commit()
                except Error as e:
                    logging.warning(f"Error creating database tables: {e}")

    def connect_db(self) -> Connection:
        return sqlite3.connect(":memory:")

    def get_db_filename(self) -> str:
        return self.file_loc

    def add_metadata(self, metadata: Metadata) -> None:
        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                SELECT * FROM metadata 
                WHERE name = ?
                AND
                data_type = ?
                AND
                relic_type = ?
                AND
                storage_name = ?
                LIMIT 1
                """,
                (
                    metadata.name,
                    metadata.data_type,
                    metadata.relic_type,
                    metadata.storage_name,
                ),
            )
            rows = cur.fetchall()

            if len(rows) > 0:
                self.update_metadata(metadata)
                return

            cur.execute(
                "INSERT into metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    None,
                    metadata.name,
                    metadata.data_type,
                    metadata.relic_type,
                    metadata.storage_name,
                    metadata.size,
                    metadata.shape,
                    metadata.last_modified,
                ),
            )

            self.conn.commit()

        except Error as e:
            print(e)

    def get_metadata_by_name(
        self, name: str, data_type: str, relic_type: str, storage_name: str
    ) -> Metadata:
        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                SELECT * FROM metadata 
                WHERE name = ?
                AND
                data_type = ?
                AND
                relic_type = ?
                AND
                storage_name = ?
                LIMIT 1
                """,
                (name, data_type, relic_type, storage_name),
            )
            rows = cur.fetchall()

            if len(rows) > 0:
                return Metadata(
                    name=rows[0][1],
                    data_type=rows[0][2],
                    relic_type=rows[0][3],
                    storage_name=rows[0][4],
                    size=rows[0][5],
                    last_modified=rows[0][7],
                    shape=rows[0][6],
                    id=rows[0][0],
                )
            else:
                return None

        except Error as e:
            print(e)

    def get_all_metadata(self) -> List:
        try:
            cur = self.conn.cursor()

            cur.execute("""SELECT * FROM metadata """)
            rows = cur.fetchall()
            print(rows)
            if len(rows) > 0:
                for row in rows:
                    yield Metadata(
                        name=row[1],
                        data_type=row[2],
                        relic_type=row[3],
                        storage_name=row[4],
                        size=row[5],
                        last_modified=row[7],
                        shape=row[6],
                        id=row[0],
                    )
            else:
                return []

        except Error as e:
            print(e)

    def update_metadata(self, metadata: Metadata) -> None:
        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                UPDATE metadata 
                SET name=?, data_type=?, relic_type=?, storage_name=?, size=?, shape=?, last_modified=?
                WHERE id=?
                """,
                (
                    metadata.name,
                    metadata.data_type,
                    metadata.relic_type,
                    metadata.storage_name,
                    metadata.size,
                    metadata.shape,
                    metadata.last_modified,
                    metadata.id,
                ),
            )

            self.conn.commit()

        except Error as e:
            print(e)

    def delete_metadata(self, name: str) -> None:
        raise RuntimeError("method not initialized")

    def query(self, statement: str) -> List:
        results = []

        try:
            cur = self.conn.cursor()

            cur.execute(statement)

            results = cur.fetchall()
        except Error as e:
            logging.warning(f"Error quering database: {e}")

        return results

    def sync_metadata(self, ext: Metadata) -> None:
        int = self.get_metadata_by_name(
            ext.name, ext.data_type, ext.relic_type, ext.storage_name
        )

        if int is not None:
            if dt.datetime.strptime(
                ext.last_modified, dt_format
            ) > dt.datetime.strptime(int.last_modified, dt_format):
                self.update_metadata(ext)
        else:
            self.add_metadata(ext)

    def sync_tags(self, ext: RelicTag) -> None:
        if len(self.get_by_relic_tag(ext)) == 0:
            self.add_relic_tag(
                ext
            )  # FIXME something here is breaking querying reliquery

    def add_relic_tag(self, relic_tag: RelicTag) -> List[RelicTag]:
        try:
            cur = self.conn.cursor()

            for key, value in relic_tag.tags.items():
                cur.execute(
                    """
                    INSERT INTO relic_tags VALUES (?,?,?,?,?,?,?)
                    """,
                    (
                        relic_tag.id,
                        relic_tag.relic_name,
                        relic_tag.relic_type,
                        relic_tag.storage_name,
                        key,
                        value,
                        relic_tag.date_created,
                    ),
                )

            self.conn.commit()
        except Error as e:
            logging.warning(f"Error creating relic tag: {relic_tag.get_dict()} | {e}")

        return self.get_by_relic_tag(relic_tag)

    def remove_relic_tag(self, relic_tag: RelicTag) -> None:
        try:
            cur = self.conn.cursor()

            for key, value in relic_tag.tags:
                cur.execute(
                    """
                    DELETE FROM relic_tags
                    WHERE relic_name=?
                    AND relic_type=?
                    AND storage_type=?
                    AND key=?
                    AND value=?
                    """,
                    (
                        relic_tag.relic_name,
                        relic_tag.relic_type,
                        relic_tag.storage_name,
                        key,
                        value,
                    ),
                )

            self.conn.commit()

        except Error as e:
            logging.warning(f"Error deleting relic tag: {e}")

    def get_by_key_value(self, key: str, value: str) -> List:
        tags = []
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT * FROM relic_tags
                WHERE key=?
                AND value=?
                """,
                (key, value),
            )
            tags.extend(cur.fetchall())

        except Error as e:
            logging.warning(f"Error getting relic tag: {e}")

        return tags

    def get_all_tags_from_relic(
        self, relic_name: str, relic_type: str, storage_name: str
    ) -> List:
        tags = []

        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                SELECT * FROM relic_tags
                WHERE relic_name=?
                AND relic_type=?
                AND storage_name=?
                """,
                (relic_name, relic_type, storage_name),
            )

            queries = cur.fetchall()

            if queries:
                for tag in queries:
                    tags.append(RelicTag.parse_sql_result(tag))

        except Error as e:
            logging.warning(f"Error getting all tags: {e}")

        return tags

    def get_by_relic_tag(self, tag: RelicTag) -> List:
        tags = []
        try:
            cur = self.conn.cursor()

            for key, value in tag.tags.items():
                cur.execute(
                    """
                    SELECT * FROM relic_tags
                    WHERE key=?
                    AND value=?
                    AND relic_name=?
                    AND relic_type=?
                    AND storage_name=?
                    """,
                    (key, value, tag.relic_name, tag.relic_type, tag.storage_name),
                )
                tags.extend(cur.fetchall())

                self.conn.commit()

        except Error as e:
            logging.warning(f"Error getting relic tag: {e}")

        return list(map(RelicTag.parse_sql_result, tags))
