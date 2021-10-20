import sqlite3
from sqlite3 import Error
from sqlite3.dbapi2 import Connection
from typing import Dict, List, Tuple, Optional
import datetime as dt
import logging


dt_format = "%m/%d/%Y %H:%M:%S"


class Data:
    def get_dict(self) -> Dict:
        raise NotImplementedError

    @classmethod
    def parse_dict(self, dict: Dict):
        raise NotImplementedError

    @classmethod
    def parse_sql_result(self, result: List):
        raise NotImplementedError


class RelicData(Data):
    def __init__(
        self,
        relic_name: str,
        relic_type: str,
        storage_name: str,
        id: int = None,
        last_modified: str = None,
    ) -> None:
        self.relic_name = relic_name
        self.relic_type = relic_type
        self.storage_name = storage_name
        self.id = id
        self.last_modified = last_modified

    def get_dict(self) -> Dict:
        return {
            "id": self.id,
            "relic_name": self.relic_name,
            "relic_type": self.relic_type,
            "storage_name": self.storage_name,
            "last_modified": self.last_modified,
        }

    @classmethod
    def parse_dict(self, dict: Dict) -> Data:
        return RelicData(
            relic_name=dict["relic_name"],
            relic_type=dict["relic_type"],
            storage_name=dict["storage_name"],
            id=dict["id"] if "id" in dict else None,
            last_modified=dict["last_modified"] if "last_modified" in dict else None,
        )

    @classmethod
    def parse_sql_result(self, result: List):
        return {
            "id": result[0],
            "relic_name": result[1],
            "relic_type": result[2],
            "storage_name": result[3],
            "last_modified": result[4],
        }

    @classmethod
    def relic_data(self, relic_name: str, relic_type: str, storage_name: str):
        return RelicData(relic_name, relic_type, storage_name)


class RelicTag(Data):
    def __init__(
        self,
        relic: RelicData,
        tags: Dict,
        id: int = None,
        date_created: str = None,
    ) -> None:
        self.id = id
        self.relic = relic
        self.tags = tags
        self.date_created = date_created

    def get_dict(self) -> Dict:
        return {
            "id": self.id,
            "relic_name": self.relic.relic_name,
            "relic_type": self.relic.relic_type,
            "storage_name": self.relic.storage_name,
            "tags": self.tags,
        }

    @classmethod
    def parse_dict(self, tags: Dict, relic: RelicData) -> Data:
        return RelicTag(relic=relic, tags=tags)

    @classmethod
    def parse_sql_result(self, result: List, relic: RelicData):
        return {
            "id": result[0],
            "relic_name": relic.relic_name,
            "relic_type": relic.relic_type,
            "storage_name": relic.storage_name,
            "tags": {result[2]: result[3]},
        }


class Metadata(Data):
    def __init__(
        self,
        name: str,
        data_type: str,
        relic: RelicData,
        size: float = None,
        shape: str = None,
        id: int = None,
        last_modified: str = None,
    ) -> None:
        self.id = id
        self.name = name
        self.data_type = data_type
        self.relic = relic
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
            "relic_name": self.relic.relic_name,
            "relic_type": self.relic.relic_type,
            "storage_name": self.relic.storage_name,
            "size": self.size,
            "shape": self.shape,
            "last_modified": self.last_modified,
        }

    @classmethod
    def parse_dict(self, dict: Dict, relic: RelicData) -> Data:
        metadata = Metadata(
            name=dict["name"],
            data_type=dict["data_type"],
            relic=relic,
            last_modified=dict["last_modified"],
        )

        if "size" in dict:
            metadata.size = dict["size"]

        if "shape" in dict:
            metadata.shape = dict["shape"]

        return metadata

    @classmethod
    def parse_sql_result(self, result: List, relic: RelicData) -> Dict:
        return {
            "id": result[0],
            "name": result[1],
            "data_type": result[2],
            "relic_name": relic.relic_name,
            "relic_type": relic.relic_type,
            "storage_name": relic.storage_name,
            "size": result[4],
            "shape": result[5],
            "last_modified": result[6],
        }


class MetadataDB:

    metadata_table = """
    CREATE TABLE IF NOT EXISTS metadata (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        name text NOT NULL,
        data_type text NOT NULL,
        relic_id integer NOT NULL,
        size real,
        shape text,
        last_modified text NOT NULL
    )
    """

    relic_tag_table = """
    CREATE TABLE IF NOT EXISTS relic_tags (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        relic_id integer NOT NULL,
        key text NOT NULL,
        value text NOT NULL
    )
    """

    relic_table = """
    CREATE TABLE IF NOT EXISTS relics (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        relic_name text NOT NULL,
        relic_type text NOT NULL,
        storage_name text NOT NULL,
        last_modified text NOT NULL
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
                    cur.execute(self.relic_table)
                    self.conn.commit()
                except Error as e:
                    logging.warning(f"Error creating database tables: {e}")

    def connect_db(self) -> Connection:
        return sqlite3.connect(":memory:")

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
                relic_id = ?
                LIMIT 1
                """,
                (metadata.name, metadata.data_type, metadata.relic.id),
            )
            rows = cur.fetchall()

            if len(rows) > 0:
                self.update_metadata(metadata)
                return

            cur.execute(
                "INSERT into metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    None,
                    metadata.name,
                    metadata.data_type,
                    metadata.relic.id,
                    metadata.size,
                    metadata.shape,
                    metadata.last_modified,
                ),
            )

            self.conn.commit()

        except Error as e:
            logging.warning(f"Error adding metadata: {metadata} | {e.__class__}: {e}")

    # TODO add test coverage
    def get_metadata_by_name(self, name: str, data_type: str, relic: RelicData):

        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                SELECT * FROM metadata
                WHERE name = ?
                AND
                data_type = ?
                AND
                relic_id = ?
                LIMIT 1
                """,
                (name, data_type, relic.id),
            )
            rows = cur.fetchall()

            if rows:
                return Metadata(
                    name=rows[0][1],
                    data_type=rows[0][2],
                    relic=relic,
                    size=rows[0][4],
                    last_modified=rows[0][6],
                    shape=rows[0][5],
                    id=rows[0][0],
                )
            else:
                return None

        except Error as e:
            logging.warning(
                "Error getting metadata by name: "
                + f"name={name}, type={data_type} | {e.__class__()}: {e}"
            )

    def get_all_metadata(self) -> List:
        try:
            cur = self.conn.cursor()

            cur.execute("""SELECT * FROM metadata """)
            rows = cur.fetchall()

            if len(rows) > 0:
                for row in rows:
                    relic = self.get_relic_data_by_id(row[3])
                    yield Metadata(
                        name=row[1],
                        data_type=row[2],
                        relic=relic,
                        size=row[4],
                        last_modified=row[6],
                        shape=row[5],
                        id=row[0],
                    )
            else:
                return []

        except Error as e:
            logging.warning("Error getting all metadata | " + f"{e.__class__()}: {e}")

    def update_metadata(self, metadata: Metadata) -> None:
        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                UPDATE metadata
                SET name=?, data_type=?, relic_id=?, size=?, shape=?, last_modified=?
                WHERE id=?
                """,
                (
                    metadata.name,
                    metadata.data_type,
                    metadata.relic.id,
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
        int = self.get_metadata_by_name(ext.name, ext.data_type, ext.relic)

        if int is not None:
            if dt.datetime.strptime(
                ext.last_modified, dt_format
            ) > dt.datetime.strptime(int.last_modified, dt_format):
                self.update_metadata(ext)
        else:
            self.add_metadata(ext)

    def sync_tags(self, ext: RelicTag) -> None:
        if len(self.get_by_relic_tag(ext)) == 0:
            self.add_relic_tag(ext)

    def add_relic_tag(self, relic_tag: RelicTag) -> List[RelicTag]:

        cur = self.conn.cursor()
        if "tags" in relic_tag.tags:
            relic_tag.tags = relic_tag.tags["tags"]
        for key, value in relic_tag.tags.items():
            cur.execute(
                """
                INSERT INTO relic_tags VALUES (?,?,?,?)
                """,
                (
                    None,
                    relic_tag.relic.id,
                    key,
                    value,
                ),
            )

        self.conn.commit()

        return self.get_by_relic_tag(relic_tag)

    def remove_relic_tag(self, relic_tag: RelicTag) -> None:
        try:
            cur = self.conn.cursor()

            for key, value in relic_tag.tags:
                cur.execute(
                    """
                    DELETE FROM relic_tags
                    WHERE relic_id = ?
                    AND key=?
                    AND value=?
                    """,
                    (
                        relic_tag.relic.id,
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

    def get_all_tags_from_relic(self, relic: RelicData) -> List:
        tags = []

        try:
            cur = self.conn.cursor()

            cur.execute(
                """
                SELECT * FROM relic_tags
                WHERE relic_id = ?
                """,
                (str(relic.id)),
            )

            queries = cur.fetchall()

            if queries:
                for tag in queries:
                    tags.append(RelicTag.parse_sql_result(tag, relic))

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
                    AND relic_id = ?
                    """,
                    (key, value, tag.relic.id),
                )
                tags.extend(cur.fetchall())

                self.conn.commit()

        except Error as e:
            logging.warning(f"Error getting relic tag: {e}")

        return [RelicTag.parse_sql_result(i, tag.relic) for i in tags]

    def sync_relic_data(self, relic_data: RelicData) -> RelicData:
        relic = self.get_relic_data_by_name(
            relic_data.relic_name, relic_data.relic_type, relic_data.storage_name
        )

        if relic:
            return relic
        else:
            self._create_relic_data(
                relic_data.relic_name, relic_data.relic_type, relic_data.storage_name
            )
            return self.get_relic_data_by_name(
                relic_data.relic_name, relic_data.relic_type, relic_data.storage_name
            )

    def get_relic_data_by_name(
        self, relic_name: str, relic_type: str, storage_name: str
    ) -> RelicData:

        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT * FROM relics
                WHERE relic_name = ?
                AND relic_type = ?
                AND storage_name =?
                LIMIT 1
                """,
                (relic_name, relic_type, storage_name),
            )
            rows = cur.fetchall()
            if rows:
                return RelicData.parse_dict(RelicData.parse_sql_result(rows[0]))
            else:
                return None

        except Error as e:
            logging.warning(
                "Error getting Relic data by name: "
                + f"{relic_name, relic_type, storage_name} | {e.__class__()}: {e}"
            )

    def _create_relic_data(
        self, relic_name: str, relic_type: str, storage_name: str
    ) -> None:
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
            INSERT INTO relics VALUES(?,?,?,?,?)
            """,
                (
                    None,
                    relic_name,
                    relic_type,
                    storage_name,
                    dt.datetime.utcnow().strftime(dt_format),
                ),
            )

            self.conn.commit()

        except Error as e:
            logging.warning(f"Error creating Relic data: {e}")

    def get_relic_data_by_id(self, relic_id: int) -> Optional[RelicData]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT * FROM relics WHERE id=? LIMIT 1;
            """,
            (relic_id,),
        )

        rows = cur.fetchall()

        if rows:
            return RelicData.parse_dict(RelicData.parse_sql_result(rows[0]))

    def get_relics_by_tag(self, key: str, value: str) -> List[Tuple]:
        # TODO add ability to query using multiple tags

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT rl.relic_name, rl.relic_type, rl.storage_name
            FROM relic_tags rt
            JOIN relics rl ON rl.id=rt.relic_id
            WHERE key=? AND value=?;
            """,
            (key, value),
        )

        return cur.fetchall()
