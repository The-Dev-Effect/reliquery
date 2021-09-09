import sqlite3
from sqlite3 import Error
from sqlite3.dbapi2 import Connection
from typing import Dict, List
import datetime as dt


class Metadata:
    dt_format = "%m/%d/%Y %H:%M:%S"

    def __init__(
        self,
        name: str,
        data_type: str,
        relic_type: str,
        storage_type: str,
        size: float = None,
        shape: str = None,
        id: int = None,
        last_modified: str = None,
    ) -> None:
        self.id = id
        self.name = name
        self.data_type = data_type
        self.relic_type = relic_type
        self.storage_type = storage_type
        self.size = size
        self.shape = shape
        self.last_modified = (
            last_modified
            if last_modified is not None
            else dt.datetime.utcnow().strftime(self.dt_format)
        )

    def get_metadata(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "data_type": self.data_type,
            "relic_type": self.relic_type,
            "storage_type": self.storage_type,
            "size": self.size,
            "shape": self.shape,
            "last_modified": self.last_modified,
        }

    @classmethod
    def parse_dict(self, dict: Dict):
        print(dict)
        metadata = Metadata(
            dict["name"],
            dict["data_type"],
            dict["relic_type"],
            dict["storage_type"],
            last_modified=dict["last_modified"],
        )

        if "size" in dict:
            metadata.size = dict["size"]

        if "shape" in dict:
            metadata.shape = dict["shape"]

        return metadata


class MetadataDB:
    dt_format = "%m/%d/%Y %H:%M:%S"

    metadata_table = """
    CREATE TABLE IF NOT EXISTS metadata (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        name text NOT NULL,
        data_type text NOT NULL,
        relic_type text NOT NULL,
        storage_type text NOT NULL,
        size real,
        shape text,
        last_modified text NOT NULL
    )
    """

    def __init__(self) -> None:
        self.conn = None

        try:
            self.conn = self.connect_db()
        except Error as e:
            print(e)
        finally:
            if self.conn:
                try:
                    cur = self.conn.cursor()
                    cur.execute(self.metadata_table)
                    self.conn.commit()
                except Error as e:
                    print(e)

    def connect_db(self) -> Connection:
        conn = None

        try:
            conn = sqlite3.connect(":memory:")
        except Error as e:
            print(e)
        finally:
            if conn:
                return conn
            else:
                raise RuntimeError("Failed to open database")

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
                storage_type = ?
                LIMIT 1
                """,
                (
                    metadata.name,
                    metadata.data_type,
                    metadata.relic_type,
                    metadata.storage_type,
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
                    metadata.storage_type,
                    metadata.size,
                    metadata.shape,
                    metadata.last_modified,
                ),
            )

            self.conn.commit()

        except Error as e:
            print(e)

    def get_metadata_by_name(
        self, name: str, data_type: str, relic_type: str, storage_type: str
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
                storage_type = ?
                LIMIT 1
                """,
                (name, data_type, relic_type, storage_type),
            )
            rows = cur.fetchall()

            if len(rows) > 0:
                return Metadata(
                    name=rows[0][1],
                    data_type=rows[0][2],
                    relic_type=rows[0][3],
                    storage_type=rows[0][4],
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

            if len(rows) > 0:
                for row in rows:
                    yield Metadata(
                        name=row[1],
                        data_type=row[2],
                        relic_type=row[3],
                        storage_type=row[4],
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
                SET name=?, data_type=?, relic_type=?, storage_type=?, size_mb=?, shape=?, last_modified=?
                WHERE id=?
                """,
                (
                    metadata.name,
                    metadata.data_type,
                    metadata.relic_type,
                    metadata.storage_type,
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
            print(e)

        return results

    def sync(self, ext: Metadata) -> Metadata:
        int = self.get_metadata_by_name(
            ext.name, ext.data_type, ext.relic_type, ext.storage_type
        )

        if int is not None:
            if dt.datetime.strptime(
                ext.last_modified, self.dt_format
            ) > dt.datetime.strptime(int.last_modified, self.dt_format):
                self.update_metadata(ext)
        else:
            self.add_metadata(ext)
