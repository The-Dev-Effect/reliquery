import sqlite3
from sqlite3 import Error
from sqlite3.dbapi2 import Connection
from typing import Dict, List, Tuple
import datetime as dt


class MetadataDB:

    metadata_table = """
    CREATE TABLE IF NOT EXISTS metadata (
        id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        name text NOT NULL,
        data_type text NOT NULL,
        relic_type text NOT NULL,
        size_mb real NOT NULL,
        shape text,
        last_updated text NOT NULL
    );
    """

    def __init__(self, sqlite_file: str) -> None:
        self.file_loc = sqlite_file
        conn = None

        try:
            conn = self.connect_db()
        except Error as e:
            print(e)
        finally:
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute(self.metadata_table)
                except Error as e:
                    print(e)
                finally:
                    conn.close()

    def connect_db(self) -> Connection:
        conn = None

        try:
            conn = sqlite3.connect(self.file_loc)
        except Error as e:
            print(e)
        finally:
            if conn:
                return conn
            else:
                raise RuntimeError("Failed to open database")

    def get_db_filename(self) -> str:
        return self.file_loc

    def add_metadata(self, metadata: Dict, relic_type: str) -> None:
        conn = None
        try:
            conn = self.connect_db()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT * FROM metadata 
                WHERE name = ?
                AND
                data_type = ?
                AND
                relic_type = ?
                LIMIT 1
                """,
                (metadata["name"], metadata["data_type"], relic_type),
            )
            rows = cur.fetchall()

            if len(rows) > 0:
                self.update_metadata(rows[0][0], metadata, relic_type)
                conn.close()
                return

            name, data_type, size, shape = self.parse_metadata_dict(metadata)
            last_updated = str(dt.datetime.utcnow())

            cur.execute(
                "INSERT into metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
                (None, name, data_type, relic_type, size, shape, last_updated),
            )

            conn.commit()

        except Error as e:
            print(e)
        finally:
            if conn:
                conn.close()

    def get_metadata(self, name: str) -> Dict:
        raise Exception

    def list_metadata(self) -> List:
        raise Exception

    def update_metadata(self, id: int, metadata: Dict, relic_type: str) -> None:

        conn = None
        try:
            conn = self.connect_db()
            cur = conn.cursor()
            name, data_type, size, shape = self.parse_metadata_dict(metadata)
            last_updated = str(dt.datetime.utcnow())

            cur.execute(
                """
                UPDATE metadata 
                SET name=?, data_type=?, relic_type=?, size_mb=?, shape=?, last_updated=?
                WHERE id=?
                """,
                (name, data_type, relic_type, size, shape, last_updated, id),
            )

            conn.commit()

        except Error as e:
            print(e)
        finally:
            if conn:
                conn.close()

    def delete_metadata(self, name: str) -> None:
        pass

    @classmethod
    def parse_metadata_dict(self, dict: Dict) -> Tuple[str, int, str]:
        return dict["name"], dict["data_type"], dict["size"], dict["shape"]
