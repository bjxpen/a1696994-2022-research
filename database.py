"""Database API for this project

"""

from typing import Iterable, Tuple
from contextlib import contextmanager

import sqlite3

from config import (
    DB_PATH, DB_TABLE_FETCH_SIZE,
    REPO_RAW_TABLE_NAME,
    REPO_FILTERED_TABLE_NAME,
    REPO_FIRST_SAMPLE_TABLE_NAME,
    REPO_SAMPLE_BLACKLIST_TABLE_NAME
)
from schema_types import REPO_TABLE_SCHEMA

ASC = "ASC"
DESC = "DESC"

class DatabaseUtil:
    """Some utility methods to preprocess before queries.

    """
    @classmethod
    def format_schema(cls, schema: str) -> str:
        """Formats any schema from config.py to become compatible with CREATE TABLE statement.

        """
        lines = schema.strip().split("\n")
        for i in range(len(lines) - 1):
            line = lines[i].strip()
            if line[-1] != ",":
                lines[i] = line + ","

        return "".join(lines)

    @classmethod
    def table_schema_to_fields(cls, schema):
        """Extracts fields from any schema specified in config.py.

        """
        fields = [line.strip().split()[0] for line in schema.strip().split("\n")]
        return fields


class Database:
    """A wrapper class for managing database connection over sqlite3.

    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.is_connected = False
        self.connection = None
        self.cursor = None

    @contextmanager
    def connect(self):
        """Connects to the configured database in config.py.

        Used in `with` scope
            yields self
            closes connection upon exiting `with` scope

        Example:
            with db.connect():
                # do something with db
        """
        if self.is_connected:
            return
        self.is_connected = True
        self.connection = con = sqlite3.connect(self.db_path)
        self.cursor = con.cursor()
        yield self
        con.close()
        self.is_connected = False
        self.cursor = None
        self.connection = None

    def get_active_cursor(self):
        """Returns current connection cursor.

        Must run connect first.

        Raises:
            RuntimeError if not connected
        """
        if not self.is_connected or self.cursor is None:
            raise RuntimeError("Database not connected")
        return self.cursor

    def get_active_connection(self):
        """Returns current active connection.

        Must run connect first.

        Raises:
            RuntimeError if not connected
        """
        if not self.is_connected or self.connection is None:
            raise RuntimeError("Database not connected")
        return self.connection

    def create_table(self, table_name: str, schema: str, format_schema: bool=False):
        """Creates a table given schema, does nothing if already exists.

        Args:
            table_name
            schema
            format_schema: Formats schema if schema uses format in config.py
        """
        if format_schema:
            schema = DatabaseUtil.format_schema(schema)

        sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema}
            );
        """.strip()
        self.get_active_cursor().execute(sql)

    def drop_table(self, table_name):
        """Deletes given table in current db. Does nothing if no such table name.

        """
        sql = f"""
        DROP TABLE IF EXISTS {table_name};
        """.strip()
        self.get_active_cursor().execute(sql)
        self.vacuum()

    def vacuum(self):
        """Shrinks db size (if something has been deleted).

        """
        self.get_active_cursor().execute("VACUUM;")

    def add_table_rows(self, table_name, fields, iter_of_values: Iterable[Tuple]):
        """Adds new rows to an existing table.

        Args:
            table_name: str
            fields: Iterable[str]
            iter_of_values: Iterable[Any] :
                An iterable with values to be inserted
        """
        sql = f'''
        INSERT OR REPLACE INTO {table_name} ({", ".join(fields)})
        VALUES ({",".join("?" for _ in range(len(fields)))})
        '''.strip()
        self.get_active_cursor().executemany(sql, iter_of_values)
        self.get_active_connection().commit()

    def iterate_table_rows(self, table_name: str,
        select="*", where=None, order=None, reverse=False,
        one_row_a_time: bool = False, first_col_only: bool = False,
        fetch_size: int = DB_TABLE_FETCH_SIZE):
        """Fetches table rows by given criteria.

        A generator, so you control the rate of fetching.
        After all rows fetched, an empty tuple is yielded.

        Yields:
            one_row_a_time | first_col_only |     Yields    |     Type
                False      |      False     |  rows of rows | Tuple[Tuple]
                False      |      True      |  rows of col  |  Tuple[Any]
                True       |      False     |      row      |  Tuple[Any]
                True       |      True      |      col      |      Any

        Args:
            table_name:
            fetch_size:
                How many rows to fetch at max at once.
                You can use a large number to fetch all at once.
            one_row_a_time:
                Yields one row instead of rows at a time
            first_col_only:
                Yields first column instead of a while row
            select:
                Column names expression
                e.g. Multiple cols, sum(col), count(col)..
            where:
                Conditions expression
            order:
                A list of `Column name ASC/DSC`
            reverse:
                Reverse the order
        """
        order_sql = "" if order is None else f"ORDER BY {order}"
        where_sql = "" if where is None else f"WHERE {where}"
        reverse_sql = "" if reverse is False else "REVERSE"
        sql = f"""
        SELECT {select} FROM {table_name} {where_sql} {order_sql} {reverse_sql};
        """
        cursor = self.get_active_cursor()
        cursor.execute(sql)

        while True:
            rows: Tuple[Tuple] = cursor.fetchmany(size = fetch_size)

            if one_row_a_time and first_col_only:
                for row in rows:
                    yield row[0]
            elif not one_row_a_time and not first_col_only:
                yield rows
            elif one_row_a_time and not first_col_only:
                yield from rows
            else:
                yield tuple(row[0] for row in rows)

            if len(rows) == 0:
                break

    
    def is_table_exists(self, table_name: str):
        """Returns True if given table is in database.
        
        """
        sql = f"""
            SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';
        """
        self.get_active_cursor().execute(sql)
        res = self.get_active_cursor().fetchall()
        return len(res) > 0

    def copy_table(self, src_table_name: str, dest_table_name: str):
        """Copies all rows from one table to another table.
        
        """
        sql = f"""
        INSERT OR REPLACE INTO {dest_table_name} SELECT * FROM {src_table_name};
        """
        self.get_active_cursor().execute(sql)
        self.get_active_connection().commit()

class Table:
    """A convenient wrapper class of Table instance over Database.

    """
    def __init__(self, table_name: str, schema: str, database: Database):
        self.table_name = table_name
        self.database = database
        self.schema = DatabaseUtil.format_schema(schema)
        self.fields = DatabaseUtil.table_schema_to_fields(schema)

    def connect_database(self):
        """Database connect context.

        A convenient method
        More info visit Database.connect()
        """
        return self.database.connect()

    def create(self):
        """Creates table if not exists.

        """
        self.database.create_table(self.table_name, self.schema, format_schema=False)

    def drop(self):
        """Deletes table in db if exists.

        """
        self.database.drop_table(self.table_name)

    def add_rows(self, iter_of_values: Iterable[Tuple]):
        """Adds new rows to table.

        """
        self.database.add_table_rows(self.table_name, self.fields, iter_of_values)

    def iterate_rows(self, select="*", where=None, order=None, reverse=False,
        one_row_a_time: bool = False, first_col_only: bool = False,
        fetch_size: int = DB_TABLE_FETCH_SIZE):
        """Fetches table rows by given criteria.

        """
        yield from self.database.iterate_table_rows(self.table_name,
            select=select, where=where,
            order=order, reverse=reverse,
            one_row_a_time=one_row_a_time,
            first_col_only=first_col_only,
            fetch_size=fetch_size)

    def is_exists(self):
        """Returns True if this table exists in database.
        
        """
        return self.database.is_table_exists(self.table_name)

    def copy_to_table(self, dest_table: "Table"):
        """Copies all rows in this table and insert into another table.
        
        """
        return self.database.copy_table(self.table_name, dest_table.table_name)


database = Database(DB_PATH)
repo_raw_table = Table(
    table_name = REPO_RAW_TABLE_NAME,
    schema = REPO_TABLE_SCHEMA,
    database = database,
)
repo_filtered_table = Table(
    table_name = REPO_FILTERED_TABLE_NAME,
    schema = REPO_TABLE_SCHEMA,
    database = database,
)
repo_first_sample_table = Table(
    table_name = REPO_FIRST_SAMPLE_TABLE_NAME,
    schema = REPO_TABLE_SCHEMA,
    database = database,
)
repo_sample_blacklist_table = Table(
    table_name = REPO_SAMPLE_BLACKLIST_TABLE_NAME,
    schema = REPO_TABLE_SCHEMA,
    database = database,
)
