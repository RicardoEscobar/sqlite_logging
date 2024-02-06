"""This is the unit tests for the sqlite_handler module."""

import unittest
from pathlib import Path
import sqlite3
import logging

from sqlite_logger.sqlite_handler import SqliteHandler


class TestSqliteHandler(unittest.TestCase):
    """Test the SqliteHandler class."""

    def test___init__(self):
        """Test the __init__ method."""
        # Assert that the handler.database_file attribute is ":memory:" when no
        # database_file argument is provided
        handler = SqliteHandler()
        self.assertEqual(handler.database_file, ":memory:")

        # Assert that the handler.database_file attribute is ":memory:" when
        # the database_file argument is ":memory:"
        handler = SqliteHandler(":memory:")
        self.assertEqual(handler.database_file, ":memory:")

        # Assert that the handler.database attribute is a path object when the
        # database_file argument is a path object
        database_filepath = Path("logging.db")
        handler = SqliteHandler(database_filepath)
        self.assertIsInstance(handler.database_file, Path)

        # Assert that the handler.database attribute is a path object when the
        # database_file argument is a string
        database_filepath = "logging.db"
        handler = SqliteHandler(database_filepath)
        self.assertIsInstance(handler.database_file, Path)

        # Assert that the handler raises a TypeError when the database_file is
        # not a string or path object
        with self.assertRaises(TypeError):
            SqliteHandler(1)

        # Assert that the handler raises a ValueError when the database_file is
        # an empty string
        with self.assertRaises(ValueError):
            SqliteHandler("")

        # Assert that the handler raises a ValueError when the database_file is
        # None
        with self.assertRaises(ValueError):
            SqliteHandler(None)

    def test_open(self):
        """Test the open method."""
        # Assert that the open method creates a database connection
        handler = SqliteHandler()
        handler.open()
        self.assertIsNotNone(handler.connection)
        self.assertIsNotNone(handler.cursor)
        row_type = type(sqlite3.Row)
        self.assertIsInstance(handler.connection.row_factory, row_type)
        handler.close()

    def test_close(self):
        """Test the close method."""
        # Assert that the close method closes the database connection
        handler = SqliteHandler()
        handler.open()
        handler.close()
        self.assertIsNone(handler.connection)
        self.assertIsNone(handler.cursor)

    def test_get_columns(self):
        """Test the get_columns method."""
        # Create a SqliteHandler object
        handler = SqliteHandler()

        # Assert that the get_columns method returns a list of column names
        # when the table exists
        handler = SqliteHandler(":memory:")
        handler.open()
        columns = handler.get_columns("log_record")
        handler.close()
        expected_columns = [
            "id",
            "args",
            "asctime",
            "asctime_utc",
            "created",
            "exc_info",
            "filename",
            "funcName",
            "levelname",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "taskName",
        ]
        self.assertEqual(columns, expected_columns)

    def test_create_logging_table(self):
        """Test the create_logging_table method."""
        # Create a SqliteHandler object
        handler = SqliteHandler()

        # Assert that the create_logging_table method creates the log_record
        # table in the database
        handler.open()
        handler.create_logging_table()
        columns = handler.get_columns("log_record")
        expected_columns = [
            "id",
            "args",
            "asctime",
            "asctime_utc",
            "created",
            "exc_info",
            "filename",
            "funcName",
            "levelname",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "taskName",
        ]
        handler.close()
        self.assertEqual(columns, expected_columns)

    def test_insert_log(self):
        """Test the insert_log method."""
        # Create a SqliteHandler object
        handler = SqliteHandler()

        # Assert that the insert_log method inserts a log record into the
        # log_record table
        handler.open()
        handler.create_logging_table()
        values = {
            "args": None,
            "asctime": "2021-10-10 10:10:10,000",
            "asctime_utc": "2021-10-10 10:10:10,000",
            "created": 1633852210.0,
            "exc_info": None,
            "filename": "test_sqlite_handler.py",
            "funcName": "test_insert_log",
            "levelname": "INFO",
            "lineno": 100,
            "message": "test message",
            "module": "test_sqlite_handler",
            "msecs": 0,
            "msg": "test message",
            "name": "test_sqlite_handler",
            "pathname": "test_sqlite_handler.py",
            "process": 1000,
            "processName": "MainProcess",
            "relativeCreated": 0,
            "stack_info": None,
            "thread": 1,
            "threadName": "MainThread",
            "taskName": "MainTask",
        }
        handler.insert_log(values)
        sql = "SELECT * FROM log_record;"
        handler.cursor.execute(sql)
        result = handler.cursor.fetchone()
        handler.close()

        for key, value in zip(result.keys(), tuple(result)):
            if key != "id":
                self.assertEqual(result[key], values[key])
                self.assertEqual(value, values[key])

    # @unittest.skip("Not implemented")
    def test_emit(self):
        """Test the emit method."""
        handler = SqliteHandler()
        handler.open()
        handler.create_logging_table()
        record = logging.LogRecord(
            "test_logger",
            logging.INFO,
            "test_sqlite_handler.py",
            100,
            "test message",
            None,
            None,
            "test_emit",
        )
        handler.emit(record)
        sql = "SELECT * FROM log_record;"
        handler.cursor.execute(sql)
        row = handler.cursor.fetchone()
        for key, value in zip(row.keys(), tuple(row)):
            if (
                key != "id"
                and key != "asctime"
                and key != "asctime_utc"
                and key != "message"
            ):
                self.assertEqual(str(row[key]), str(getattr(record, key)))
                self.assertEqual(str(value), str(getattr(record, key)))
        handler.close()

    def test_get_tables(self):
        """Test the get_tables method."""
        # Create a SqliteHandler object
        handler = SqliteHandler()

        # Assert that the get_tables method returns an empty list when the
        # database is empty
        handler.open()
        tables = handler.get_tables()
        handler.close()
        self.assertEqual(tables, ["log_record"])

        # Assert that the get_tables method returns a list of tables in the
        # database
        handler = SqliteHandler(":memory:")
        handler.open()
        tables = handler.get_tables()
        self.assertEqual(tables, ["log_record"])
        handler.close()


if __name__ == "__main__":
    unittest.main()
