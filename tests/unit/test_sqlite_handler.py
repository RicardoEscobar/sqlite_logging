"""This is the unit tests for the sqlite_handler module."""

import unittest
from pathlib import Path
import sqlite3

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

        # Assert that the get_columns method returns an empty list when the
        # table does not exist
        handler.open()
        columns = handler.get_columns("log_record")
        self.assertEqual(columns, [])
        handler.close()

        # Assert that the get_columns method returns a list of column names
        # when the table exists
        handler = SqliteHandler(":memory:")
        handler.open()
        sql = "CREATE TABLE log_record (id INTEGER PRIMARY KEY, message TEXT);"
        handler.cursor.execute(sql)
        handler.connection.commit()

        columns = handler.get_columns("log_record")
        handler.close()
        self.assertEqual(columns, ["id", "message"])

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

    @unittest.skip("Not implemented")
    def test_emit(self):
        """Test the emit method."""
        # pylint: disable=protected-access
        # Create a SqliteHandler object
        handler = SqliteHandler()

        # Verify that the emit method raises a NotImplementedError
        with self.assertRaises(NotImplementedError):
            handler.emit(None)


if __name__ == "__main__":
    unittest.main()
