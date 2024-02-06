"""This is the unit tests for the sqlite_handler module."""

import unittest
from pathlib import Path

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
