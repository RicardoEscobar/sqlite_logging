"""This is the unit tests for the sqlite_handler module."""
import unittest
from pathlib import Path

from sqlite_logger.sqlite_handler import SqliteHandler


class TestSqliteHandler(unittest.TestCase):
    """Test the SqliteHandler class."""

    def test___init__(self):
        """Test the __init__ method."""
        # Create a SqliteHandler object
        database_filepath = Path("logging.db")
        handler = SqliteHandler(database_filepath)

        # Assert that the handler.database_file attribute is ":memory:" when no
        # database_file argument is provided
        handler = SqliteHandler()
        self.assertEqual(handler.database_file, ":memory:")
        
        # Assert that the handler.database attribute is a path object
        self.assertIsInstance(handler.database_file, Path)


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
