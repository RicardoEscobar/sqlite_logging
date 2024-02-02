"""This module contains the MyLogger class.
It's a subclass of the logging.Logger class and is used to create a custom
logger that can log to a file, the console, and a database."""

import logging
from pathlib import Path
from typing import Union
from sqlite3 import Connection, Cursor, connect


class MyLogger(logging.Logger):
    """This class represents the logger for the application."""

    def __init__(
        self,
        name: str,
        log_file: Union[Path, str] = "logs/app.log",
        database_file: Union[Path, str] = ":memory:",
        level: int = logging.INFO,
        log_to_file: bool = True,
        log_to_console: bool = True,
        log_to_database: bool = True,
    ):
        """Initialize the logger."""
        super().__init__(name, level)

        # Validate the log_file argument
        if isinstance(log_file, str):
            log_file = Path(log_file)
        if not isinstance(log_file, Path):
            raise TypeError(f"log_file must be a Path or str, not {type(log_file)}")

        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        # Validate the database_file argument
        self.database_file = None
        if isinstance(database_file, str) and database_file != ":memory:":
            database_file = Path(database_file)
            database_file.parent.mkdir(parents=True, exist_ok=True)
        if not isinstance(database_file, Path):
            raise TypeError(
                f"database_file must be a Path or str, not {type(database_file)}"
            )

        self.database_file = database_file

        # Create a file handler to write to the log file
        self.file_handler = None
        if log_to_file:
            self.file_handler = logging.FileHandler(self.log_file)
            self.file_handler.setLevel(level)
            self.file_formatter = logging.Formatter(
                "%(asctime)s|%(name)s|%(levelname)s|%(message)s"
            )
            self.file_handler.setFormatter(self.file_formatter)
            self.addHandler(self.file_handler)
            self.info("File logging enabled")

        # Create a stream handler to print to stdout
        self.stream_handler = None
        if log_to_console:
            self.stream_handler = logging.StreamHandler()
            self.stream_handler.setLevel(level)
            self.stream_formatter = logging.Formatter("%(message)s")
            self.stream_handler.setFormatter(self.stream_formatter)
            self.addHandler(self.stream_handler)
            self.info("Console logging enabled")

        # Validate the initial_database_script argument
        if isinstance(initial_database_script, str):
            initial_database_script = Path(initial_database_script)
        if not isinstance(initial_database_script, Path):
            raise TypeError(
                (
                    f"initial_database_script must be a Path or str,"
                    f" not {type(initial_database_script)}"
                )
            )
        if not initial_database_script.exists():
            raise FileNotFoundError(
                f"initial_database_script not found at {initial_database_script}"
            )
        else:
            self.initial_database_script = initial_database_script

        # Create a database handler to write to a database
        self.database = None
        if log_to_database:
            if database:
                if isinstance(database, Database):
                    self.database = database
                elif isinstance(database, str):
                    database_path = Path(database)
                    self.database = Database(database_path)
                    # Check if database file exists and contains the correct
                    # table "log_record"
                    if not self.database.table_exists("log_record"):
                        self.database.close()
                        self.database = Database(
                            database_path, self.initial_database_script
                        )
                elif isinstance(database, Path):
                    self.database = Database(database, self.initial_database_script)
                    # Create the database file if it does not exist
                    if not database.exists():
                        database.parent.mkdir(parents=True, exist_ok=True)
                        self.database = Database(database)
                else:
                    raise TypeError("database must be a Database or str")
            else:
                DEFAULT_DATABASE = "database/logging.db"
                database_path = Path(DEFAULT_DATABASE)
                database_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    self.database = Database(database_path)
                except Exception as exeption:
                    self.warning(
                        "Unable to open database at %s: %s",
                        database_path,
                        exeption,
                    )
                    # Create an empty database object
                    self.database = Database(":memory:")

            # TODO: Replace this with a check to see if the database is an
            # actual Database object
            # self.database = database
            self.sqlite_handler = SqliteHandler(self.database)
            self.sqlite_handler.setLevel(level)
            self.addHandler(self.sqlite_handler)
            self.info("Database logging enabled")

        self.info("Logger initialized")
