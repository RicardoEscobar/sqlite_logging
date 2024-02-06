"""Custom handler for logging record information on a SQLite3 database."""

import logging
from pathlib import Path
import sqlite3
from datetime import datetime, UTC
from typing import Union, List, Dict


class SqliteHandler(logging.StreamHandler):
    """Custom handler for logging record information on a SQLite3 database."""

    def __init__(self, database_file: Union[Path, str] = ":memory:") -> None:
        """Initialize the handler.
        args:
            database_file: The path to the SQLite3 database file. If the file
            does not exist, it will be created. If the file is ":memory:", a
            new database will be created in memory.
        """
        super().__init__()
        self.database_file = None
        self.connection = None
        self.cursor = None

        # Validate the database_file argument
        if (
            isinstance(database_file, str)
            and database_file != ":memory:"
            and database_file != ""
            and database_file is not None
        ):
            database_file = Path(database_file)
            database_file.parent.mkdir(parents=True, exist_ok=True)
        elif database_file == ":memory:":
            self.database_file = database_file
        elif database_file == "":
            raise ValueError("database_file cannot be an empty string")
        elif database_file is None:
            raise ValueError("database_file cannot be None")
        elif not isinstance(database_file, Path):
            raise TypeError(
                f"database_file must be a Path or str, not {type(database_file)}"
            )
        self.database_file = database_file

    def open(self):
        """Open the database connection."""
        self.connection = sqlite3.connect(self.database_file)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        # Create the log_record table in the database if it does not exist
        if "log_record" not in self.get_tables():
            self.create_logging_table()

    def close(self):
        """Close the database connection."""
        super().close()
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.cursor = None

    def create_logging_table(self, sql_initial_script: Path = None) -> None:
        """Create the log_record table in the database."""
        # Initialize sql_initial_script to a default value if it is None
        if sql_initial_script is None:
            sql_initial_script = Path("sqlite_logger/logging.sql")
        with open(sql_initial_script, "r", encoding="utf-8") as sql_file:
            sql = sql_file.read()
            self.cursor.executescript(sql)

    def get_columns(self, table_name: str) -> List[str]:
        """Return a list of columns in the table."""
        sql = f"PRAGMA table_info({table_name});"
        if self.database_file == ":memory:":
            self.cursor.execute(sql)
            columns = self.cursor.fetchall()
        else:
            self.open()
            self.cursor.execute(sql)
            columns = self.cursor.fetchall()
            self.close()
        result = [column["name"] for column in columns]
        return result

    def get_tables(self) -> List[str]:
        """Return a list of tables in the database."""
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' and name != 'sqlite_sequence';"
        )
        tables = self.cursor.fetchall()

        # This list comprehension is used to convert the list of tuples
        # returned to a list of strings.
        result = [table[0] for table in tables]
        return result

    def insert_log(self, values: Dict[str, str]) -> None:
        """Insert a log record into the log_record table."""
        columns = list(values.keys())
        values = list(values.values())
        sql = f"INSERT INTO log_record ({', '.join(columns)}) VALUES ({', '.join(['?']*len(columns))});"
        self.cursor.execute(sql, values)
        self.connection.commit()

    def emit(self, record):
        """Emit a record to the provided SQLite database."""
        # Get the log_record table column list
        columns = self.get_columns("log_record")
        insert_columns = list()
        values = list()
        # list the attributes of the record object
        for attribute in dir(record):
            if attribute == "getMessage":
                get_message = getattr(record, attribute)()
                values.append(get_message)
                insert_columns.append("message")
            if attribute in columns:
                # When the attribute is "created" convert the timestamp into a
                # datetime object and format it into a string to be added to
                # the values list
                if attribute == "created":
                    timestamp = getattr(record, attribute)
                    # Convert the timestamp into a datetime object
                    asctime_utc = datetime.fromtimestamp(timestamp, UTC)
                    asctime_local = asctime_utc.astimezone()

                    # Format the datetime object into a string
                    format_str = "%Y-%m-%d %H:%M:%S,%f"
                    formatted_time_utc = asctime_utc.strftime(format_str)[:-3]
                    values.append(formatted_time_utc)
                    insert_columns.append("asctime_utc")

                    formatted_time_local = asctime_local.strftime(format_str)[:-3]
                    values.append(formatted_time_local)
                    insert_columns.append("asctime")

                value = getattr(record, attribute)
                if not isinstance(value, (int, float, bytes, str)):
                    value = str(value)
                values.append(value)
                insert_columns.append(attribute)

        # Insert the record into the database
        values_dict = dict(zip(insert_columns, values))
        if self.database_file == ":memory:":
            self.insert_log(values_dict)
        else:
            self.open()
            self.insert_log(values_dict)
            self.close()


def main():
    """Main function."""
    database_path = Path("sqlite_logger/logging.db")
    sql_initial_script = Path("sqlite_logger/logging.sql")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s; %(levelname)s; %(message)s")
    handler = logging.StreamHandler()
    sqlite_handler = SqliteHandler(database_path)
    # sqlite_handler.open()
    sqlite_handler.setLevel(logging.DEBUG)
    sqlite_handler.setFormatter(formatter)
    handler.setFormatter(formatter)
    logger.addHandler(sqlite_handler)

    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warn message")
    try:
        1 / 0
    except ZeroDivisionError as exeption:
        logger.error("%s:\nerror message", exeption, exc_info=True, stack_info=True)
        logger.critical(
            "%s:\ncritical message", exeption, exc_info=True, stack_info=True
        )
    # sqlite_handler.close()


if __name__ == "__main__":
    main()
