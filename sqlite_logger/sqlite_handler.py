"""Custom handler for logging record information on a SQLite3 database."""

import logging
from pathlib import Path
from sqlite3 import Connection, Cursor
from datetime import datetime, UTC
from typing import Union


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

    def emit(self, record):
        """Emit a record to the provided SQLite database."""
        # Get the log_record table column list
        columns = self.database.get_columns("log_record")
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
        self.database.insert("log_record", insert_columns, values)


def main():
    """Main function."""
    database_path = Path("database/logging.db")
    sql_initial_script = Path("database/logging.sql")
    database = Database(database_path)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s; %(levelname)s; %(message)s")
    handler = logging.StreamHandler()
    sqlite_handler = SqliteHandler(database, sql_initial_script)
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


if __name__ == "__main__":
    main()
