"""SQL helper utilities for inserts and toggles."""

from typing import Any, Sequence, Optional
import os
import logging
import pyodbc

logger = logging.getLogger(__name__)


def insert_to_sql(
    category: str, new_row: Sequence[Any], connection_string: str
) -> None:
    """Insert a row into SQL Server. Caller provides `connection_string`.

    Raises pyodbc.Error on failure (logged).
    """
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        sql_query_1 = f"""
            IF NOT EXISTS
            (SELECT object_id
            FROM sys.objects
            WHERE object_id = OBJECT_ID(N'[{category}]')
            AND type = 'U')
            BEGIN
                CREATE TABLE [{category}]
                (
                    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    RedVol REAL,
                    YellowVol REAL,
                    GreenVol REAL,
                    Timestamp DATETIME
                )
            END
        """
        cursor.execute(sql_query_1)

        sql_query_2 = f"""
            INSERT INTO {category} (RedVol, YellowVol, GreenVol, Timestamp)
            VALUES (?, ?, ?, ?)
        """
        cursor.execute(sql_query_2, (new_row[0], new_row[1], new_row[2], new_row[3]))
        connection.commit()
        connection.close()
        logger.info("Completed insert of %s data.", category)
    except pyodbc.Error as ex:
        logger.exception("An error occurred in SQL Server: %s", ex)
        raise


def maybe_insert(
    category: str,
    new_row: Sequence[Any],
    connection_string: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> None:
    """Insert into SQL only when enabled.

    - If `enabled` is None, read from env var `INSERT_TO_SQL`.
    - `connection_string` is required when inserting.
    """
    if enabled is None:
        enabled = os.getenv("INSERT_TO_SQL", "True").strip().lower() in (
            "1",
            "true",
            "yes",
        )
    if not enabled:
        logger.info("Skipping SQL insert for %s (INSERT_TO_SQL disabled).", category)
        return
    if not connection_string:
        logger.warning("Cannot insert %s: no connection string provided.", category)
        return
    insert_to_sql(category, new_row, connection_string)
