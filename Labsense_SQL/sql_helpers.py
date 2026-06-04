"""SQL helper utilities for inserts, toggles, and table exports."""

from typing import Any, Sequence, Optional
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import csv
import os
import logging
import pyodbc

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional runtime dependency
    load_dotenv = None

logger = logging.getLogger(__name__)


def _load_sql_env() -> None:
    """Load Labsense SQL environment variables from .env when available."""
    env_path = Path(__file__).parent / ".env"
    if load_dotenv and env_path.exists():
        load_dotenv(dotenv_path=env_path)


def _build_sql_connection_string() -> str:
    """Build SQL connection string from environment variables."""
    sql_server = os.getenv("SQL_SERVER", "MSM-FPM-70203\\LABSENSE").strip()
    sql_database = os.getenv("SQL_DATABASE", "labsense").strip()
    sql_trusted = os.getenv("SQL_TRUSTED_CONNECTION", "yes").strip().lower()
    sql_encrypt = os.getenv("SQL_ENCRYPTION", "Optional").strip()
    sql_user = os.getenv("SQL_USER", "").strip()
    sql_password = os.getenv("SQL_PASSWORD", "").strip()

    if not sql_server or not sql_database:
        raise ValueError("SQL_SERVER and SQL_DATABASE must be configured.")

    if sql_trusted == "yes":
        return (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={sql_server};"
            f"DATABASE={sql_database};"
            "Trusted_Connection=yes;"
            f"Encrypt={sql_encrypt}"
        )

    if not sql_user or not sql_password:
        raise ValueError(
            "SQL_USER and SQL_PASSWORD are required when SQL_TRUSTED_CONNECTION is not 'yes'."
        )

    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_password};"
        f"Encrypt={sql_encrypt}"
    )


def _escape_sql_identifier(identifier: str) -> str:
    """Escape a SQL Server identifier to prevent malformed bracket names."""
    return identifier.replace("]", "]]")


def export_tables_between_dates(
    start_date: str,
    end_date: str,
    output_dir: str,
    connection_string: Optional[str] = None,
) -> list[str]:
    """Export all timestamped SQL tables to CSV files for a date range.

    Date strings must be in YYYY-MM-DD format.
    The range is inclusive on both days: [start_date 00:00:00, end_date 23:59:59].
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt_exclusive = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    if end_dt_exclusive <= start_dt:
        raise ValueError("end_date must be the same as or later than start_date.")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    conn_str = connection_string or _build_sql_connection_string()
    exported_files: list[str] = []

    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        table_rows = cursor.execute(
            """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
        ).fetchall()

        for schema_name, table_name in table_rows:
            has_timestamp = cursor.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = 'Timestamp'
                """,
                (schema_name, table_name),
            ).fetchone()
            if not has_timestamp:
                logger.info(
                    "Skipping %s.%s: no Timestamp column found.",
                    schema_name,
                    table_name,
                )
                continue

            safe_schema = _escape_sql_identifier(schema_name)
            safe_table = _escape_sql_identifier(table_name)
            query = (
                f"SELECT * FROM [{safe_schema}].[{safe_table}] "
                "WHERE [Timestamp] >= ? AND [Timestamp] < ? "
                "ORDER BY [Timestamp] ASC"
            )
            table_cursor = connection.cursor()
            table_cursor.execute(query, (start_dt, end_dt_exclusive))
            rows = table_cursor.fetchall()
            headers = (
                [column[0] for column in table_cursor.description]
                if table_cursor.description
                else []
            )

            csv_filename = f"{schema_name}_{table_name}_{start_date}_to_{end_date}.csv"
            csv_path = output_path / csv_filename
            with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(rows)

            exported_files.append(str(csv_path))
            logger.info(
                "Exported %s rows from %s.%s to %s",
                len(rows),
                schema_name,
                table_name,
                csv_path,
            )

    return exported_files


def _build_arg_parser() -> argparse.ArgumentParser:
    """Create CLI parser for SQL table export operations."""
    parser = argparse.ArgumentParser(
        description="Export SQL tables with Timestamp column between two dates"
    )
    parser.add_argument(
        "--export-between-dates",
        action="store_true",
        help="Export every timestamped SQL table between --start-date and --end-date",
    )
    parser.add_argument(
        "--start-date",
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "plots" / "sql_exports"),
        help="Directory to write table CSV exports",
    )
    return parser


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

    - If `enabled` is None, read from env var `CHEMINVENTORY_INSERT_TO_SQL`.
    - `connection_string` is required when inserting.
    """
    if enabled is None:
        enabled = os.getenv("CHEMINVENTORY_INSERT_TO_SQL", "True").strip().lower() in (
            "1",
            "true",
            "yes",
        )
    if not enabled:
        logger.info(
            "Skipping SQL insert for %s (CHEMINVENTORY_INSERT_TO_SQL disabled).",
            category,
        )
        return
    if not connection_string:
        logger.warning("Cannot insert %s: no connection string provided.", category)
        return
    insert_to_sql(category, new_row, connection_string)


if __name__ == "__main__":
    _load_sql_env()
    parser = _build_arg_parser()
    args = parser.parse_args()

    if not args.export_between_dates:
        parser.error("No operation selected. Use --export-between-dates.")

    if not args.start_date or not args.end_date:
        parser.error(
            "--start-date and --end-date are required with --export-between-dates."
        )

    exported = export_tables_between_dates(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
    )
    print(f"Exported {len(exported)} table(s) to: {args.output_dir}")
