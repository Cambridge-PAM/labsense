"""Fetch minute-level electricity consumption from EmonCMS and insert into the SQL Server elecMinute table."""

import datetime
from urllib.request import urlopen
import json
import argparse
import os
import csv
from pathlib import Path
from dotenv import load_dotenv
import pyodbc

# Load environment variables from Labsense_SQL/.env
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
repo_root = Path(__file__).resolve().parents[1]

# Connection information - pull from environment variables with defaults
sqlServerName = os.getenv("SQL_SERVER", "MSM-FPM-70203\\LABSENSE")
databaseName = os.getenv("SQL_DATABASE", "labsense")
trusted_connection = os.getenv("SQL_TRUSTED_CONNECTION", "yes")
encryption_pref = os.getenv("SQL_ENCRYPTION", "Optional")

# Connection string information
connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sqlServerName};"
    f"DATABASE={databaseName};"
    f"Trusted_Connection={trusted_connection};"
    f"Encrypt={encryption_pref}"
)

# Get API key from environment variables
EMONCMS_API_KEY = os.getenv("EMONCMS_API_KEY")
if not EMONCMS_API_KEY:
    raise ValueError(
        "EMONCMS_API_KEY not found in environment variables. Please check your .env file."
    )


def create_table_if_not_exists():
    """Create the elecMinute table if it doesn't exist."""
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute(
            """
            IF 
            ( NOT EXISTS 
            (select object_id from sys.objects where object_id = OBJECT_ID(N'[elecMinute]') and type = 'U')
            )
            BEGIN
                CREATE TABLE elecMinute
                (
                    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    EnergyValue REAL,
                    Timestamp DATETIME
                )
            END
            """
        )
        connection.commit()
        connection.close()
    except pyodbc.Error as ex:
        print("An error occurred creating table:", ex)
        raise


def insert_sql(datapoints):
    """Insert minute-level consumption data.

    Args:
        datapoints: List of [timestamp_ms, cumulative_energy] pairs from API

    Returns:
        tuple: (success_count, skip_count)
    """
    if not datapoints or len(datapoints) < 2:
        return 0, 0

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        success_count = 0
        skip_count = 0

        # Calculate interval energy by subtracting consecutive cumulative values
        for i in range(1, len(datapoints)):
            _prev_timestamp_ms, prev_cumulative = datapoints[i - 1]
            curr_timestamp_ms, curr_cumulative = datapoints[i]

            if prev_cumulative is None or curr_cumulative is None:
                continue

            # Energy consumed during this interval (difference between cumulative values)
            interval_energy = curr_cumulative - prev_cumulative

            # Convert millisecond timestamp to datetime (use current timestamp as interval end)
            timestamp = datetime.datetime.fromtimestamp(curr_timestamp_ms / 1000)

            # Check if record already exists for this timestamp
            cursor.execute(
                "SELECT COUNT(*) FROM elecMinute WHERE Timestamp = ?", (timestamp,)
            )
            if cursor.fetchone()[0] > 0:
                skip_count += 1
                continue

            cursor.execute(
                """
                INSERT INTO elecMinute (EnergyValue, Timestamp)
                VALUES (?,?)""",
                (interval_energy, timestamp),
            )
            success_count += 1

        connection.commit()
        connection.close()
        return success_count, skip_count

    except pyodbc.Error as ex:
        print("An error occurred in SQL Server:", ex)
        return 0, 0


def get_minute_data_for_date(target_date):
    """
    Get minute-level data for a specific date.

    Args:
        target_date: datetime.date object for the day to get data

    Returns:
        list: List of [timestamp_ms, cumulative_energy] pairs, or None if failed
    """
    try:
        # Start of target date
        start_datetime = datetime.datetime.combine(target_date, datetime.time.min)
        start_timestamp_ms = int(start_datetime.timestamp()) * 1000

        # Start of next day
        end_datetime = datetime.datetime.combine(
            target_date + datetime.timedelta(days=1), datetime.time.min
        )
        end_timestamp_ms = int(end_datetime.timestamp()) * 1000

        url_minute = (
            "http://10.247.40.36/feed/average.json?id=21&start="
            + str(start_timestamp_ms)
            + "&end="
            + str(end_timestamp_ms)
            + f"&interval=60&apikey={EMONCMS_API_KEY}"
        )

        response = urlopen(url_minute)
        data_json = json.loads(response.read())

        if not data_json:
            print(f"No data returned for {target_date}")
            return None

        # data_json is a list of [timestamp_ms, value] pairs
        return data_json

    except Exception as ex:
        print(f"Error fetching data for {target_date}:", ex)
        return None


def process_date_range(start_date, end_date):
    """
    Process minute-level consumption for a range of dates.

    Args:
        start_date: datetime.date object for the first day
        end_date: datetime.date object for the last day (inclusive)
    """
    create_table_if_not_exists()

    current_date = start_date
    total_success = 0
    total_skip = 0
    error_count = 0

    while current_date <= end_date:
        print(f"Processing {current_date}...")

        datapoints = get_minute_data_for_date(current_date)

        if datapoints is not None:
            success, skip = insert_sql(datapoints)
            print(f"  Inserted: {success} records, Skipped: {skip} duplicates")
            total_success += success
            total_skip += skip
        else:
            error_count += 1

        current_date += datetime.timedelta(days=1)

    print(
        f"\nSummary: {total_success} records inserted, {total_skip} skipped, {error_count} days with errors"
    )


def export_sql_data_for_date(target_date, output_path=None):
    """Export elecMinute SQL rows for a given date to CSV.

    Args:
        target_date: datetime.date for the day to export
        output_path: Optional CSV path. Defaults to plots/elecMinute_YYYY-MM-DD.csv

    Returns:
        Path to the written CSV file.
    """
    start_datetime = datetime.datetime.combine(target_date, datetime.time.min)
    end_datetime = start_datetime + datetime.timedelta(days=1)

    output_file = (
        Path(output_path)
        if output_path
        else repo_root / "plots" / f"elecMinute_{target_date.isoformat()}.csv"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT id, EnergyValue, Timestamp
            FROM elecMinute
            WHERE Timestamp >= ? AND Timestamp < ?
            ORDER BY Timestamp ASC
            """,
            (start_datetime, end_datetime),
        )
        rows = cursor.fetchall()
        connection.close()
    except pyodbc.Error as ex:
        print(f"An error occurred exporting SQL data: {ex}")
        raise

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["id", "EnergyValue", "Timestamp"])
        for row in rows:
            writer.writerow([row.id, row.EnergyValue, row.Timestamp])

    print(f"Exported {len(rows)} elecMinute rows to {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Insert minute-level electricity consumption data into SQL Server. "
        "By default, inserts data for yesterday only."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format. If provided, --end-date must also be specified.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (inclusive). If provided, --start-date must also be specified.",
    )
    parser.add_argument(
        "--export-date",
        type=str,
        help="Export elecMinute SQL data for a single date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional output CSV path for --export-date.",
    )

    args = parser.parse_args()

    # Validate arguments
    if (args.start_date and not args.end_date) or (
        args.end_date and not args.start_date
    ):
        parser.error("Both --start-date and --end-date must be provided together")

    if args.export_date and (args.start_date or args.end_date):
        parser.error("--export-date cannot be combined with --start-date/--end-date")

    if args.export_date:
        try:
            export_date = datetime.datetime.strptime(
                args.export_date, "%Y-%m-%d"
            ).date()
        except ValueError as ex:
            parser.error(f"Invalid export date format: {ex}. Use YYYY-MM-DD format.")

        export_sql_data_for_date(export_date, output_path=args.output)
        return

    if args.start_date and args.end_date:
        # Parse dates
        try:
            start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()

            if start_date > end_date:
                parser.error("--start-date must be before or equal to --end-date")

            print(f"Processing date range: {start_date} to {end_date}")
            process_date_range(start_date, end_date)

        except ValueError as ex:
            parser.error(f"Invalid date format: {ex}. Use YYYY-MM-DD format.")
    else:
        # Default behavior: process yesterday only
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        print(f"Processing yesterday: {yesterday}")
        process_date_range(yesterday, yesterday)


if __name__ == "__main__":
    main()
