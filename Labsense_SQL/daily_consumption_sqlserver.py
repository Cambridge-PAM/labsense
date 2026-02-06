import datetime
from urllib.request import urlopen
import json
import pyodbc
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file in the same directory as this script
script_dir = Path(__file__).parent
env_path = script_dir / '.env'
load_dotenv(dotenv_path=env_path)

# Connection information
# Your SQL Server instance
sqlServerName = "MSM-FPM-70203\\LABSENSE"
# Your database
databaseName = "labsense"
# Use Windows authentication
trusted_connection = "yes"
# Encryption
encryption_pref = "Optional"
# Connection string information
connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sqlServerName};"
    f"DATABASE={databaseName};"
    f"Trusted_Connection={trusted_connection};"
    f"Encrypt={encryption_pref}"
)

# Get API key from environment variables
EMONCMS_API_KEY = os.getenv('EMONCMS_API_KEY')
if not EMONCMS_API_KEY:
    raise ValueError("EMONCMS_API_KEY not found in environment variables. Please check your .env file.")


def create_table_if_not_exists():
    """Create the elecDaily table if it doesn't exist."""
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute(
            """
            IF 
            ( NOT EXISTS 
            (select object_id from sys.objects where object_id = OBJECT_ID(N'[elecDaily]') and type = 'U')
            )
            BEGIN
                CREATE TABLE elecDaily
                (
                    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    Esum REAL,
                    Datestamp DATE
                )
            END
            """
        )
        connection.commit()
        connection.close()
    except pyodbc.Error as ex:
        print("An error occurred creating table:", ex)
        raise


def insert_sql(daily_consumption, date):
    """Insert daily consumption data for a specific date."""
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        # Check if record already exists for this date
        cursor.execute(
            "SELECT COUNT(*) FROM elecDaily WHERE Datestamp = ?",
            (date,)
        )
        if cursor.fetchone()[0] > 0:
            print(f"Record for {date} already exists, skipping...")
            connection.close()
            return False
        
        cursor.execute(
            """
            INSERT INTO elecDaily (Esum, Datestamp)
            VALUES (?,?)""",
            (daily_consumption, date),
        )
        
        connection.commit()
        connection.close()
        return True

    except pyodbc.Error as ex:
        print(f"An error occurred in SQL Server for date {date}:", ex)
        return False


def get_daily_consumption_for_date(target_date):
    """
    Get daily consumption for a specific date.
    
    Args:
        target_date: datetime.date object for the day to get consumption
        
    Returns:
        float: Daily consumption value, or None if failed
    """
    try:
        # Start of target date
        start_datetime = datetime.datetime.combine(target_date, datetime.time.min)
        start_timestamp_ms = int(start_datetime.timestamp()) * 1000
        
        # Start of next day
        end_datetime = datetime.datetime.combine(
            target_date + datetime.timedelta(days=1), 
            datetime.time.min
        )
        end_timestamp_ms = int(end_datetime.timestamp()) * 1000
        
        url_daily = (
            "http://10.247.40.36/feed/data.json?id=21&start="
            + str(start_timestamp_ms)
            + "&end="
            + str(end_timestamp_ms)
            + f"&mode=daily&apikey={EMONCMS_API_KEY}"
        )
        
        response_daily = urlopen(url_daily)
        data_json_daily = json.loads(response_daily.read())
        
        if len(data_json_daily) < 2:
            print(f"Insufficient data for {target_date}")
            return None
            
        e_sum_start = data_json_daily[0][1]
        e_sum_end = data_json_daily[1][1]
        daily_consumption = e_sum_end - e_sum_start
        
        return daily_consumption
        
    except Exception as ex:
        print(f"Error fetching data for {target_date}:", ex)
        return None


def process_date_range(start_date, end_date):
    """
    Process daily consumption for a range of dates.
    
    Args:
        start_date: datetime.date object for the first day
        end_date: datetime.date object for the last day (inclusive)
    """
    create_table_if_not_exists()
    
    current_date = start_date
    success_count = 0
    skip_count = 0
    error_count = 0
    
    while current_date <= end_date:
        print(f"Processing {current_date}...")
        
        daily_consumption = get_daily_consumption_for_date(current_date)
        
        if daily_consumption is not None:
            if insert_sql(daily_consumption, current_date):
                print(f"  Inserted: {daily_consumption} kWh for {current_date}")
                success_count += 1
            else:
                skip_count += 1
        else:
            error_count += 1
            
        current_date += datetime.timedelta(days=1)
    
    print(f"\nSummary: {success_count} inserted, {skip_count} skipped, {error_count} errors")


def main():
    parser = argparse.ArgumentParser(
        description="Insert daily electricity consumption data into SQL Server. "
                    "By default, inserts data for yesterday only."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format. If provided, --end-date must also be specified."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (inclusive). If provided, --start-date must also be specified."
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
        parser.error("Both --start-date and --end-date must be provided together")
    
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
