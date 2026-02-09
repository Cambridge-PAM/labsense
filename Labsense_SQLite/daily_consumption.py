import datetime
import time
from urllib.request import urlopen
import json
import sqlite3


def insert_sql(daily_consumption, date):

    conn = sqlite3.connect("labsense.db", timeout=10)  # create connection
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS elec_daily (
        id INTEGER PRIMARY KEY,
        Esum REAL NOT NULL,
        Date DATE NOT NULL
    )
    """
    )  # create table

    cursor.execute(
        """
    INSERT INTO elec_daily (Esum,Date)
    VALUES (?,?)""",
        (daily_consumption, date),
    )  # insert into table

    # cursor.execute('SELECT * FROM elec_daily')
    # rows = cursor.fetchall()

    # column_names = [description[0] for description in cursor.description]
    # print(f"{column_names}")
    # for row in rows:
    #    print(row)  #printing table, for debugging purposes
    conn.commit()
    conn.close()


def main():
    midnight = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
    timestamp_seconds = int(midnight.timestamp())
    timestamp_milliseconds = (
        timestamp_seconds * 1000
    )  # finding UNIX_MILLISECOND timestamp at midnight

    now = datetime.datetime.now()
    midnight_previous_day = datetime.datetime.combine(
        now.date() - datetime.timedelta(days=1), datetime.time.min
    )

    timestamp_seconds = int(midnight_previous_day.timestamp())
    timestamp_milliseconds_previous_day = (
        timestamp_seconds * 1000
    )  # finding UNIX_MILLISECOND timestamp at previous midnight

    url_daily = (
        "http://10.247.40.36/feed/data.json?id=21&start="
        + str(timestamp_milliseconds_previous_day)
        + "&end="
        + str(timestamp_milliseconds)
        + "&mode=daily&apikey=APIKEY"
    )  # details about how url is formed can be found here https://emoncms.org/site/api#feed , add your own APIKEY

    response_daily = urlopen(url_daily)
    data_json_daily = json.loads(response_daily.read())
    e_sum_previous_day = data_json_daily[0][1]
    e_sum_midnight = data_json_daily[1][1]  # extracting data from json
    daily_consumption = e_sum_midnight - e_sum_previous_day

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    insert_sql(daily_consumption, yesterday)  # inserting data into the table


main()
