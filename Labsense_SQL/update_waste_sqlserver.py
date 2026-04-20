"""Read a waste spreadsheet and upsert recent waste records into SQL Server."""

import os
from datetime import datetime, timedelta, date
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
import pyodbc

from Labsense_SQL.constants import to_litre

# Load environment variables from Labsense_SQL/.env
load_dotenv(Path(__file__).resolve().parent / ".env")

# Connection information (from Labsense_SQL/.env)
sqlServerName = os.getenv("SQL_SERVER", "MSM-FPM-70203\\LABSENSE")
databaseName = os.getenv("SQL_DATABASE", "labsense")
trusted_connection = os.getenv("SQL_TRUSTED_CONNECTION", "yes")
encryption_pref = os.getenv("SQL_ENCRYPTION", "Optional")
# Connection string
connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sqlServerName};"
    f"DATABASE={databaseName};"
    f"Trusted_Connection={trusted_connection};"
    f"Encrypt={encryption_pref}"
)


def insert_sql(hp, volume, datestamp):
    try:
        # Create a connection
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute(
            """
        IF 
        ( NOT EXISTS 
        (select object_id from sys.objects where object_id = OBJECT_ID(N'[chemWaste]') and type = 'U')
        )
        BEGIN
            CREATE TABLE chemWaste
            (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                HP VARCHAR(4),
                Volume REAL,
                Datestamp DATE
            )
        END
        """
        )  # create table

        cursor.execute(
            """
        INSERT INTO chemWaste (HP,Volume,Datestamp)
        VALUES (?,?,?)""",
            (hp, volume, datestamp),
        )  # insert into table

        # cursor.execute('SELECT * FROM chemOrders')
        # rows = cursor.fetchall()

        # column_names = [description[0] for description in cursor.description]
        # print(f"{column_names}")
        # for row in rows:
        #    print(row)  #printing table, for debugging purposes
        connection.commit()
        connection.close()

    except pyodbc.Error as ex:
        print("An error occurred in SQL Server:", ex)


df = pd.read_excel("Waste Master.xlsx")  # add path to the file you need to read from

df["Unnamed: 0"] = pd.to_datetime(df["Unnamed: 0"], errors="coerce", dayfirst=True)
df = df.dropna(subset=["Unnamed: 0"])
date_30_days_ago = datetime.now() - timedelta(days=30)
current_date = datetime.now()
df_filtered = df[
    (df["Unnamed: 0"] >= date_30_days_ago)
]  # selects more recent data(at most 30 days ago)

# df_filtered['Unnamed: 0'] = pd.to_datetime(df_filtered['Unnamed: 0']).dt.date
date_set = df_filtered["Unnamed: 0"].unique()
print(date_set)
results = []

for date in date_set:
    sub_df = df[df["Unnamed: 0"] == date]
    # print(f"Sub DataFrame for date {date}:")
    # print(sub_df)
    for column in sub_df.columns:
        if column.startswith("HP") and sub_df[column].dtype in ["int64", "float64"]:
            column_sum = (
                sub_df[column]
                * sub_df["Unnamed: 3"]
                * sub_df["Unnamed: 4"].map(to_litre)
            ).sum()
            insert_sql(column, column_sum, date)
            print(f"Sum of column '{column}' for date {date}: {column_sum}")
