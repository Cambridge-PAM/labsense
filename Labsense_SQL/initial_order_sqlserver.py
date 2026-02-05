from datetime import date
import pandas as pd
import pyodbc

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

from Labsense_SQL.constants import gsk_2016
from Labsense_SQL.constants import to_litre

# `to_litre` moved to `Labsense_SQL.constants` to avoid duplication.


def insert_sql(cas, name, volume, datestamp):
    try:
        # Create a connection
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute(
            """
        IF 
        ( NOT EXISTS 
        (select object_id from sys.objects where object_id = OBJECT_ID(N'[chemOrders]') and type = 'U')
        )
        BEGIN
            CREATE TABLE chemOrders
            (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                CAS VARCHAR(15),
                Name VARCHAR(30),
                Volume REAL,
                Datestamp DATE
            )
        END
        """
        )  # create table

        cursor.execute(
            """
        INSERT INTO chemOrders (CAS,Name,Volume,Datestamp)
        VALUES (?,?,?,?)""",
            (cas, name, volume, datestamp),
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


def main():
    # import order sheet to be read, define as "df"
    df = pd.read_excel(
        "SPREADSHEET.xlsx", engine="openpyxl"
    )  # add the file you want to read from

    # filter full sheet to retain only those with an entry in "CAS Number" column, define as "ord_chem"
    df = df[df["CAS Number"].notnull()]

    # filter CAS-restricted list to columns of use ("Full Name", "Volume/Weight/Size", "Unit", "Number", "CAS Number", "Date ordered"), define as "chemlist_red"
    df = df.iloc[:, [0, 3, 4, 7, 8, 17]]

    new_df = pd.DataFrame(
        columns=["CAS Number", "Name", "Volume", "Timestamp"]
    )  # creating columns for data frame

    for key, value in gsk_2016.items():
        ord_chem_cas = df.loc[df["CAS Number"] == value]
        if ord_chem_cas.empty:
            print(f"No records for {key}\n")
            temp_sum = 0
        else:
            ord_chem_cas = ord_chem_cas.astype(
                {"Volume/Weight/Size": "float", "Number": "float"}
            )
            ord_chem_cas["Total Volume (L)"] = (
                ord_chem_cas["Volume/Weight/Size"]
                * ord_chem_cas["Number"]
                * ord_chem_cas["Unit"].map(to_litre)
            )  # finding total volume of a chemical-converted to litres
            temp = ord_chem_cas["Total Volume (L)"]
            temp_sum = temp.sum()
            print(f"{key} {value}\n{temp_sum}\n")  # for debugging, can be removed

        today = date.today()
        insert_sql(value, key, temp_sum, today)


main()
