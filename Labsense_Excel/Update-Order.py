import pandas as pd
from datetime import datetime, timedelta, date

from Labsense_SQL.constants import gsk_2016

# `gsk_2016` moved to `Labsense_SQL.constants` to avoid duplication.

from Labsense_SQL.constants import to_litre

# `to_litre` moved to `Labsense_SQL.constants` to avoid duplication.

df = pd.read_excel("read-file-name.xlsx")  # add path ro file name you want to read from
# filter full sheet to retain only those with an entry in "CAS Number" column, define as "ord_chem"
df = df[df["CAS Number"].notnull()]


# filter CAS-restricted list to columns of use ("Full Name", "Volume/Weight/Size", "Unit", "Number", "CAS Number", "Date ordered"), define as "chemlist_red"
df = df.iloc[:, [0, 3, 4, 7, 8, 17]]
df["Date Ordered"] = pd.to_datetime(df["Date Ordered"], errors="coerce", dayfirst=True)
df = df.dropna(subset=["Date Ordered"])
date_7_days_ago = datetime.now() - timedelta(days=7)
current_date = datetime.now()
df_filtered = df[(df["Date Ordered"] >= date_7_days_ago)]
print(df_filtered)


for key, value in gsk_2016.items():
    ord_chem_cas = df.loc[df["CAS Number"] == value]
    if ord_chem_cas.empty:
        # print(f"No records for {key}")
        temp_sum = 0
    else:
        ord_chem_cas = ord_chem_cas.astype(
            {"Volume/Weight/Size": "float", "Number": "float"}
        )
        ord_chem_cas["Total Volume (L)"] = (
            ord_chem_cas["Volume/Weight/Size"]
            * ord_chem_cas["Number"]
            * ord_chem_cas["Unit"].map(to_litre)
        )  # converting total volume to litres
        temp = ord_chem_cas["Total Volume (L)"]
        temp_sum = temp.sum()
        print(f"{key}\n\n{value}\n{temp_sum}\n\n")
        writer = pd.ExcelWriter(
            "NewOrdersData.xlsx", mode="a", engine="openpyxl", if_sheet_exists="replace"
        )
        write_df = pd.read_excel("NewOrdersData.xlsx", sheet_name="Sheet1")
        newRow = {
            "CAS Number": value,
            "Name": key,
            "Volume": temp_sum,
            "Timestamp": date.today(),
        }
        new_row = pd.DataFrame([newRow])  # creating new dataframe row
        write_df = pd.concat([write_df, new_row], ignore_index=True)
        # Write the pandas dataframe to the excel file
        write_df.to_excel(writer, sheet_name="Sheet1", index=False)
        writer.close()
