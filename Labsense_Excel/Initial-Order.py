from datetime import date
import pandas as pd

from Labsense_SQL.constants import gsk_2016

# `gsk_2016` moved to `Labsense_SQL.constants` to avoid duplication.

from Labsense_SQL.constants import to_litre

# `to_litre` moved to `Labsense_SQL.constants` to avoid duplication.


def main():
    # import order sheet to be read, define as "df"
    df = pd.read_excel(
        "read-file-name.xlsx", engine="openpyxl"
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
            )  # finding total volume of a chemical-converted to litres
            temp = ord_chem_cas["Total Volume (L)"]
            temp_sum = temp.sum()
            # print(f"{key}\n\n{value}\n{temp_sum}\n\n") #for debugging, can be removed
            today = date.today()
            next_index = len(new_df)
            new_df.loc[next_index] = [
                value,
                key,
                temp_sum,
                today,
            ]  # adding new row to dataframe
    file_name = "NewOrdersData.xlsx"  # excel sheet file name you want to save to
    new_df.to_excel(file_name)  # converting dataframe to excel sheet


main()
