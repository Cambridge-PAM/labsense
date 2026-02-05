import pandas as pd
from datetime import datetime, timedelta, date

from Labsense_SQL.constants import gsk_2016
# `gsk_2016` moved to `Labsense_SQL.constants` to avoid duplication.

from Labsense_SQL.constants import to_litre
# `to_litre` moved to `Labsense_SQL.constants` to avoid duplication.

df = pd.read_excel("read-file-name.xlsx") #add path to the file you need to read from

df['Unnamed: 0'] = pd.to_datetime(df['Unnamed: 0'], errors='coerce',dayfirst=True)
df = df.dropna(subset=['Unnamed: 0'])
date_30_days_ago = datetime.now() - timedelta(days=30)
current_date=datetime.now()
df_filtered = df[(df['Unnamed: 0'] >= date_30_days_ago)] #selects more recent data(at most 30 days ago)

#df_filtered['Unnamed: 0'] = pd.to_datetime(df_filtered['Unnamed: 0']).dt.date
date_set=df_filtered['Unnamed: 0'].unique()
print(date_set)
results = []

for date in date_set:
    sub_df=df[df['Unnamed: 0'] == date]
    print(f"Sub DataFrame for date {date}:")
    print(sub_df)
    for column in sub_df.columns:
        if column.startswith('HP') and sub_df[column].dtype in ['int64', 'float64']:
            column_sum = (sub_df[column] * sub_df["Unnamed: 3"] * sub_df['Unnamed: 4'].map(to_litre)).sum()
            writer = pd.ExcelWriter('NewOrdersData.xlsx',mode='a',engine="openpyxl",if_sheet_exists="replace")
            write_df=pd.read_excel('NewOrdersData.xlsx', sheet_name="Sheet1")
            newRow= {"Date":date,"HP Number":column,"Volume(L)":column_sum}
            new_row=pd.DataFrame([newRow])
            write_df=pd.concat([write_df,new_row],ignore_index=True)
            # Write the pandas dataframe to the excel file
            write_df.to_excel(writer, sheet_name='Sheet1',index=False)
            writer.close()
