import pandas as pd
 
to_litre={'µl':0.000001,
          'µL':0.000001,
          'ul':0.000001,
          'uL':0.000001,
          'ml':0.001,
          'mL':0.001,
          'l':1.0,
          'L':1.0,
          'µg':0.00000000125,
          'ug':0.00000000125,
          'mg':0.00000125,
          'g':0.00125,
          'kg':1.25,
          'oz':0.035436875,
          'lb':0.56699,
          'lbs':0.56699,
          'gal':4.54609}
 
 
df = pd.read_excel("Waste Master.xlsx") #file you need to read from, make sure it's in the same folder as this python script

df['Unnamed: 0'] = pd.to_datetime(df['Unnamed: 0']).dt.date
date_set=df['Unnamed: 0'].unique()
results = []
 
for date in date_set:
    sub_df=df[df['Unnamed: 0'] == date]
    print(f"Sub DataFrame for date {date}:")
    print(sub_df)
    for column in sub_df.columns:
        if column.startswith('HP') and sub_df[column].dtype in ['int64', 'float64']:
            column_sum = (sub_df[column] * sub_df["Unnamed: 3"] * sub_df['Unnamed: 4'].map(to_litre)).sum()
            results.append({'Date': date, 'HP Number': column, 'Volume(L)': column_sum})
            print(f"Sum of column '{column}' for date {date}: {column_sum}")
 
result_df = pd.DataFrame(results)
file_name = 'NewWasteSheet.xlsx'
result_df.to_excel(file_name)
 
 
