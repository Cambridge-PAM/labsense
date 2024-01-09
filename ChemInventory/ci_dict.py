import requests as req
import pandas as pd
import datetime
from authtoken import authtoken
from dict import solvent_cas

dt=datetime.datetime.now()
timestamp=dt.strftime("%Y%m%d-%H%M%S")

for key, value in solvent_cas.items():
    ci = req.post("https://app.cheminventory.net/api/search/execute",
                   json = {"authtoken": authtoken,
                           "inventory": 873,
                           "type": "cas",
                           "contents": value})
    ci_json_raw = ci.json()
    ci_json_data = pd.json_normalize(ci_json_raw ['data']['containers'])

    ci_df = pd.DataFrame(ci_json_data)
    if ci_df.empty:
        print(f"No records for {key}") #escape to allow for a null return
    else:
        ci_df_quant = ci_df.iloc[:, [1, 2, 3, 4, 5, 6, 7, 19]] #restrict columns to 'name', 'location', 'size', 'unitid', 'unit', 'substance', 'cas', and 'locationtext'
        ci_df_real = ci_df.loc[ci_df["location"]!=527895] #remove any entries in "Missing - Stockcheck Only" location
        if ci_df_real.empty:
            print(f"No records for {key}") #second esacpe if null return after filtering
        else:
            print(f"{key}\n{ci_df_real}\n\n")