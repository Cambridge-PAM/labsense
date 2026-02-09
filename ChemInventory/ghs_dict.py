import requests as req
import json
import pandas as pd
import datetime
from authtoken import authtoken
from dict import hazard_groups

dt = datetime.datetime.now()
timestamp = dt.strftime("%Y%m%d-%H%M%S")

for key, value in hazard_groups.items():
    ghs = req.post(
        "https://app.cheminventory.net/api/search/execute",
        json={
            "authtoken": authtoken,
            "inventory": 873,
            "type": "ghs",
            "contents": {"searchtype": "or", "items": value},
        },
    )
    ghs_json_raw = ghs.json()
    ghs_json_data = pd.json_normalize(ghs_json_raw["data"]["containers"])

    ghs_df = pd.DataFrame(ghs_json_data)
    if ghs_df.empty:
        print(f"No records for {key}")  # escape to allow for a null return
    else:
        ghs_df_quant = ghs_df.iloc[
            :, [1, 2, 3, 4, 5, 6, 7]
        ]  # restrict columns to 'name', 'location', 'size', 'unitid', 'unit', 'substance', and 'cas'
        ghs_df_real = ghs_df_quant.loc[
            ghs_df_quant["location"] != 527895
        ]  # remove any entries in "Missing - Stockcheck Only" location

        if ghs_df_real.empty:
            print(
                f"No records for {key}"
            )  # second esacpe if null return after filtering
        else:
            print(f"{key}\n{ghs_df_real}\n\n")
