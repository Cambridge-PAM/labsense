import requests as req
import pandas as pd
from authtoken import authtoken

# Initial info request for account information, required to obtain Inventory ID for later use
# TODO Move "authtoken" to secrets.py and link to prevent direct reveal.
ci_info = req.post(
    "https://app.cheminventory.net/api/general/getdetails",
    json={"authtoken": authtoken},
)
ci_info_raw = ci_info.json()
ci_info_df = pd.DataFrame(ci_info_raw["data"])
ci_info_df_compact = ci_info_df.iloc[[4, 5, 3], 0]
print(ci_info_df_compact)

# Request to list all physical locations in ChemInventory records
ci_locs = req.post(
    "https://app.cheminventory.net/api/location/load", json={"authtoken": authtoken}
)
ci_locs_raw = ci_locs.json()
ci_locs_df = pd.DataFrame(ci_locs_raw["data"])
print(ci_locs_df)

# Example query, using Inventory ID obtained from ci_info
# TODO Directly link "inventory" to result from ci_info, to allow for simple user & inventory change.
ci = req.post(
    "https://app.cheminventory.net/api/search/execute",
    json={
        "authtoken": authtoken,
        "inventory": 873,
        "type": "cas",
        "contents": "75-09-2",
    },
)
ci_json_raw = ci.json()
ci_json_data = pd.json_normalize(ci_json_raw["data"]["containers"])

ci_df = pd.DataFrame(ci_json_data)
print(ci_df)

ci_df_real = ci_df.loc[ci_df["location"] != 527895]
print(ci_df_real)


print(ci_df.columns)
