import requests as req
import json
import pandas as pd
from authtoken import authtoken

#Example search using "Red" environmental hazards
ghs = req.post("https://app.cheminventory.net/api/search/execute",
                   json = {"authtoken": authtoken,
                           "inventory": 873,
                           "type": "ghs",
                           "contents": {
                                "searchtype": "or",
                                "items": ["H400", "H401", "H410", "H411", "H412", "H420", "H441"]}})
ghs_json_raw = ghs.json()
ghs_json_data = pd.json_normalize(ghs_json_raw ['data']['containers'])

ghs_df = pd.DataFrame(ghs_json_data)
print(ghs_df)

ghs_df_quant = ghs_df.iloc[:, [1, 3, 4, 5, 6, 7]]
print(ghs_df_quant)

#ghs_df_quant.to_json('ghs_test.json')