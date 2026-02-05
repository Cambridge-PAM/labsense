import os
import asyncio
import time
import schedule
import uuid
import hashlib
from datetime import datetime
import requests as req
import pandas as pd
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from Labsense_SQL.constants import gsk_2016
# `gsk_2016` moved to `Labsense_SQL.constants` to avoid duplication.

# need to redefine gsk_2016 with waste categories

def create_uuid(val1,val2,val3):
    concat_string=str(val1)+str(val2)+str(val3)
    hex_string = hashlib.md5(concat_string.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)

async def main():
    # Do ChemInventory processing
    for key, value in gsk_2016.items():
        ci = req.post("https://app.cheminventory.net/api/search/execute",
                    json = {"authtoken": os.getenv("CHEMINVENTORY_CONNECTION_STRING"),
                            "inventory": 873,
                            "type": "cas",
                            "contents": value})
        ci_json_raw = ci.json()
        ci_json_data = pd.json_normalize(ci_json_raw ['data']['containers'])
        ci_df = pd.DataFrame(ci_json_data)

        # format of DataFrame requires header of [wasteCategory, Volume (in L), hp1:hp15, pops] where hp1:hp15 and pops values are either 1 or 0
        if ci_df.empty:
            print(f"No records for {key}") #escape to allow for a null return
            temp_sum=0
        else:
            ci_df_real = ci_df.loc[ci_df["location"]!=527895] #remove any entries in "Missing - Stockcheck Only" location
            if ci_df_real.empty:
                print(f"No records for {key}") #second escape if null return after filtering
                temp_sum=0
            else:
                temp=pd.to_numeric(ci_df_real['size']) # convert string to float
                temp_sum=temp.sum()
                print(f"Total volume for {key} is {temp_sum}")
    
        # Fetch the connection string from an environment variable
        conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

        # Create instance of the device client using the authentication provider
        device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
        await device_client.connect()

        # Send a message
        labID=1
        sublabID=3
        vol=[str(key),temp_sum, hp1, hp1, hp3, hp4, hp5, hp6, hp7, hp8, hp9, hp10, hp11, hp12, hp13, hp14, hp15, pops]
        time_send=datetime.now()
        
        msg_output='waste'
        msg_id=str(create_uuid(time_send,labID,sublabID))
        msg_payload=str({"labId":labID,"sublabId":sublabID,"sensorReadings":{"waste":vol}, "measureTimestamp":time_send.strftime('%Y-%m-%d %H:%M:%S')})
        msg=Message(msg_payload,message_id=msg_id,output_name=msg_output)
        await device_client.send_message(msg)
        print("Message successfully sent!")

        await device_client.shutdown()

        time.sleep(2)

def job():
    print("Job started")
    asyncio.run(main())
    print("job finished")

if __name__ == "__main__":
    schedule.every(1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)