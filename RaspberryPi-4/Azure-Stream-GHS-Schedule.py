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

solvent_cas={"dcm":"75-09-2",
               "chcl3":"67-66-3",
               "meoh":"67-56-1",
               "etoh":"64-17-5",
               "tol":"108-88-3",
               "etoac":"141-78-6",
               "thf":"109-99-9",
               "dmf":"68-12-2",
               "et2o":"60-29-7",
               "ipa":"67-63-0",
               "hex":"101-54-3",
               "pet4060":"64742-49-0",
               "acet":"67-64-1",
               "mecn":"75-05-8",
               "diox":"123-91-1"}

def create_uuid(val1,val2,val3):
    concat_string=str(val1)+str(val2)+str(val3)
    hex_string = hashlib.md5(concat_string.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)

async def main():
    # Do ChemInventory processing
    for key, value in solvent_cas.items():
        ci = req.post("https://app.cheminventory.net/api/search/execute",
                    json = {"authtoken": os.getenv("CHEMINVENTORY_CONNECTION_STRING"),
                            "inventory": 873,
                            "type": "cas",
                            "contents": value})
        ci_json_raw = ci.json()
        ci_json_data = pd.json_normalize(ci_json_raw ['data']['containers'])
        ci_df = pd.DataFrame(ci_json_data)

        if ci_df.empty:
            print(f"No records for {key}") #escape to allow for a null return
            temp_sum=0
        else:
            ci_df_real = ci_df.loc[ci_df["location"]!=527895] #remove any entries in "Missing - Stockcheck Only" location
            if ci_df_real.empty:
                print(f"No records for {key}") #second esacpe if null return after filtering
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
        vol=[str(key),temp_sum]
        time_send=datetime.now()
        
        msg_output='electricity'
        msg_id=str(create_uuid(time_send,labID,sublabID))
        msg_payload=str({"labId":labID,"sublabId":sublabID,"sensorReadings":{"chem":vol}, "measureTimestamp":time_send.strftime('%Y-%m-%d %H:%M:%S')})
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