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


def create_uuid(val1, val2, val3):
    concat_string = str(val1) + str(val2) + str(val3)
    hex_string = hashlib.md5(concat_string.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)


async def main():
    # import order sheet to be read, define as "df"
    ord = pd.read_excel("/home/labsense1/Documents/Evans Group Ordering Sheet.xlsx")

    # filter full sheet to retain only those with an entry in "CAS Number" column, define as "ord_chem"
    ord_chem = ord[ord["CAS Number"].notnull()]

    # filter CAS-restricted list to columns of use ("Full Name", "Volume/Weight/Size", "Unit", "Number", "CAS Number", "Date ordered"), define as "chemlist_red"
    ord_chem_red = ord_chem.iloc[:, [0, 3, 4, 7, 8, 16]]
    print(ord_chem_red)

    for key, value in gsk_2016.items():
        ord_chem_cas = ord_chem_red.loc[ord_chem_red["CAS Number"] == value]
        if ord_chem_cas.empty:
            print(f"No records for {key}")
            temp_sum = 0
        else:
            ord_chem_cas = ord_chem_cas.astype(
                {"Volume/Weight/Size": "float", "Number": "float"}
            )
            ord_chem_cas["Total Volume (L)"] = (
                ord_chem_cas["Volume/Weight/Size"] * ord_chem_cas["Number"]
            )
            temp = ord_chem_cas["Total Volume (L)"]
            temp_sum = temp.sum()
            print(f"{key}\n{temp_sum}\n\n")

        # Fetch the connection string from an environment variable
        conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

        # Create instance of the device client using the authentication provider
        device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
        await device_client.connect()

        # Send a message
        labID = 1
        sublabID = 3
        vol = [str(key), temp_sum]
        time_send = datetime.now()

        msg_output = "order"
        msg_id = str(create_uuid(time_send, labID, sublabID))
        msg_payload = str(
            {
                "labId": labID,
                "sublabId": sublabID,
                "sensorReadings": {"order": vol},
                "measureTimestamp": time_send.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        msg = Message(msg_payload, message_id=msg_id, output_name=msg_output)
        await device_client.send_message(msg)
        print("Message successfully sent!")

        await device_client.shutdown()

        time.sleep(10)


def job():
    print("Job started")
    asyncio.run(main())
    print("job finished")


if __name__ == "__main__":
    schedule.every(1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
