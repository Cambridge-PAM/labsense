import os
import asyncio
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
from gpiozero import CPUTemperature

import requests as req
import json
import pandas as pd
from authtoken import authtoken


async def main():
    # Example search using "Red" environmental hazards
    ghs = req.post(
        "https://app.cheminventory.net/api/search/execute",
        json={
            "authtoken": authtoken,
            "inventory": 873,
            "type": "ghs",
            "contents": {
                "searchtype": "or",
                "items": ["H400", "H401", "H410", "H411", "H412", "H420", "H441"],
            },
        },
    )
    ghs_json_raw = ghs.json()
    ghs_json_data = pd.json_normalize(ghs_json_raw["data"]["containers"])

    ghs_df = pd.DataFrame(ghs_json_data)

    ghs_df_quant = ghs_df.iloc[:, [1, 3, 4, 5, 6, 7]]

    # Fetch the connection string from an environment variable
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

    # Create instance of the device client using the authentication provider
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)

    # Connect the device client.
    await device_client.connect()

    # Send a single message
    print("Sending message...")
    cpu = CPUTemperature()
    time_send = datetime.now()
    cpu_msg = str(
        {"GHS": ghs_df_quant, "Time": time_send.strftime("%Y-%m-%d %H:%M:%S")}
    )
    await device_client.send_message(cpu_msg)
    print("Message successfully sent!")

    # finally, shut down the client
    await device_client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
