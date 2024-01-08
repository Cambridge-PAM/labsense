import os
import asyncio
import time
import schedule
import uuid
import hashlib
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient

def create_uuid(val1,val2,val3):
    concat_string=str(val1)+str(val2)+str(val3)
    hex_string = hashlib.md5(concat_string.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)

async def main():
    # Fetch the connection string from an environment variable
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

    # Create instance of the device client using the authentication provider
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    await device_client.connect()

    # Send a message
    labID=1
    sublabID=1
    distance=15
    light=0.9
    time_send=datetime.now()
    messageId=str(create_uuid(time_send,labID,sublabID))
    msg=str({"messageId":messageId,"labId":labID,"sublabId":sublabID,"sensorReadings":{"distance":distance,"light":light}, "measureTimestamp":time_send.strftime('%Y-%m-%d %H:%M:%S')})
    await device_client.send_message(msg)
    print("Message successfully sent!")

    await device_client.shutdown()

def job():
    print("Job started")
    asyncio.run(main())
    print("job finished")

if __name__ == "__main__":
    schedule.every(1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)