import os
import asyncio
import time
import schedule
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
from gpiozero import CPUTemperature

async def main():
    # Fetch the connection string from an environment variable
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

    # Create instance of the device client using the authentication provider
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    await device_client.connect()

    # Send a message
    cpu=CPUTemperature()
    time_send=datetime.now()
    cpu_msg=str({"Temp":cpu.temperature, "Time":time_send.strftime('%Y-%m-%d %H:%M:%S')})
    await device_client.send_message(cpu_msg)
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