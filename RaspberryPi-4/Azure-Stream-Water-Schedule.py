import os
import asyncio
import time
import schedule
import uuid
import hashlib
import numpy as np
import threading
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
import RPi.GPIO as GPIO

def create_uuid(val1,val2,val3):
    concat_string=str(val1)+str(val2)+str(val3)
    hex_string = hashlib.md5(concat_string.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)

# set up GPIO
FLOW_SENSOR_GPIO = 4
LED_GPIO = 2
FLOW_RATE_FACTOR = 5  # value based on the sensor's specification

global count
count=0

global temp_volume_val
temp_volume_val=0

global total_volume_val
total_volume_val=0

GPIO.setmode(GPIO.BCM)
GPIO.setup(FLOW_SENSOR_GPIO, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(LED_GPIO, GPIO.OUT)
GPIO.setwarnings(True)

def countPulse(channel):
    global count
    count=count+1

GPIO.add_event_detect(FLOW_SENSOR_GPIO, GPIO.FALLING, callback=countPulse)

def gpio_monitoring():
    while True:
        count=0
        time.sleep(1)
        vol=(1000*count)/(FLOW_RATE_FACTOR*60)
        print(f"The current volume is: {vol} mL")
        count=0
        global temp_volume_val
        temp_volume_val=vol

# Start the background gpio_monitoring thread
thread = threading.Thread(target=gpio_monitoring)
thread.start()

# Function to sum water volume over a set time period in seconds
def total_volume(interval):
    vol_arr=[]
    GPIO.output(LED_GPIO,GPIO.HIGH)
    for i in range (0,interval):
        time.sleep(1)
        vol_arr.append(temp_volume_val)
    GPIO.output(LED_GPIO,GPIO.LOW)
    global total_volume_val
    total_volume_val=np.sum(vol_arr)
    
async def main():
    # record data
    labID=1
    sublabID=3
    total_volume(5)
    global total_volume_val
    water=total_volume_val
    time_send=datetime.now()
    print(f"The summed volume is: {total_volume_val} L")

    # Fetch the connection string from an environment variable
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

    # Create instance of the device client using the authentication provider
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    await device_client.connect()

    # Send a message
    msg_output='water'
    msg_id=str(create_uuid(time_send,labID,sublabID))
    msg_payload=str({"labId":labID,"sublabId":sublabID,"sensorReadings":{"water":water}, "measureTimestamp":time_send.strftime('%Y-%m-%d %H:%M:%S')})
    msg=Message(msg_payload,message_id=msg_id,output_name=msg_output)
    await device_client.send_message(msg)
    print("Message successfully sent!")

    await device_client.disconnect()
    
def job():
    print("Job started")
    asyncio.run(main())
    global temp_volume_val
    temp_volume_val=0
    global total_volume_val
    total_volume_val=0
    print("job finished")

if __name__ == "__main__":
    schedule.every(0.1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)