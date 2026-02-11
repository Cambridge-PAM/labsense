import asyncio
import time
import schedule
import numpy as np
import threading
from datetime import datetime
import RPi.GPIO as GPIO
import paho.mqtt.publish as publish
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# set up GPIO
FLOW_SENSOR_GPIO_1 = 4
FLOW_SENSOR_GPIO_2 = 17
LED_GPIO = 2
FLOW_RATE_FACTOR = 5  # value based on the sensor's specification
MQTT_SERVER = os.getenv(
    "MQTT_SERVER"
)  # specify the broker address,in this case the IP address of the computer
MQTT_PATH = "water"

global count
count = 0

global temp_volume_val
temp_volume_val = 0

global total_volume_val
total_volume_val = 0

GPIO.setmode(GPIO.BCM)
GPIO.setup(FLOW_SENSOR_GPIO_1, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(FLOW_SENSOR_GPIO_2, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(LED_GPIO, GPIO.OUT)
GPIO.setwarnings(True)


def countPulse(channel):
    global count
    count = count + 1


GPIO.add_event_detect(FLOW_SENSOR_GPIO_1, GPIO.FALLING, callback=countPulse)
GPIO.add_event_detect(FLOW_SENSOR_GPIO_2, GPIO.FALLING, callback=countPulse)


def gpio_monitoring():
    while True:
        global count
        count = 0
        time.sleep(1)
        vol = (1000 * count) / (FLOW_RATE_FACTOR * 60)  # volume in mL
        # print(f"The current volume is: {vol} mL")
        count = 0
        global temp_volume_val
        temp_volume_val = vol


# Start the background gpio_monitoring thread
thread = threading.Thread(target=gpio_monitoring)
thread.start()


# Function to sum water volume over a set time period in seconds
def total_volume(interval):
    vol_arr = []
    GPIO.output(LED_GPIO, GPIO.HIGH)
    for i in range(0, interval):
        time.sleep(1)
        vol_arr.append(temp_volume_val)
    GPIO.output(LED_GPIO, GPIO.LOW)
    global total_volume_val
    total_volume_val = np.sum(vol_arr)


async def main():
    while True:
        # record data
        labID = 1
        sublabID = 3
        total_volume(5)
        global total_volume_val
        water = total_volume_val
        time_send = datetime.now()
        print(f"The summed volume is: {total_volume_val} mL")
        if total_volume_val > 0.0:
            # Send a message
            msg_payload = str(
                {
                    "labId": labID,
                    "sublabId": sublabID,
                    "sensorReadings": {"water": float(water)},
                    "measureTimestamp": time_send.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            publish.single(MQTT_PATH, msg_payload, hostname=MQTT_SERVER)  # send data
            print("Message successfully sent!")
        time.sleep(10)


def job():
    print("Job started")
    asyncio.run(main())
    global temp_volume_val
    temp_volume_val = 0
    global total_volume_val
    total_volume_val = 0
    print("job finished")


if __name__ == "__main__":
    asyncio.run(main())
    schedule.every(1).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
