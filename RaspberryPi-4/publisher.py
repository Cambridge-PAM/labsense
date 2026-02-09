import paho.mqtt.publish as publish
import time
import os
from gpiozero import CPUTemperature
from datetime import datetime


MQTT_SERVER = "172.28.52.169"
MQTT_PATH = "test_channel"

while True:
    cpu = CPUTemperature()
    time_send = datetime.now()
    cpu_msg = str(
        {"Temp": cpu.temperature, "Time": time_send.strftime("%Y-%m-%d %H:%M:%S")}
    )
    publish.single(
        MQTT_PATH, cpu_msg, hostname=MQTT_SERVER
    )  # send data continuously every 3 seconds
    print("Message sent")
    time.sleep(10)
