import asyncio
import time
import schedule
import time
import numpy as np
from datetime import datetime
import VL53L1X # distance sensor
from ltr559 import LTR559 # light & proximity sensor
import paho.mqtt.publish as publish

MQTT_SERVER = "10.253.179.46"#specify the broker address,in this case the IP address of the computer
MQTT_PATH = "fumehood"

## DISTANCE SENSOR

# Open and start the VL53L1X sensor.
# If you've previously used change-address.py then you
# should use the new i2c address here.
# If you're using a software i2c bus (ie: HyperPixel4) then
# you should `ls /dev/i2c-*` and use the relevant bus number.
tof = VL53L1X.VL53L1X(i2c_bus=1, i2c_address=0x29)
tof.open()

# Optionally set an explicit timing budget
# These values are measurement time in microseconds,
# and inter-measurement time in milliseconds.
# If you uncomment the line below to set a budget you
# should use `tof.start_ranging(0)`
# tof.set_timing(66000, 70)
tof.start_ranging(1)  # Start ranging
                      # 0 = Unchanged
                      # 1 = Short Range
                      # 2 = Medium Range
                      # 3 = Long Range

## LIGHT SENSOR
ltr559 = LTR559()

## AIR FLOW SENSOR
# to be written

async def main():
    # record data
    labID=1
    sublabID=3
    time_send=datetime.now()
    distance = tof.get_distance() #in mm
    ltr559.update_sensor()
    lux = ltr559.get_lux()
    airflow=0.0
    
    print(f"The fumehood measurements at {time_send} are: {distance} mm, {lux} lux and {airflow} flow.")
    # Send a message
    msg_payload=str({"labId":labID,"sublabId":sublabID,"sensorReadings":{"fumehood":{"distance":float(distance),"light":float(lux),"airflow":float(airflow)}}, "measureTimestamp":time_send.strftime('%Y-%m-%d %H:%M:%S')})    
    publish.single(MQTT_PATH, msg_payload, hostname=MQTT_SERVER) #send data
    print("Message successfully sent!")
    time.sleep(10)
    
def job():
    print("Job started")
    asyncio.run(main())
    print("job finished")

if __name__ == "__main__":
    asyncio.run(main())
    schedule.every(1).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
