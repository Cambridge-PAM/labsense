import paho.mqtt.client as mqtt #import library
import json
from openpyxl import Workbook, load_workbook
import os
import pandas as pd
import sqlite3

MQTT_SERVER = "172.28.52.169" #specify the broker address, it can be IP of raspberry pi or simply localhost
MQTT_PATH = "test_channel" #this is the name of topic, like temp
EXCEL_FILE = "CPUTemperature.xlsx"

def insert_sql(temperature, timestamp):
    conn = sqlite3.connect('labsense.db',timeout=10)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS test (
        id INTEGER PRIMARY KEY,
        Temperature REAL NOT NULL,
        Timestamp DATETIME NOT NULL
    )
    ''')
    
    cursor.execute('''
    INSERT INTO test (Temperature, Timestamp)
    VALUES (?,?)''',(temperature,timestamp))

    conn.commit()
    
    cursor.execute('SELECT * FROM test')
    rows = cursor.fetchall()
    
    conn.close()

 
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
 
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_PATH)
 

def on_message(client, userdata, msg): 
    try: 
        message = msg.payload.decode('utf-8') 
        message=message.replace("'",'"')
        data = json.loads(message) 
        sensor = data.get('Temp') 
        value = data.get('Time') 
        print(f"Received message: Temp={sensor}, Time={value}") 
        insert_sql(sensor,value)
    except json.JSONDecodeError as e: 
        print(f"Failed to decode JSON message: {e}")
 
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_SERVER,1883,60)
client.loop_forever()# use this line if you don't want to write any further code. It blocks the code forever to check for data
#client.loop_start()  #use this line if you want to write any more 