import paho.mqtt.client as mqtt #import library
import json
import pyodbc

MQTT_SERVER = "10.253.179.46" #specify the broker address, in this case the IP address of the computer
TOPICS = ["water", "fumehood"] #this is the name of topic, like water

#Connection information
# Your SQL Server instance
sqlServerName = 'MSM-FPM-70203\\LABSENSE'
#Your database
databaseName = 'labsense'
# Use Windows authentication
trusted_connection = 'yes'
# Encryption
encryption_pref = 'Optional'
# Connection string information
connection_string = (
f"DRIVER={{ODBC Driver 18 for SQL Server}};"
f"SERVER={sqlServerName};"
f"DATABASE={databaseName};"
f"Trusted_Connection={trusted_connection};"
f"Encrypt={encryption_pref}"
)

def insert_sql_water(labId, sublabId, water, timestamp):
    if water is None:
        water = 0.0 #ensures water isn't null before inserting into table
    try:
        # Create a connection
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute('''
        IF 
        ( NOT EXISTS 
        (select object_id from sys.objects where object_id = OBJECT_ID(N'[water]') and type = 'U')
        )
        BEGIN
            CREATE TABLE water
            (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                LabId INTEGER,
                SublabId INTEGER,           
                Water REAL,
                Timestamp DATETIME
            )
        END
        ''') # create table

        cursor.execute('''
        INSERT INTO water (LabId,SublabId, Water, Timestamp)
        VALUES (?,?,?,?)''',(labId, sublabId, water,timestamp)) #insert into water table
        
        #cursor.execute('SELECT * FROM water')
        #rows = cursor.fetchall()
        
        #column_names = [description[0] for description in cursor.description]
        #print(f"{column_names}")
        # Print each row
        #for row in rows:
        #    print(row) #view table
        connection.commit()
        connection.close()
     
    except pyodbc.Error as ex:
        print("An error occurred in SQL Server:", ex)

def insert_sql_fumehood(labId,sublabId,distance,light,airflow,timestamp):
    if distance is None: #ensures distance isn't null before inserting into table
        distance=0.0
    if light is None: #ensures light isn't null before inserting into table
        light=0.0
    if airflow is None: #ensures airflow isn't null before inserting into table
        airflow=0.0

    try:
        # Create a connection
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute('''
        IF 
        ( NOT EXISTS 
        (select object_id from sys.objects where object_id = OBJECT_ID(N'[fumehood]') and type = 'U')
        )
        BEGIN
            CREATE TABLE fumehood
            (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                LabId INTEGER,
                SublabId INTEGER,           
                Distance REAL,
                Light REAL,
                Airflow REAL,
                Timestamp DATETIME
            )
        END
        ''') # create table

        cursor.execute('''
        INSERT INTO fumehood (LabId,SublabId, Distance, Light, Airflow, Timestamp)
        VALUES (?,?,?,?,?,?)''',(labId, sublabId, distance,light,airflow,timestamp))
        
        #cursor.execute('SELECT * FROM fumehood')
        #rows = cursor.fetchall()
        #column_names = [description[0] for description in cursor.description]
        #print(f"{column_names}")
        # Print each row
        #for row in rows:
        #    print(row)
        connection.commit()
        connection.close()
     
    except pyodbc.Error as ex:
        print("An error occurred in SQL Server:", ex)
 
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
 
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for topic in TOPICS:    
        client.subscribe(topic)
 
def on_message(client, userdata, msg): 
    try: 
        print(msg.payload.decode('utf-8') ) #view the message being sent, used for debugging and can be removed
        message = msg.payload.decode('utf-8') 
        message=message.replace("'",'"') #modify the message so that it can be converted to a json dictionary-equivalent
        data = json.loads(message) 
        labId=data.get('labId')
        sublabId=data.get('sublabId')
        timestamp=data.get('measureTimestamp')
        sensorReadings=data.get('sensorReadings') #extract necessary data from the message
        if "water" in sensorReadings: #checks where the message is coming from
            water = sensorReadings.get('water') 
            print("water in ml:",water)#for degugging, can be removed
            water_litres=float(water)/1000
            print("water in litres:",water_litres)
            insert_sql_water(labId, sublabId, water_litres, timestamp) #inserts data we just extracted into the table

        if "fumehood" in sensorReadings: #checks where the message is coming from
            temp=sensorReadings['fumehood']
            distance=temp['distance']
            light=temp['light']
            airflow=temp['airflow']
            print(distance,light,airflow)
            insert_sql_fumehood(labId,sublabId,distance,light,airflow,timestamp)#inserts data we just extracted into the table

    except json.JSONDecodeError as e: 
        print(f"Failed to decode JSON message: {e}")
 
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_SERVER,1883,60) #connects to the mqtt server, on port 1883 and timeout of 60s
client.loop_forever()# use this line if you don't want to write any further code. It blocks the code forever to check for data
#client.loop_start()  #use this line if you want to write any more 