from urllib.request import urlopen
import json
import pyodbc
import time
import datetime 
url="http://10.247.40.36/feed/fetch.json?ids=20,21&apikey=APIKEY" #details about how url is formed can be found here https://emoncms.org/site/api#feed ,add your own APIKEY

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

def insert_sql(esum, psum,timestamp):
    try:
        # Create a connection
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute('''
        IF 
        ( NOT EXISTS 
        (select object_id from sys.objects where object_id = OBJECT_ID(N'[emoncms]') and type = 'U')
        )
        BEGIN
            CREATE TABLE emoncms 
            (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                Psum REAL,
                Esum REAL,
                Timestamp DATETIME
            )
        END
        ''') # create table
   
        cursor.execute('''
        INSERT INTO emoncms (Psum,Esum,Timestamp)
        VALUES (?,?,?)''',(psum,esum,timestamp)) #insert into table
 
        #cursor.execute('SELECT * FROM emoncms')
        #rows = cursor.fetchall()
        #column_names = [description[0] for description in cursor.description]
        #print(f"{column_names}")
        #for row in rows:
        #    print(row) # print table, used for debugging, can be removed
        connection.commit()
        connection.close()
     
    except pyodbc.Error as ex:
        print("An error occurred in SQL Server:", ex)

while True:
    midnight = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
    timestamp_seconds = int(midnight.timestamp())
    timestamp_milliseconds = timestamp_seconds * 1000 #finding UNIX_MILLISECOND timestamp at midnight
 
    current_timestamp_milliseconds = round(time.time()*1000) #finding current UNIX_MILLISECOND timestamp
 
    url_midnight="http://10.247.40.36/feed/data.json?id=21&start="+str(timestamp_milliseconds)+"&end="+str(current_timestamp_milliseconds)+"&mode=daily&apikey=APIKEY"  #details about how url is formed can be found here https://emoncms.org/site/api#feed , add your own APIKEY

    response_midnight=urlopen(url_midnight)
    data_json_midnight=json.loads(response_midnight.read())
    e_sum_midnight=data_json_midnight[0][1] #extracting elec_sum at midnight

    response=urlopen(url)
    data_json=json.loads(response.read())
    p_sum=data_json[0] #extracting current p_sum
    e_sum=data_json[1]-e_sum_midnight #current daily sum= current e_sum- e_sum at midnight
    timestamp=datetime.datetime.now() #finding current time to insert into table
    insert_sql(e_sum,p_sum,timestamp) #inserting new values into table
    print(e_sum,p_sum,timestamp) #for debugging purposes, can be removed
    time.sleep(60) #repeat every minute, can be changed