import serial
import requests as req
import pandas as pd
from authtoken import authtoken
import serial.tools.list_ports as port_list

#Script pauses and prompts to scan barcode
barcode=input("Please scan barcode")

#Open serial port to balance, settings for Denver Instruments SI-2002, adjust as required
ser = serial.Serial(
 port='COM3',
 baudrate = 1200,
 parity=serial.PARITY_ODD,
 stopbits=serial.STOPBITS_ONE,
 bytesize=serial.SEVENBITS,
 timeout=20
)
#Clear any data sitting in balance output buffer (likely not required, but ensures clear starting point)
ser.reset_output_buffer

#Wait for data from balance, decode from bytes to unicode, then split output into "number" and "unit" strings
while 1:
 x=ser.readline()
 weight_decode=(x.decode())
 weight=weight_decode.split()
 print(weight)
 break

#Request search from ChemInventory for scanned barcode
ci = req.post("https://app.cheminventory.net/api/search/execute",
                   json = {"authtoken": authtoken,
                           "inventory": 873,
                           "type": "barcode",
                           "contents": barcode})

#Convert JSON output to dataframe, isolate unique container "ID" and convert to string
ci_json_raw = ci.json()
ci_json_data = pd.json_normalize(ci_json_raw ['data']['containers'])
ci_df = pd.DataFrame(ci_json_data)
id=ci_df.iloc[0,0]
id_str=str(id)

#Send edit request to ChemInventory for saved "ID", edit feld "current weight" to recorded mass and save
cf = req.post("https://app.cheminventory.net/api/container/information/save",
                   json = {"authtoken": authtoken,
                           "containerid":id_str,
                           "field":"cf-7317",
                           "newvalue":weight})

#Print confirmation to user that scanned barcode mass has been updated
print(f'Container {barcode} updated')