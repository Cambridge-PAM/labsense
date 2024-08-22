import asyncio
import time
import serial
import requests as req
import pandas as pd
import serial.tools.list_ports as port_list
import board
import busio
import adafruit_character_lcd.character_lcd_rgb_i2c as character_lcd
from authtoken import authtoken

async def main():
        #Uncomment next line to identify available ports
        #print(port_list)
        i2c = busio.I2C(board.SCL, board.SDA)
        lcd_columns = 16
        lcd_rows = 2
        
        lcd = character_lcd.Character_LCD_RGB_I2C(i2c, lcd_columns, lcd_rows)
        lcd.clear()
        lcd.message = "Please\n Scan Barcode\n"
        #Script pauses and prompts to scan barcode
        barcode=input("Please scan barcode")
        print(barcode)
        lcd.message = "Please\n Weigh Chemical\n"

        #Request search from ChemInventory for scanned barcode
        ci = req.post("https://app.cheminventory.net/api/search/execute",
                        json = {"authtoken": authtoken,
                                "inventory": 873,
                                "type": "barcode",
                                "contents": barcode})
        
        #Convert JSON output to dataframe, isolate unique container "ID" and convert to string
        ci_json_raw = ci.json()
        ci_json_data = pd.json_normalize(ci_json_raw ['data']['containers'])
        print(ci_json_data)
        ci_df = pd.DataFrame(ci_json_data)
        id=ci_df.iloc[0,0]
        id_str=str(id)
        
        #Open serial port to balance, settings for Denver Instruments SI-2002, adjust as required
        ser = serial.Serial(
                port='/dev/ttyUSB0',
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
                weight_split=weight_decode.split()
                weight=weight_split[1]
                print(weight)
                break

        

        #Send edit request to ChemInventory for saved "ID", edit feld "current weight" to recorded mass and save
        cf = req.post("https://app.cheminventory.net/api/container/information/save",
                        json = {"authtoken": authtoken,
                                "containerid":id_str,
                                "field":"cf-7317",
                                "newvalue":weight})

        #Print confirmation to user that scanned barcode mass has been updated
        print(f'Container {barcode} updated')
        lcd.message = "Success\n Weight updated\n"
        time.sleep(2)

while True:
        asyncio.run(main())
