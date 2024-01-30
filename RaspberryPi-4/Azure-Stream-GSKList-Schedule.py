import os
import asyncio
import time
import schedule
import uuid
import hashlib
from datetime import datetime
import requests as req
import pandas as pd
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message

#Solvent:CAS dictionary derived from GSK 2016 solvent sustainability guide
gsk_2016 = {
    "Water":"7732-18-5",
    "Lactic Acid":"50-21-5",
    "Propionic Acid":"79-09-4",
    "Methanesulfonic Acid":"75-75-2",
    "Formic Acid":"64-18-6",
    "Acetic Acid (Glacial)":"64-19-7",
    "1,3-Propanediol":"504-63-2",
    "1-Pentanol":"71-41-0",
    "2-Ethyl Hexanol":"104-76-7",
    "1-Heptanol":"111-70-6",
    "Ethylene Glycol":"107-21-1",
    "Di(ethylene glycol)":"111-46-6",
    "Tri(ethylene glycol)":"112-27-6",
    "1,2-Propanediol":"57-55-6",
    "Benzyl Alcohol":"100-51-6",
    "Isoamyl Alcohol":"123-51-3",
    "1-Octanol":"111-87-5",
    "Glycerol":"56-81-5",
    "1,4-Butanediol":"110-63-4",
    "Cyclohexanol":"108-93-0",
    "Isobutanol":"78:83:1",
    "2-Pentanol":"6032-29-7",
    "1-Hexanol":"111-27-3",
    "1-Butanol":"71-36-3",
    "1-Propanol":"71-23-8",
    "Ethanol":"64-17-5",
    "2-Butanol":"78-92-2",
    "2-Propanol":"67-63-0",
    "t-Amyl Alcohol":"75-85-4",
    "1,2-Isopropylidene Glycerol":"100-79-8",
    "t-Butanol":"75-65-0",
    "IMS (Ethanol, Denatured)":"64-17-5",
    "Methanol":"67-56-1",
    "Tetrahydrofurfuryl Alcohol":"97-99-4",
    "2-Methoxyethanol":"109-86-4",
    "Glycerol Triacetate":"102-76-1",
    "Glycerol Diacetate":"111-55-7",
    "Isobutyl Acetate":"110-19-0",
    "Amyl Acetate":"628-63-7",
    "2-Ethylhexyl Acetate":"103-09-3",
    "Butyl Acetate":"123-86-4",
    "Methyl Oleate":"112-62-9",
    "Isoamyl Acetate":"123-92-2",
    "Isopropyl Acetate":"108-21-4",
    "Propyl Acetate":"109-60-4",
    "Dimethyl Succinate":"106-65-0",
    "n-Octyl Acetate":"112-14-1",
    "Ethyl Acetate":"141-78-6",
    "Ethyl Lactate":"97-64-3",
    "Diethyl Succinate":"123-25-1",
    "Dimethyl Adipate":"627-93-0",
    "gamma-Valerolactone":"108-29-2",
    "Diisopropyl Adipate":"6938-94-9",
    "Methyl Lactate":"547-64-8",
    "t-Butyl Acetate":"540-88-5",
    "Ethyl Formate":"109-94-4",
    "Methyl Acetate":"79-20-9",
    "Methyl Propionate":"554-12-1",
    "Ethyl Propionate":"105-37-3",
    "Methyl Formate":"107-31-3",
    "Propylene Carbonate":"108-32-7",
    "Ethylene Carbonate":"96-49-1",
    "Diethyl Carbonate":"105-58-8",
    "Dimethyl Carbonate":"616-38-6",
    "Butylene Carbonate":"4437-85-8",
    "Cyclopentanone":"120-92-3",
    "Cyclohexanone":"108-94-1",
    "3-Pentanone":"96-22-0",
    "Methylisobutyl Ketone":"108-10-1",
    "2-Pentanone":"107-87-9",
    "Methylethyl Ketone":"78-93-3",
    "Acetone":"67-64-1",
    "2,4,6-Collidine":"108-75-8",
    "Anisole":"100-66-3",
    "Ethoxybenzene":"103-73-1",
    "p-Xylene":"106-42-3",
    "Mesitylene":"108-67-8",
    "p-Cymene":"99-87-6",
    "Cumene":"98-82-8",
    "Toluene":"108-88-3",
    "Trifluorotoluene":"98-08-8",
    "Pyridine":"110-86-1",
    "Benzene":"71-43-2",
    "Isooctane":"540-84-1",
    "cis-Decalin":"493-01-6",
    "Heptane":"142-82-5",
    "L-Limonene":"5989-54-8",
    "Cyclohexane":"110-82-7",
    "D-Limonene":"5989-27-5",
    "Methylcyclohexane":"108-87-2",
    "Methylcyclopentane":"96-37-7",
    "Pentane":"109-66-0",
    "2-Methylpentane":"107-83-5",
    "Hexane":"110-54-3",
    "Petroleum Spirit":"8032-32-4",
    "Diethylene Glycol Monobutyl Ether":"112-34-5",
    "Dimethyl Isosorbide":"5306-85-4",
    "Dibutyl Ether":"142-96-1",
    "t-Amyl Methyl Ether":"994-05-8",
    "1,2,3-Trimethoxypropane":"20637-49-4",
    "Diphenyl Ether":"101-84-8",
    "t-Butyl Ethyl Ether":"637-92-3",
    "1,3-Dioxolane":"646-06-0",
    "Cyclopentyl Methyl Ether":"5614-37-9",
    "Diethoxymethane":"462-95-3",
    "2-Methyltetrahydrofuran":"96-47-9",
    "t-Butylmethyl Ether":"1634-04-4",
    "Diisopropyl Ether":"108-20-3",
    "Dimethoxymethane":"109-87-5",
    "Tetrahydrofuran":"109-99-9",
    "Bis(2-methoxyethyl)ether":"111-96-6",
    "1,4-Dioxane":"123-91-1",
    "Diethyl Ether":"60-29-7",
    "1,2-Dimethoxyethane":"110-71-4",
    "Dimethyl Ether":"115-10-6",
    "Dimethylpropylene Urea":"7226-23-5",
    "Dimethyl Sulphoxide":"67-68-5",
    "1,3-Dimethyl-2-imidazolidinone":"80-73-9",
    "Acetonitrile":"75-05-8",
    "Propanenitrile":"107-12-0",
    "Sulfolane":"126-33-0",
    "Formamide":"75-12-7",
    "N-Methyl Pyrrolidone":"872-50-4",
    "Dimethyl Acetamide":"127-19-5",
    "N-Ethylpyrrolidone":"2687-91-4",
    "N-Methylformamide":"123-39-7",
    "Dimethyl Formamide":"68-12-2",
    "Tetramethylurea":"632-22-4",
    "Carbon Disulfide":"75-15-0",
    "1,2,4-Trichlorobenzene":"120-82-1",
    "Chlorobenzene":"108-90-7",
    "1,2-Dichlorobenzene":"95-50-1",
    "Trichloroacetonitrile":"545-06-2",
    "Perfluorotoluene":"434-64-0",
    "Fluorobenzene":"462-06-6",
    "Perfluorocyclic Ether":"335-36-4",
    "Dichloromethane":"75-09-2",
    "1,2-Dichloroethane":"107-06-2",
    "Perfluorocyclohexane":"355-68-0",
    "Chloroform":"67-66-3",
    "Trichloroacetic Acid":"76-03-9",
    "Chloroacetic Acid":"79-11-8",
    "Trifluoroacetic Acid":"76-05-1",
    "Perfluorohexane":"355-42-0",
    "Carbon Tetrachloride":"56-23-5",
    "2,2,2-Trifluoroethanol":"75-89-8",
    "Furfural":"98-01-1",
    "N,N-Dimethylacetamide":"14433-76-2",
    "Dihydrolevoglucosenone":"1087696-49-8",
    "N,N-Dimethyloctanamide":"1118-92-9",
    "N,N-Dimethylaniline":"121-69-7",
    "Acetic Anhydride":"108-24-7",
    "Nitromethane":"75-52-5",
    "Triethylamine":"121-44-8",
    "Petroleum Ether":"64742-49-0",
    "Hexanes (Mixed Isomers)":"107-83-5",
}

#Dictionary to convert entries volume and mass to litres for summation
#Mass to litre conversion assumes an average density of 0.8 g/ml
to_litre={'µl':0.000001,
          'µL':0.000001,
          'ul':0.000001,
          'uL':0.000001,
          'ml':0.001,
          'mL':0.001,
          'l':1.0,
          'L':1.0,
          'µg':0.00000000125,
          'ug':0.00000000125,
          'mg':0.00000125,
          'g':0.00125,
          'kg':1.25,
          'oz':0.035436875,
          'lb':0.56699,
          'lbs':0.56699,
          'gal':4.54609}

def create_uuid(val1,val2,val3):
    concat_string=str(val1)+str(val2)+str(val3)
    hex_string = hashlib.md5(concat_string.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)

def tolitre(dataframe,value,unit):
    for row in dataframe:

async def main():
    # Do ChemInventory processing
    for key, value in gsk_2016.items():
            ci = req.post("https://app.cheminventory.net/api/search/execute",
                        json = {"authtoken": os.getenv("CHEMINVENTORY_CONNECTION_STRING"),
                                "inventory": 873,
                                "type": "cas",
                                "contents": value})
            ci_json_raw = ci.json()
            ci_json_data = pd.json_normalize(ci_json_raw ['data']['containers'])
            ci_df = pd.DataFrame(ci_json_data)

            if ci_df.empty:
                print(f"No records for {key}") #escape to allow for a null return
                temp_sum=0
            else:
                ci_df_real = ci_df.loc[ci_df["location"]!=527895] #remove any entries in "Missing - Stockcheck Only" location
                if ci_df_real.empty:
                    print(f"No records for {key}") #second escape if null return after filtering
                    temp_sum=0
                else:
                    size=list(ci_df_real['size']) #convert 'size' column to list, and then floats
                    size_f=[float(i) for i in size]
                    unit=list(ci_df_real['unit']) #convert 'unit' column to list
                    conversion=[] #create blank list for litre-standardised conversion factor
                    for item in unit: #for each item in 'unit' list, reference 'conversion' dictionary to call conversion factor and append to list
                        factor=to_litre.get(item)
                        conversion.append(factor)
                    temp = [size_f[i] * conversion[i] for i in range(len(size))] #for each item in 'size' list, multiply by corresponding conversion factor from 'conversion' list, then sum converted list
                    temp_sum=sum(temp)
                    print(f"Total volume for {key} is {temp_sum} litres")
    
        # Fetch the connection string from an environment variable
        conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

        # Create instance of the device client using the authentication provider
        device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
        await device_client.connect()

        # Send a message
        labID=1
        sublabID=3
        vol=[str(key),temp_sum]
        time_send=datetime.now()
        
        msg_output='chem'
        msg_id=str(create_uuid(time_send,labID,sublabID))
        msg_payload=str({"labId":labID,"sublabId":sublabID,"sensorReadings":{"chem":vol}, "measureTimestamp":time_send.strftime('%Y-%m-%d %H:%M:%S')})
        msg=Message(msg_payload,message_id=msg_id,output_name=msg_output)
        await device_client.send_message(msg)
        print("Message successfully sent!")

        await device_client.shutdown()

        time.sleep(2)

def job():
    print("Job started")
    asyncio.run(main())
    print("job finished")

if __name__ == "__main__":
    schedule.every(1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)