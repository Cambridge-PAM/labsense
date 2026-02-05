from datetime import date
import pandas as pd
import pyodbc

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

from Labsense_SQL.constants import gsk_2016
# `gsk_2016` moved to `Labsense_SQL.constants` to avoid duplication.,
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
from Labsense_SQL.constants import to_litre
# `to_litre` moved to `Labsense_SQL.constants` to avoid duplication.

def insert_sql(cas,name,volume,datestamp):
    try:
        # Create a connection
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute('''
        IF 
        ( NOT EXISTS 
        (select object_id from sys.objects where object_id = OBJECT_ID(N'[chemOrders]') and type = 'U')
        )
        BEGIN
            CREATE TABLE chemOrders
            (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                CAS VARCHAR(15),
                Name VARCHAR(30),
                Volume REAL,
                Datestamp DATE
            )
        END
        ''') # create table

        cursor.execute('''
        INSERT INTO chemOrders (CAS,Name,Volume,Datestamp)
        VALUES (?,?,?,?)''',(cas,name,volume,datestamp)) #insert into table

        # cursor.execute('SELECT * FROM chemOrders')
        # rows = cursor.fetchall()
        
        # column_names = [description[0] for description in cursor.description]
        # print(f"{column_names}")
        # for row in rows:
        #    print(row)  #printing table, for debugging purposes
        connection.commit()
        connection.close()

    except pyodbc.Error as ex:
        print("An error occurred in SQL Server:", ex)

def main():
   #import order sheet to be read, define as "df"
   df = pd.read_excel("SPREADSHEET.xlsx",engine='openpyxl')#add the file you want to read from

   #filter full sheet to retain only those with an entry in "CAS Number" column, define as "ord_chem"
   df = df[df["CAS Number"].notnull()]

   #filter CAS-restricted list to columns of use ("Full Name", "Volume/Weight/Size", "Unit", "Number", "CAS Number", "Date ordered"), define as "chemlist_red"
   df = df.iloc[:, [0, 3, 4, 7, 8, 17]]

   new_df=pd.DataFrame(columns=['CAS Number','Name','Volume','Timestamp']) #creating columns for data frame

   for key, value in gsk_2016.items():
        ord_chem_cas = df.loc[df["CAS Number"]==value]
        if ord_chem_cas.empty:
           print(f"No records for {key}\n")
           temp_sum=0
        else:
           ord_chem_cas = ord_chem_cas.astype({'Volume/Weight/Size':'float', 'Number':'float'})
           ord_chem_cas["Total Volume (L)"] = (ord_chem_cas["Volume/Weight/Size"]*ord_chem_cas["Number"]*ord_chem_cas["Unit"].map(to_litre))#finding total volume of a chemical-converted to litres
           temp=ord_chem_cas['Total Volume (L)']
           temp_sum=temp.sum()
           print(f"{key} {value}\n{temp_sum}\n") #for debugging, can be removed
        
        today = date.today()
        insert_sql(value,key,temp_sum,today)

main()