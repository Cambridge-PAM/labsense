import sqlite3
 
# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('labsense.db')
cursor = conn.cursor()
drop_table_query = "DROP TABLE IF EXISTS elec_daily"
cursor.execute(drop_table_query)
conn.commit()
conn.close()
 
print("Table deleted successfully.")
