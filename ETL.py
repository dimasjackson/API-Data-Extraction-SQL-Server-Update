import pyodbc # Conectar ao Banco de Dados SQL Server
import requests # Request data from API 
import pandas as pd # Dataset management
import time # To set a time to wait during requests
from datetime import datetime, timedelta # Calculate time interval
import sqlite3 # Send the data to SQL Server

# Microsoft's instructions for installing their latest ODBC drivers onto a variety of Linux/UNIX-based platforms are here: 
#https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server.

# Create a temporary text file for defining the ODBC DSN (Data Source Name) to your database, something like this:

#[MSSQLServerDatabase]
#Driver      = ODBC Driver 18 for SQL Server
#Description = Connect to my SQL Server instance
#Trace       = No
#Server      = mydbserver.mycompany.com

# After saving your temporary config file you can create a "System DSN" by using the following commands:

# register the SQL Server database DSN information in ~/.odbc.ini
#odbcinst -i -s -f /path/to/your/temporary/dsn/file -h

# check the DSN installation with:
#cat ~/.odbc.ini   # should contain a section called [MSSQLServerDatabase]

server = 'servername.database.windows.net'
database = 'db_name'
username = 'user_id'
password = 'xxxxxxx'

conn = pyodbc.connect('DSN=MSSQLServerDatabase' + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

cursor = conn.cursor()

# Execute uma consulta
cursor.execute('SELECT * FROM schema_name.table_name')

# Itera sobre os resultados
empresas = []
for row in cursor:
    empresas.append(row)

# Login and tolken request from API
url = 'http://api.name.com.br:8080/security/logon'
handshake = {"username":empresas[0][1], "password":empresas[0][2],  "appid":"1234", "token": "null", "expiration": "null"}  

response = requests.post(url, json=handshake).json() # API response with access token

token = response['object']['token'] 

# Request the list of vehicles to API
veiculos_url = 'http://api.name.com.br:9870/vehicles'
header = {"token": token}
veiculos_response = []
veiculos_response.append(requests.post(veiculos_url, headers=header).json())

veiculos_df = pd.DataFrame(veiculos_response[0]['object'])

# Query the last update date from SQL server
last_date_cursor = cursor.execute('SELECT TOP 1 dataProcessamento, ROW_NUMBER() OVER(ORDER BY dataProcessamento) AS Idx FROM company.geoloc_table ORDER BY Id DESC;')
last_update = last_date_cursor.fetchone()
last_update_Unix = int(last_update[0])

# Close connection
conn.close()

# Select the initial and final data
final_date = datetime.now() - timedelta(days=1) # Colect data until yesterday

# Convert to Unix timestamp
final_date_Unix = int(datetime.timestamp(final_date))

str(int(last_update_Unix/1000))

# Request vehicle position over the last 5 days
veiculos_periodo_url = 'http://api.name.com.br:8080/position/vehicle'

header = {"token": token, "dateInicial":str(int(last_update_Unix/1000)), "dateFinal":str(final_date_Unix)}
veiculos_posicoes_response = []
for veiculo in range(len(veiculos_response[0]['object'])):
    veiculo_json = veiculos_response[0]['object'][veiculo]
    veiculos_posicoes_response.append(requests.post(veiculos_periodo_url, headers=header, json=veiculo_json).json())
    time.sleep(3) # wait 3 seconds (tempo exigido pela API entre requisições)

# Store only the allowed vehicles and print the forbidden
veiculos_permitidos = []
veiculos_proibidos = []
for veiculo in veiculos_posicoes_response:
    if veiculo['status'] == 'OK':
        veiculos_permitidos.append(veiculo)
    else:
        veiculos_proibidos.append(veiculo)

print('There are', len(veiculos_proibidos), 'forbidden vehicles.')

# Concat all the vehicles gps data
list_df=[]
for i in range(0,len(veiculos_permitidos)):
    list_df.append(pd.DataFrame(veiculos_permitidos[i]['object']['dispositivos'][0]['posicoes']))
    list_df[i]['id_veiculo'] = veiculos_permitidos[i]['object']['id']

dados_gps_veiculos_df = pd.concat(list_df)

# Send the data to SQL Server
conn_load = sqlite3.connect(database)

dados_gps_veiculos_df = dados_gps_veiculos_df.astype('str') # Convert all data types to string
dados_gps_veiculos_df.to_sql('geoloc_table',con=con,schema='schema_name',if_exists='append',index=False, chunksize=50, method='multi')

# Commit the changes
conn_load.commit()

#conn_out.close()
conn_load.close()

