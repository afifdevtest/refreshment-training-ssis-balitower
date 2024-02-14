from sqlalchemy import text
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime
from sqlalchemy import text
import gspread
import pandas as pd
import sqlalchemy
import numpy as np
import os
import access as ac
import urllib.parse as ur
import numpy as np


# Google sheet connection
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]
script_directory = os.path.dirname(os.path.abspath(__file__))
cred_json = os.path.join(script_directory, 'credentials.json')
credentials = ServiceAccountCredentials.from_json_keyfile_name(cred_json,scopes=scope)
gc = gspread.authorize(credentials)


# Set up variable
xlsx_file_id = ac.datapatroli   # ID file .xlsx on Google Drive
worksheet_name = 'Data Patroli' # Worsheet name on Google Sheets
schema_name = 'ops_gsheet'      # schema target
table_name = 'datapatroli'      # table target

# Create a Drive service
drive_service = build('drive', 'v3', credentials=credentials)

# Copy and convert the file to Google Sheets format
copied_file = drive_service.files().copy(
    fileId=xlsx_file_id,
    body={"name": "Copy of Original", "mimeType": "application/vnd.google-apps.spreadsheet"}
).execute()

# Get the new Google Sheets file's ID
new_sheets_file_id = copied_file['id']

# Use the new ID for reference if needed
sheets_file_id = new_sheets_file_id

# Access the new Google Sheets document
spreadsheet = gc.open_by_key(sheets_file_id)

# Get the worksheet based on name
worksheet = spreadsheet.worksheet(worksheet_name)

# Construct connection to postgres
database_url = sqlalchemy.create_engine(
        f'postgresql://{ac.user}:{ur.quote(ac.pwd)}@{ac.host}:{ac.port}/{ac.database}'
    )
pg_conn=database_url.connect()

column_mapping={
    '.Foto Tampak Jauh (before)':'Foto Tampak Jauh (before)'
}

spec_columns=['Tanggal','Scope Of Work','Category Issue','Lat','Long','Tolong di isi LONGLAT Diatas DI kolom INI','Priority','PIC','Nama Jalan','City','Site Name','Cluster MP','Temuan','Pic Perbaikan','Status activity','Start Progres','End Progres','Cable','Type pole crosing','Route Fo','Posisi Jalan Raya','Grup Cable','Kondisi Cable','Jumlah Cable','Jenis Cable (Core)','Jenis Pole','Type Ground pole','Kondisi Pole','Pondasi Pole','Aksesori Pole','Type Aksesoris','Stang Slack','Kebutuhan ACC&Pole','NOTE','Foto Tampak Dekat (before )','Foto Tampak Jauh (before)','Foto Cable (before)','Foto Aksesoris (before)','Foto Pondasi(before)','Foto Tiang (before)','Foto Crosingan (before)','Foto Kabel Landai (before)','Foto tambahan 1(before)','Foto tambahan 2(before)','Foto tambahan 3(before)','Foto tambahan 4(before)','Foto tambahan 5(before)','Foto tambahan 6(before)','Foto tambahan 7(before)','Foto Tampak Dekat (after)','Foto Tampak Jauh (after)','Foto Cable (After)','Foto Accesories (after)','Foto Pondasi(after)','Foto Tiang (after)','Foto Tambahan1 (after)','Foto Tambahan2 (after)','Foto Tambahan3 (after)','Foto Tambahan4 (after)','No','Uniqe','Index','status Dalam Foto','Unique 2','Site ID atau ID Pole','Site','MP','Link Photo','Arah Kabel','Total Kabel Crosing','Panjang kabel A','Jenis Kabel A','Total Panjang Kabel A','Panjang Kabel B','Jenis Kabel B','Total Panjang Kabel B','OTB','JC','Pole/Tiang','Remark']

# Split LatLong from another column
def split_latlong(latlong):
    return pd.Series(latlong.split(",",1))

# Function to convert variant date formats to yyyy-mm-dd
def date_conversion(date_string):
    if date_string is None or pd.isnull(date_string) or date_string == '':
        return None

    formats_to_try = ['%d-%b-%y', '%m/%d/%Y']  # Add more formats as needed

    for date_format in formats_to_try:
        try:
            date_obj = datetime.strptime(date_string, date_format)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # If no format matches, return None or handle the error as needed
    return None

# Construct source dataframe
data = worksheet.get_all_values()
df = pd.DataFrame(data[1:],columns=data[0],index=None)

# Hapus kolom existing Latitude dan Longitude
df.drop(columns=['Lat','Long'],axis=1)

# Konversi kolom tanggal dengan proper format
df['Tanggal']=df['Tanggal'].apply(date_conversion)

# Define Latitude dan Longitude dari kolom ini
df['Tolong di isi LONGLAT Diatas DI kolom INI']=df['Tolong di isi LONGLAT Diatas DI kolom INI'].str.replace("106,","106.")
df[['Lat','Long']]=df['Tolong di isi LONGLAT Diatas DI kolom INI'].str.split(',',expand=True)

# Mapping column
df.rename(columns=column_mapping,inplace=True)

# Konversi kolom ke numeric, untuk data yang tidak proper auto-null
numeric_columns=['Lat','Long','Arah Kabel','Total Kabel Crosing']
for col in numeric_columns:
    df[col]=pd.to_numeric(df[col],errors='coerce')

df.dropna(how='all', axis=0, subset=df.columns[df.isin(['',' ',np.NaN]).any()])
df['No']=df['No'].replace(r'^\s*$', np.nan, regex=True)
df_spec=df[spec_columns]

# Truncate table
sqltruncate=f'truncate table {schema_name}.{table_name}'
pg_conn.execute(text(sqltruncate))
pg_conn.commit()
pg_conn.close()

# Insert data
df_spec.to_sql(table_name,con=database_url,schema=schema_name,if_exists='append',index=False)