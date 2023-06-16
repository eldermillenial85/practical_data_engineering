
import os
import io
import pandas as pd
import json

from google.oauth2 import service_account
from apiclient.discovery import build

credentials = service_account.Credentials.from_service_account_file("/work/credentials.json")
scopes = ["https://www.googleapis.com/auth/drive.readonly"]
scoped_credentials = credentials.with_scopes(scopes)

drive = build("drive", "v3", credentials=scoped_credentials)

request = drive.files().list().execute()
files = request.get("files", [])

import io
from googleapiclient.http import MediaIoBaseDownload

request = drive.files().get_media(fileId = files[1]["id"])
f = io.BytesIO()
downloader = MediaIoBaseDownload(f, request)

done = False
while done is False:
    status, done = downloader.next_chunk()
    #print(f"Download {int(status.progress() * 100)}% complete")

file_content = f.getvalue()

def create_drive_client():
    credentials = service_account.Credentials.from_service_account_file("/work/credentials.json")
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    scoped_credentials = credentials.with_scopes(scopes)

    drive = build("drive", "v3", credentials=scoped_credentials)

    return drive

def postgres_upsert(table, conn, keys, data_iter):


    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded} 
    )
    conn.execute(upsert_statement)

def download_file(file_id):
    request = drive.files().get_media(fileId=file_id)
    f = io.BytesIO()
    downloader = MediaIoBaseDownload(f, request)

    done = False
    while done is False:
        status, done = downloader.next_chunk()
        #print(f"Download {int(status.progress() * 100)}% complete")

    return f.getvalue()


def list_files():
    drive = create_drive_client()

    request = drive.files().list().execute()
    files = request.get("files", [])
    files = [f for f in files if f["mimeType"] != "application/vnd.google-apps.folder"]

    return files


def load_file_to_df(f):
    filename, ext = f["name"].split(".")

    assert filename.count("__") == 2, "Filename is not in expected format, it needs to include 2 '__'s."

    location, date, employee = filename.split("__")

    file_content = download_file(f["id"])

    file_df = pd.read_excel(io=file_content)

    file_df ["location"] = location
    file_df ["date"] = date
    file_df ["employee"] = employee
    
    return file_df


def load_files_to_df(files=None, dfs_in=None):

    dfs = []
    malformed_filenames = []
    
    files = list_files()

    for f in files:
        try:
            file_df = load_file_to_df(f)
            dfs.append(file_df)
        except AssertionError:
            #print(f"Filename {f['name']} is not following the expected format.")
            malformed_filenames.append(f["name"])
            continue
    
    if len(dfs) > 0:
        concat_df = pd.concat(dfs)
    else:
        concat_df = pd.DataFrame()
    
    return concat_df, malformed_filenames

market_df, malformed_filenames = load_files_to_df()

def parallel_load_files_to_df(verbose=False):
    return market_df


#def parallel_load_files_to_df(verbose=False):
#    assert os.getenv("PG_CONN") is not None, "PG_CONN environment variable is not set."
#    pg_conn = os.getenv("PG_CONN")

    #df.to_sql(name=table_name, con=pg_conn, if_exists="append", method=postgres_upsert, index=False, dtype={"additional_data": JSONB})

#def load_dataframe(df, table_name="transactions"):
#    assert os.getenv("PG_CONN") is not None, "PG_CONN environment variable is not set."
#    pg_conn = os.getenv("PG_CONN")
#
#    df.to_sql(name=table_name, con=pg_conn, if_exists="append", method=postgres_upsert, index=False, dtype={"additional_data": JSONB})