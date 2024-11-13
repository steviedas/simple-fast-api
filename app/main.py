import duckdb
from fastapi import FastAPI, status

app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}


@app.get("/events", status_code=status.HTTP_200_OK)
def get_events_table() -> list:
    blob_connection_string = "<storage_account_connection_string>"
    container_name = "<container_name>"
    delta_table_name = "<delta_table_directory_name>"

    conn = duckdb.connect()
    queries = [
        "INSTALL azure;",
        "LOAD azure;",
        "INSTALL delta;",
        "LOAD delta;",
        "INSTALL parquet;",
        "LOAD parquet;"
    ]
 
    for query in queries:
        conn.execute(query)
    conn.execute(f"CREATE OR REPLACE SECRET az1 ( TYPE AZURE, CONNECTION_STRING '{blob_connection_string}')")
 
    table = f"'az://{container_name}/{delta_table_name}'"
    records = conn.execute(f"SELECT * FROM delta_scan({table})").fetch_df().to_dict('records')
   
    return records