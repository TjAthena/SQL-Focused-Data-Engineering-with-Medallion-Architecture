import boto3
import pandas as pd
from sqlalchemy import create_engine




# --------------------
# THIS IS A TEMPLATE SCRIPT FOR RAW UNDERSATNDING - REPLACE THE VALUES 
# YOU CAN IMPLEMENT LOOPING TO PROCESS ALL DATA FILES
# S3
# --------------------
BUCKET = "REPLACE_WITH_YOUR_BUCKET_NAME"
KEY = "bronze/orders/REPLACE_ME_WITH_YOUR_FILE.csv" 

# --------------------
# POSTGRES
# --------------------
DB_HOST = "REPLACE_WITH_YOUR_DB_HOST"
DB_PORT = 5432
DB_NAME = "REPLACE_WITH_YOUR_DB_NAME"
DB_USER = "REPLACE_WITH_YOUR_USER"
DB_PASSWORD = "REPLACE_WITH_YOUR_PASSWORD"

SCHEMA = "TARGET_SCHEMA_NAME usually_bronze"
TABLE = "TARGET_TABLE_NAME example_customers"

# --------------------
# READ CSV FROM S3
# --------------------
s3 = boto3.client("s3")
obj = s3.get_object(Bucket=BUCKET, Key=KEY)["datafile.csv"]

df = pd.read_csv(obj)

print("Rows read:", len(df))

# --------------------
# CONNECT DB
# --------------------
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# --------------------
# LOAD INTO SILVER
# --------------------
df.to_sql(
    TABLE,
    engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    chunksize=1000,
    method="multi"
)

print("Load completed")
