import os, pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector as sf

load_dotenv()
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE","CJA_WH"),
    database=os.getenv("SNOWFLAKE_DATABASE","CJA_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA","RAW"),
)
cs = conn.cursor()
try:
    cs.execute("CREATE WAREHOUSE IF NOT EXISTS CJA_WH WITH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
    cs.execute("CREATE DATABASE IF NOT EXISTS CJA_DB")
    cs.execute("CREATE SCHEMA IF NOT EXISTS CJA_DB.RAW")
    cs.execute("USE DATABASE CJA_DB"); cs.execute("USE SCHEMA RAW")
    cs.execute("""CREATE OR REPLACE TABLE USERS (user_id INT, signup_date TIMESTAMP_NTZ, region STRING, plan_at_signup STRING, acq_channel STRING)""")
    cs.execute("""CREATE OR REPLACE TABLE EVENTS (user_id INT, event_time TIMESTAMP_NTZ, channel STRING, event_type STRING, event_value FLOAT)""")
    cs.execute("""CREATE OR REPLACE TABLE ORDERS (user_id INT, order_time TIMESTAMP_NTZ, order_amount FLOAT)""")
    cs.execute("""CREATE OR REPLACE TABLE CAMPAIGNS (user_id INT, campaign_time TIMESTAMP_NTZ, promo_type STRING, treatment INT)""")
    cs.execute("""CREATE OR REPLACE TABLE LABELS (user_id INT, is_churn INT)""")

    for name in ["users","events","orders","campaigns","labels"]:
        path = f"data/{name}.csv"
        df = pd.read_csv(path, parse_dates=[c for c in ["signup_date","event_time","order_time","campaign_time"] if c in open(path).read(200)])
        df.columns = [c.upper() for c in df.columns]
        write_pandas(conn, df, name.upper(), auto_create_table=False)
        print(f"Loaded {name}")

    print("Now run SQL in src/features_sql.sql inside Snowflake to create features & training view.")
finally:
    cs.close(); conn.close()
