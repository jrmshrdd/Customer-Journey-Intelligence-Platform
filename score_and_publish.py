import os, joblib, pandas as pd
from dotenv import load_dotenv
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"), password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"), role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"), database=os.getenv("SNOWFLAKE_DATABASE","CJA_DB"), schema="CLEAN"
)
df = pd.read_sql("SELECT * FROM CUSTOMER_SNAPSHOT", conn)
pipe = joblib.load("models/churn_xgb.joblib")
X = df.drop(columns=["USER_ID","SIGNUP_DATE"], errors="ignore")
proba = pipe.predict_proba(X)[:,1]
pred = pd.DataFrame({"USER_ID": df["USER_ID"], "CHURN_PROBA": proba})
pred["RISK_BUCKET"] = pd.qcut(pred["CHURN_PROBA"], q=5, labels=["Very Low","Low","Medium","High","Very High"])
with conn.cursor() as cs:
    cs.execute("CREATE OR REPLACE TABLE CLEAN.PREDICTIONS (USER_ID INT, CHURN_PROBA FLOAT, RISK_BUCKET STRING)")
write_pandas(conn, pred, "PREDICTIONS")
conn.close()
