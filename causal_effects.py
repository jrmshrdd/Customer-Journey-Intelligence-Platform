import os, pandas as pd, numpy as np
from dotenv import load_dotenv
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from econml.dr import DRLearner
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

load_dotenv()
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"), password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"), role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"), database=os.getenv("SNOWFLAKE_DATABASE","CJA_DB"), schema="CLEAN"
)
df = pd.read_sql("SELECT s.*, p.CHURN_PROBA, (1-IS_CHURN) AS RETAINED FROM FEATURES_TRAINING s LEFT JOIN PREDICTIONS p USING(user_id)", conn)

df["TREATMENT"] = (df["CAMPAIGN_EXPOSURES"] > 0).astype(int)
y = df["RETAINED"].astype(float)
T = df["TREATMENT"].astype(int)
X = df[["REGION","PLAN_AT_SIGNUP","ACQ_CHANNEL","V30","S30","ATC30","CO30","REN30","SUP30","ENG30","REV90","DAYS_SINCE_ORDER","TENURE_DAYS","INACTIVITY_DAYS","DISCOUNT_HITS"]]

cat = ["REGION","PLAN_AT_SIGNUP","ACQ_CHANNEL"]
num = [c for c in X.columns if c not in cat]

pre = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
    ("num", Pipeline([("imp", SimpleImputer()),]), num)
])

dr = DRLearner(model_regression=RandomForestRegressor(n_estimators=200, random_state=42),
               model_propensity=RandomForestClassifier(n_estimators=200, random_state=42),
               random_state=42)

X_enc = pre.fit_transform(X)
dr.fit(y, T, X=X_enc)
uplift = dr.effect(X_enc)

res = pd.DataFrame({"USER_ID": df["USER_ID"], "UPLIFT": uplift})
res["UPLIFT_BUCKET"] = pd.qcut(res["UPLIFT"], q=5, labels=["Very Low","Low","Medium","High","Very High"])

with conn.cursor() as cs:
    cs.execute("CREATE OR REPLACE TABLE CLEAN.UPLIFT_EFFECTS (USER_ID INT, UPLIFT FLOAT, UPLIFT_BUCKET STRING)")
write_pandas(conn, res, "UPLIFT_EFFECTS")
conn.close()
