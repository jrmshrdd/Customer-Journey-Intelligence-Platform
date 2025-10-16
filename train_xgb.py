import os, pandas as pd, numpy as np
from dotenv import load_dotenv
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, confusion_matrix
from xgboost import XGBClassifier
import joblib

load_dotenv()
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"), password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"), role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"), database=os.getenv("SNOWFLAKE_DATABASE","CJA_DB"), schema="CLEAN"
)

df = pd.read_sql("SELECT * FROM FEATURES_TRAINING", conn)
y = df["IS_CHURN"].astype(int)
X = df.drop(columns=["IS_CHURN"])

cat = ["REGION","PLAN_AT_SIGNUP","ACQ_CHANNEL"]
num = [c for c in X.columns if c not in cat + ["USER_ID","SIGNUP_DATE"]]

pre = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
    ("num", StandardScaler(), num)
], remainder="drop")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

clf = XGBClassifier(
    n_estimators=500, max_depth=5, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
    reg_lambda=1.0, n_jobs=-1, eval_metric="logloss", random_state=42
)
pipe = Pipeline([("pre", pre), ("xgb", clf)])
pipe.fit(X_train, y_train)

proba = pipe.predict_proba(X_test)[:,1]
pred = (proba >= 0.5).astype(int)

print("ROC-AUC:", roc_auc_score(y_test, proba))
print("PR-AUC :", average_precision_score(y_test, proba))
print("F1     :", f1_score(y_test, pred))
print("CM     :\n", confusion_matrix(y_test, pred))

os.makedirs("models", exist_ok=True)
joblib.dump(pipe, "models/churn_xgb.joblib")

all_proba = pipe.predict_proba(X)[:,1]
pred_tbl = pd.DataFrame({"USER_ID": X["USER_ID"].values, "CHURN_PROBA": all_proba})
pred_tbl["RISK_BUCKET"] = pd.qcut(pred_tbl["CHURN_PROBA"], q=5, labels=["Very Low","Low","Medium","High","Very High"])

with conn.cursor() as cs:
    cs.execute("CREATE OR REPLACE TABLE CLEAN.PREDICTIONS (USER_ID INT, CHURN_PROBA FLOAT, RISK_BUCKET STRING)")
write_pandas(conn, pred_tbl, "PREDICTIONS")
conn.close()
