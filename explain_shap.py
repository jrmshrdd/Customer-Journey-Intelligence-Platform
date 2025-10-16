import joblib, pandas as pd, numpy as np, shap, os
from dotenv import load_dotenv
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
pipe = joblib.load("models/churn_xgb.joblib")
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"), password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"), role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"), database=os.getenv("SNOWFLAKE_DATABASE","CJA_DB"), schema="CLEAN"
)
df = pd.read_sql("SELECT * FROM FEATURES_TRAINING", conn)
y = df["IS_CHURN"].astype(int)
X = df.drop(columns=["IS_CHURN"])

pre = pipe.named_steps["pre"]; model = pipe.named_steps["xgb"]
X_enc = pre.transform(X)
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_enc)

feat_names = list(pre.get_feature_names_out())
abs_mean = np.abs(shap_values).mean(axis=0)
global_importance = pd.DataFrame({"feature": feat_names, "abs_mean_shap": abs_mean}).sort_values("abs_mean_shap", ascending=False).head(20)

top_idx = np.argsort(-np.abs(shap_values), axis=1)[:, :3]
top_feats = pd.DataFrame({
    "USER_ID": X["USER_ID"].values,
    "TOP1_FEATURE": [feat_names[i[0]] for i in top_idx],
    "TOP2_FEATURE": [feat_names[i[1]] for i in top_idx],
    "TOP3_FEATURE": [feat_names[i[2]] for i in top_idx],
})

with conn.cursor() as cs:
    cs.execute("CREATE OR REPLACE TABLE CLEAN.SHAP_GLOBAL (FEATURE STRING, ABS_MEAN_SHAP FLOAT)")
    cs.execute("CREATE OR REPLACE TABLE CLEAN.SHAP_TOP_FEATURES (USER_ID INT, TOP1_FEATURE STRING, TOP2_FEATURE STRING, TOP3_FEATURE STRING)")
write_pandas(conn, global_importance.rename(columns=str.upper), "SHAP_GLOBAL")
write_pandas(conn, top_feats.rename(columns=str.upper), "SHAP_TOP_FEATURES")
conn.close()
