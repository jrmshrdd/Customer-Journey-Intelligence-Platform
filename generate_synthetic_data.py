import numpy as np, pandas as pd
from pathlib import Path
rng = np.random.default_rng(7)
N_USERS = 10000; HORIZON_DAYS = 180
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True, parents=True)
channels = ["web","mobile_app","email","paid_ads","sales_rep","support"]
regions = ["US-East","US-West","EU","APAC"]
plans = ["free","basic","pro","enterprise"]
users = pd.DataFrame({
    "user_id": np.arange(1, N_USERS+1),
    "signup_day": rng.integers(1,30,size=N_USERS),
    "region": rng.choice(regions,size=N_USERS,p=[.35,.25,.25,.15]),
    "plan_at_signup": rng.choice(plans,size=N_USERS,p=[.4,.35,.2,.05]),
    "acq_channel": rng.choice(["organic","paid","partner","sales"], size=N_USERS, p=[.5,.25,.15,.10]),
})
users["signup_date"] = pd.to_datetime("2024-01-01") + pd.to_timedelta(users["signup_day"], unit="D")
users.drop(columns=["signup_day"], inplace=True)
events_list = []
for uid in users["user_id"].values:
    n_events = rng.integers(5,60); days = rng.integers(0,HORIZON_DAYS,size=n_events)
    ts = pd.to_datetime("2024-01-01") + pd.to_timedelta(days, unit="D"); ts = np.sort(ts)
    events_list.append(pd.DataFrame({
        "user_id": uid, "event_time": ts,
        "channel": rng.choice(channels,size=n_events,p=[.35,.25,.10,.15,.10,.05]),
        "event_type": rng.choice(["view","search","add_to_cart","checkout","subscription_renewal","support_ticket"],
                                 size=n_events,p=[.45,.2,.15,.05,.05,.10]),
        "event_value": rng.normal(1.0,0.3,size=n_events).round(3)
    }))
events = pd.concat(events_list, ignore_index=True)
order_users = rng.choice(users["user_id"].values, size=int(N_USERS*0.45), replace=False)
orders = []
for uid in order_users:
    k = rng.integers(1,4)
    order_ts = pd.to_datetime("2024-01-01") + pd.to_timedelta(rng.integers(5,HORIZON_DAYS,k), unit="D")
    amt = rng.gamma(2.0, 50.0, size=k).round(2)
    orders.append(pd.DataFrame({"user_id": uid, "order_time": np.sort(order_ts), "order_amount": amt}))
orders = pd.concat(orders, ignore_index=True) if orders else pd.DataFrame(columns=["user_id","order_time","order_amount"])
campaigns = []
for uid in users["user_id"].values:
    if rng.random() < 0.6:
        n = rng.integers(1,3)
        t = pd.to_datetime("2024-01-15") + pd.to_timedelta(rng.integers(0,120,n), unit="D")
        campaigns.append(pd.DataFrame({"user_id": uid, "campaign_time": np.sort(t),
                                       "promo_type": rng.choice(["discount","email_reengage","push"], size=n),
                                       "treatment": 1}))
campaigns = pd.concat(campaigns, ignore_index=True) if campaigns else pd.DataFrame(columns=["user_id","campaign_time","promo_type","treatment"])
last_date = pd.to_datetime("2024-01-01") + pd.to_timedelta(HORIZON_DAYS, unit="D")
last_event = events.groupby("user_id")["event_time"].max().rename("last_event_time")
u = users.merge(last_event, on="user_id", how="left")
u["inactive_days"] = (last_date - u["last_event_time"]).dt.days.fillna(HORIZON_DAYS)
u["is_churn"] = ((u["inactive_days"] >= 45) & (u["plan_at_signup"] != "enterprise")).astype(int)
users.to_csv("data/users.csv", index=False)
events.to_csv("data/events.csv", index=False)
orders.to_csv("data/orders.csv", index=False)
campaigns.to_csv("data/campaigns.csv", index=False)
u[["user_id","is_churn"]].to_csv("data/labels.csv", index=False)
print("Generated users, events, orders, campaigns, labels.")
