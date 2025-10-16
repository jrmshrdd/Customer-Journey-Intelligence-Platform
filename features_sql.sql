USE WAREHOUSE CJA_WH; USE DATABASE CJA_DB;
CREATE OR REPLACE SCHEMA CJA_DB.CLEAN;

CREATE OR REPLACE VIEW CLEAN.DIM_USERS AS
SELECT u.user_id, u.signup_date::DATE AS signup_date, u.region, u.plan_at_signup, u.acq_channel
FROM RAW.USERS u;

CREATE OR REPLACE VIEW CLEAN.FACT_USER_DAY AS
SELECT
  e.user_id,
  DATE_TRUNC('day', e.event_time) AS day,
  COUNT_IF(event_type='view') AS views,
  COUNT_IF(event_type='search') AS searches,
  COUNT_IF(event_type='add_to_cart') AS add_to_cart,
  COUNT_IF(event_type='checkout') AS checkouts,
  COUNT_IF(event_type='subscription_renewal') AS renewals,
  COUNT_IF(event_type='support_ticket') AS support_tickets,
  SUM(event_value) AS engagement_score
FROM RAW.EVENTS e
GROUP BY 1,2;

CREATE OR REPLACE VIEW CLEAN.FACT_ORDERS_DAY AS
SELECT user_id, DATE_TRUNC('day', order_time) AS day, COUNT(*) AS orders, SUM(order_amount) AS revenue
FROM RAW.ORDERS
GROUP BY 1,2;

SET SNAPSHOT_DATE = '2024-06-29';

CREATE OR REPLACE TABLE CLEAN.CUSTOMER_SNAPSHOT AS
WITH last30 AS (
  SELECT
    d.user_id,
    SUM(views) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS v30,
    SUM(searches) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS s30,
    SUM(add_to_cart) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS atc30,
    SUM(checkouts) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS co30,
    SUM(renewals) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS ren30,
    SUM(support_tickets) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS sup30,
    SUM(engagement_score) FILTER (WHERE day >= DATEADD('day', -30, $SNAPSHOT_DATE)) AS eng30
  FROM CLEAN.FACT_USER_DAY d
  GROUP BY 1
),
rev AS (
  SELECT user_id,
         SUM(revenue) FILTER (WHERE day >= DATEADD('day', -90, $SNAPSHOT_DATE)) AS rev90,
         MAX(day) AS last_order_day
  FROM CLEAN.FACT_ORDERS_DAY
  GROUP BY 1
),
first_last AS (
  SELECT e.user_id,
         MIN(e.event_time)::DATE AS first_event,
         MAX(e.event_time)::DATE AS last_event
  FROM RAW.EVENTS e
  GROUP BY 1
),
camp AS (
  SELECT user_id,
         COUNT(*) AS campaign_exposures,
         COUNT_IF(promo_type='discount') AS discount_hits
  FROM RAW.CAMPAIGNS
  GROUP BY 1
)
SELECT
  u.user_id, u.signup_date, u.region, u.plan_at_signup, u.acq_channel,
  NVL(l.v30,0) v30, NVL(l.s30,0) s30, NVL(l.atc30,0) atc30, NVL(l.co30,0) co30,
  NVL(l.ren30,0) ren30, NVL(l.sup30,0) sup30, NVL(l.eng30,0) eng30,
  NVL(r.rev90,0) rev90,
  DATEDIFF('day', r.last_order_day, $SNAPSHOT_DATE) AS days_since_order,
  DATEDIFF('day', first_event, $SNAPSHOT_DATE) AS tenure_days,
  DATEDIFF('day', last_event, $SNAPSHOT_DATE) AS inactivity_days,
  NVL(c.campaign_exposures,0) AS campaign_exposures,
  NVL(c.discount_hits,0) AS discount_hits
FROM CLEAN.DIM_USERS u
LEFT JOIN last30 l  ON u.user_id=l.user_id
LEFT JOIN rev r     ON u.user_id=r.user_id
LEFT JOIN first_last fl ON u.user_id=fl.user_id
LEFT JOIN camp c     ON u.user_id=c.user_id;

CREATE OR REPLACE VIEW CLEAN.FEATURES_TRAINING AS
SELECT s.*, lbl.is_churn
FROM CLEAN.CUSTOMER_SNAPSHOT s
LEFT JOIN RAW.LABELS lbl USING(user_id);
