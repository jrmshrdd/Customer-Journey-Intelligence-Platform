# Customer Journey Analytics with Power BI & Machine Learning

End-to-end Data Science project combining **Snowflake, Python (XGBoost, SHAP, EconML), and Power BI** to analyze multi-touch customer journeys, predict churn, and measure campaign uplift.

---

## Project Overview
This project demonstrates how to integrate **data engineering, machine learning, causal inference, and business intelligence** into one workflow for customer journey analytics.  
It uses **synthetic customer data** across multiple touchpoints (web, mobile, sales, campaigns) to:

- Build an **ETL pipeline** with Python + SQL into Snowflake.  
- Train an **XGBoost churn prediction model** with SHAP explainability.  
- Apply **causal inference (EconML/DoWhy)** to measure promotional impact.  
- Deliver an **interactive Power BI dashboard** with funnels, churn risk, uplift scores, and what-if scenarios.  

This balances **technical ML modeling** with **business-facing storytelling**, making it highly relevant for Data Scientist roles in **e-commerce, SaaS, and consulting**.

---

## Key Features
- **Data Engineering Pipeline** → Load and transform data into Snowflake warehouse.  
- **Customer Funnel Dashboards** → Track KPIs across journey stages in Power BI.  
- **Churn Prediction (XGBoost + SHAP)** → Predict churn probability and explain drivers.  
- **Causal Uplift Modeling** → Measure campaign effectiveness with EconML DR-Learner.  
- **Scenario Simulation** → Power BI what-if analysis for retention strategies.  

---

## 📂 Repository Structure

customer-journey-analytics/
- ├── data/ # synthetic/sample datasets
- ├── notebooks/ # EDA and modeling notebooks
- ├── src/ # Python scripts for ETL, ML, explainability, causal
- ├── powerbi/ # Power BI .pbix file + DAX measures
- ├── pipelines/ # GitHub Actions workflow (optional automation)
- ├── models/ # saved ML models (gitignored if large)
- ├── requirements.txt # Python dependencies
- └── README.md # project documentation


---

## ⚙️ Tech Stack
- **Data Engineering**: Python, SQL, Snowflake  
- **Machine Learning**: scikit-learn, XGBoost, SHAP  
- **Causal Inference**: EconML, DoWhy  
- **Visualization**: Power BI (dashboards, funnel analysis, what-if scenarios)  
- **Automation**: GitHub Actions (nightly scoring pipeline)  

---

## How to Run
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/customer-journey-analytics.git
   cd customer-journey-analytics
   ```
2. Install dependencies:
```
pip install -r requirements.txt
```
3. Generate synthetic data:
```
python src/generate_synthetic_data.py
```

4. Load data into Snowflake:
```
python src/load_to_snowflake.py
```
5. Train and score churn model:
```
python src/train_xgb.py
python src/explain_shap.py
python src/causal_effects.py
```
6. Open Power BI file (powerbi/CustomerJourney.pbix) → connect to Snowflake → explore dashboards.

---
 ## Dashboard Preview
 
- Overview → KPIs, churn risk buckets, revenue.
- Funnel Analysis → Views → Search → Cart → Checkout.
- Churn Explorer → Risk scores + SHAP top drivers.
- Causal Impact → Uplift estimates for promotions.
- What-if Simulation → Retention strategies with slicers
  
---
## License

This project is released under the MIT License.















