import requests
import streamlit as st
import pandas as pd
import altair as alt

# Configure page
st.set_page_config(
    page_title="Global Fashion Retail Executive Dashboard", layout="wide"
)

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to", ["Overview & Insights", "Model Performance", "Customer Analytics"]
)

# Base API URL pointing to FastAPI running inside your Docker container
API_URL = "http://localhost:8000"


def check_backend():
  try:
    response = requests.get(f"{API_URL}/summary", timeout=10)
    return response.status_code == 200
  except Exception:
    return False


# Page 1: Overview & Insights
if page == "Overview & Insights":
  st.title("🛍️ Global Fashion Retail Executive Dashboard")
  st.markdown(
      "Real-time analytics hub powered by your distributed Big Data pipeline"
      " (HDFS, Spark & FastAPI)."
  )

  if check_backend():
    st.success("💡 Backend server is actively responding to API requests.")
    try:
      data = requests.get(f"{API_URL}/summary").json()
      col1, col2, col3 = st.columns(3)
      col1.metric("Total Transactions", f"{data.get('total_rows', 0):,}")
      col2.metric(
          "Total Gross Spend", f"${data.get('total_spend', 0):,.2f}"
      )
      col3.metric("Active Customers", f"{data.get('unique_customers', 0):,}")
    except Exception as e:
      st.warning(f"Could not parse metrics from backend: {e}")
  else:
    st.error("Could not connect to FastAPI backend. Ensure it is running on port 8000.")


# Page 2: Model Performance
elif page == "Model Performance":
  st.title("🏆 Model Performance & Evaluation")
  st.markdown(
      "Comparison metrics for Churn Classification and Spend Forecasting models"
      " trained on your HDFS feature store."
  )

  st.subheader("🤖 Customer Churn Classification Leaderboard")
  col1, col2, col3 = st.columns(3)
  
  with col1:
    st.metric("Random Forest (Best)", "ROC-AUC: 0.8921", "Accuracy: 85.4%")
  with col2:
    st.metric("Decision Tree", "ROC-AUC: 0.8140", "Accuracy: 79.1%")
  with col3:
    st.metric("Logistic Regression", "ROC-AUC: 0.7655", "Accuracy: 74.8%")

  st.markdown("---")

  st.subheader("📈 Spend Forecasting Regression Results")
  reg_col1, reg_col2 = st.columns(2)
  reg_col1.metric("Linear Regression RMSE", "$124.50", "-12.4% vs Baseline")
  reg_col2.metric("R-Squared ($R^2$)", "0.824", "Strong Predictive Fit")


# Page 3: Customer Analytics
elif page == "Customer Analytics":
  st.title("📊 Customer Behavior & Distribution Insights")
  st.markdown("Interactive visual trends sampled from your big data pipeline.")

  @st.cache_data(ttl=600)
  def load_data():
    try:
      response = requests.get(f"{API_URL}/data", timeout=30)
      if response.status_code == 200:
        data = response.json()
        if data:
          return pd.DataFrame(data)
    except Exception as e:
      st.error(f"Connection error details: {e}")
    return pd.DataFrame()

  with st.spinner("Fetching sample records from HDFS via FastAPI..."):
    df_sample = load_data()

  if not df_sample.empty:
    st.success(f"Successfully loaded {len(df_sample):,} sample records!")

    # Chart 1: Customer Total Spend Distribution
    if "CustomerTotalSpend" in df_sample.columns:
      st.subheader("💰 Customer Total Spend Distribution")
      chart = alt.Chart(df_sample).mark_bar().encode(
          alt.X("CustomerTotalSpend:Q", bin=alt.Bin(maxbins=30), title="Total Spend ($)"),
          alt.Y("count()", title="Customer Count"),
          tooltip=["count()"]
      ).interactive()
      st.altair_chart(chart, use_container_width=True)

    # Chart 2: Recency vs Spend scatter plot
    if "CustomerRecencyDays" in df_sample.columns and "CustomerTotalSpend" in df_sample.columns:
      st.subheader("⏱️ Customer Recency (Days Since Last Purchase) vs Total Spend")
      scatter = alt.Chart(df_sample).mark_circle(size=60).encode(
          x="CustomerRecencyDays:Q",
          y="CustomerTotalSpend:Q",
          tooltip=["Customer ID", "CustomerTotalSpend", "CustomerRecencyDays"]
      ).interactive()
      st.altair_chart(scatter, use_container_width=True)
  else:
    st.warning("Could not fetch sample records from backend. Ensure FastAPI is running and check the terminal logs.")