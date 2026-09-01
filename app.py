import requests
import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="Global Fashion Retail Executive Dashboard", page_icon="🛍️", layout="wide")
API_URL = "http://localhost:8000"

def api_get(endpoint, timeout=60):
    try:
        response = requests.get(f"{API_URL}{endpoint}", timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API Connection Error: {e}")
        return None

def check_backend():
    try:
        return requests.get(f"{API_URL}/health", timeout=10).status_code == 200
    except Exception:
        return False

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Overview & Insights", "Customer Analytics", "Churn Predictions", "Model Performance"])
backend_online = check_backend()
st.sidebar.success("Backend Connected") if backend_online else st.sidebar.error("Backend Offline")

if page == "Overview & Insights":
    st.title("🛍️ Global Fashion Retail Executive Dashboard")
    st.markdown("Analytics dashboard powered by a distributed Big Data pipeline:\n\n**Kafka → HDFS → Spark → FastAPI → Streamlit**")
    st.divider()
    if not backend_online:
        st.error("Could not connect to the FastAPI backend.")
        st.stop()
    
    with st.spinner("Loading executive summary..."):
        data = api_get("/summary")
    
    if data:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Transactions", f"{data.get('total_transactions', 0):,}")
        col2.metric("Total Revenue", f"${data.get('total_revenue', 0):,.2f}")
        col3.metric("Unique Customers", f"{data.get('unique_customers', 0):,}")
        
        col4, col5, col6 = st.columns(3)
        col4.metric("Total Profit", f"${data.get('total_profit', 0):,.2f}")
        col5.metric("Profit Margin", f"{data.get('profit_margin', 0) * 100:.2f}%")
        col6.metric("Unique Orders", f"{data.get('unique_orders', 0):,}")
        st.divider()
        st.subheader("Pipeline Architecture")
        st.info("**Data Flow**\n\nKafka Producer → Kafka Consumer → HDFS Data Lake → Apache Spark Processing → Machine Learning Models → FastAPI REST API → Streamlit Dashboard")

elif page == "Customer Analytics":
    st.title("📊 Customer Analytics")
    if not backend_online:
        st.error("Backend is offline.")
        st.stop()
    
    with st.spinner("Loading customer analytics..."):
        data = api_get("/customer-analytics")
    
    if data:
        risk_data = data.get("risk_distribution", [])
        if risk_data:
            st.subheader("Customer Risk Level Distribution")
            risk_df = pd.DataFrame(risk_data)
            chart = alt.Chart(risk_df).mark_bar().encode(x=alt.X("RiskLevel:N", title="Risk Level"), y=alt.Y("count:Q", title="Customers"), tooltip=["RiskLevel", "count"])
            st.altair_chart(chart, use_container_width=True)
        st.divider()
        
        churn_data = data.get("churn_distribution", [])
        if churn_data:
            st.subheader("Historical Customer Churn Distribution")
            churn_df = pd.DataFrame(churn_data)
            churn_df["ChurnStatus"] = churn_df["ChurnLabel"].apply(lambda x: "Churned" if x == 1 else "Not Churned")
            chart = alt.Chart(churn_df).mark_bar().encode(x=alt.X("ChurnStatus:N", title="Customer Status"), y=alt.Y("count:Q", title="Customers"), tooltip=["ChurnStatus", "count"])
            st.altair_chart(chart, use_container_width=True)

elif page == "Churn Predictions":
    st.title("⚠️ Customer Churn Predictions")
    if not backend_online:
        st.error("Backend is offline.")
        st.stop()
    
    with st.spinner("Loading churn predictions..."):
        summary = api_get("/churn-summary")
    
    if summary:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Customers", f"{summary.get('total_customers', 0):,}")
        col2.metric("Predicted to Churn", f"{summary.get('predicted_churn_customers', 0):,}")
        col3.metric("Average Churn Probability", f"{summary.get('average_churn_probability', 0) * 100:.2f}%")
        st.divider()
        
        st.subheader("Customer Risk Levels")
        risk_df = pd.DataFrame({
            "Risk Level": ["HIGH", "MEDIUM", "LOW"],
            "Customers": [summary.get("high_risk_customers", 0), summary.get("medium_risk_customers", 0), summary.get("low_risk_customers", 0)]
        })
        chart = alt.Chart(risk_df).mark_bar().encode(x=alt.X("Risk Level:N", title="Risk Level"), y=alt.Y("Customers:Q", title="Number of Customers"), tooltip=["Risk Level", "Customers"])
        st.altair_chart(chart, use_container_width=True)
    st.divider()
    
    st.subheader("🚨 Top High-Risk Customers")
    with st.spinner("Loading high-risk customers..."):
        high_risk_data = api_get("/high-risk-customers")
    
    if high_risk_data:
        high_risk_df = pd.DataFrame(high_risk_data)
        preferred_columns = ["Customer ID", "ChurnProbability", "RiskLevel", "PredictedChurn"]
        available_columns = [col for col in preferred_columns if col in high_risk_df.columns]
        if available_columns:
            st.dataframe(high_risk_df[available_columns], use_container_width=True, height=500)

elif page == "Model Performance":
    st.title("🤖 Machine Learning Model Performance")
    if not backend_online:
        st.error("Backend is offline.")
        st.stop()
    
    with st.spinner("Loading model performance..."):
        data = api_get("/model-performance")
    
    if data:
        st.success(f"🏆 Best Model: {data.get('best_model', 'Unknown')}")
        models = data.get("models", [])
        if models:
            df = pd.DataFrame(models)
            st.subheader("Model Comparison")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.divider()
            
            st.subheader("ROC-AUC Comparison")
            auc_chart = alt.Chart(df).mark_bar().encode(
                x=alt.X("name:N", title="Model"),
                y=alt.Y("roc_auc:Q", title="ROC-AUC", scale=alt.Scale(domain=[0, 1])),
                tooltip=["name", "roc_auc"]
            )
            st.altair_chart(auc_chart, use_container_width=True)
            
            st.subheader("Churn F1 Score Comparison")
            f1_chart = alt.Chart(df).mark_bar().encode(
                x=alt.X("name:N", title="Model"),
                y=alt.Y("churn_f1:Q", title="Churn F1 Score", scale=alt.Scale(domain=[0, 1])),
                tooltip=["name", "churn_f1"]
            )
            st.altair_chart(f1_chart, use_container_width=True)