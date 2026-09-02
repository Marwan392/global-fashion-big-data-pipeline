import requests
import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="Global Fashion Retail", page_icon="🛍️", layout="wide")

API = "http://localhost:8000"

SQL_QUERY_ORDER = [
    ("query_1", "Basic Data Overview"),
    ("query_2", "Total Sales & Profit"),
    ("query_3", "Sales by Year"),
    ("query_4", "Monthly Sales Trend"),
    ("query_5", "Sales by Product Category"),
    ("query_6", "Sales by Product Sub-Category"),
    ("query_7", "Top 10 Products by Revenue"),
    ("query_8", "Sales by Customer Country"),
    ("query_9", "Sales by Store Country"),
    ("query_10", "Payment Method Analysis"),
    ("query_11", "Customer Gender Analysis"),
    ("query_12", "Weekday vs Weekend"),
    ("query_13", "Discount Analysis"),
    ("query_14", "Top 10 Customers by Revenue"),
    ("query_15", "Top 10 Stores by Revenue"),
]


@st.cache_data(ttl=15, show_spinner=False)
def backend_online():
    try:
        return requests.get(f"{API}/health", timeout=5).ok
    except requests.RequestException:
        return False


@st.cache_data(ttl=120, show_spinner=False)
def api(endpoint, timeout=180):
    try:
        r = requests.get(f"{API}{endpoint}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Backend request failed: {exc}") from exc


@st.cache_data(ttl=600, show_spinner=False)
def sql_analytics():
    try:
        r = requests.get(f"{API}/sql-analytics", timeout=300)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"SQL analytics request failed: {exc}") from exc



def show_api_error(exc):
    st.error("Unable to load this section from the FastAPI backend.")
    st.caption(f"Details: {exc}")


# Sidebar
st.sidebar.title("Global Fashion Retail")
st.sidebar.caption("Big Data Analytics Platform")

page = st.sidebar.radio(
    "Navigation",
    [
        "Overview & Insights",
        "SQL Analytics",
        "Customer Analytics",
        "Churn Predictions",
        "Model Performance",
    ],
)

online = backend_online()
(st.sidebar.success if online else st.sidebar.error)(
    "Backend Connected" if online else "Backend Offline"
)

if not online:
    st.error(f"FastAPI backend is unavailable at {API}.")
    st.stop()


# Overview
if page == "Overview & Insights":
    st.title("Global Fashion Retail")
    st.caption("Executive Analytics & Customer Intelligence")

    try:
        summary = api("/summary")
        churn = api("/churn-summary")
    except RuntimeError as exc:
        show_api_error(exc)
        st.stop()

    metrics = [
        ("Revenue", f"${summary['total_revenue']:,.0f}"),
        ("Profit", f"${summary['total_profit']:,.0f}"),
        ("Customers", f"{summary['unique_customers']:,}"),
        ("Orders", f"{summary['unique_orders']:,}"),
        ("Transactions", f"{summary['total_transactions']:,}"),
        ("Margin", f"{summary['profit_margin'] * 100:.1f}%"),
    ]

    for col, (label, value) in zip(st.columns(6), metrics):
        col.metric(label, value)

    st.divider()

    avg_order = (
        summary["total_revenue"] / summary["unique_orders"]
        if summary["unique_orders"] else 0
    )
    churn_rate = (
        churn["predicted_churn_customers"] / churn["total_customers"]
        if churn["total_customers"] else 0
    )

    left, right = st.columns([1.4, 1])

    with left:
        st.subheader("Business Snapshot")
        st.write(
            f"**${summary['total_revenue']:,.0f}** in revenue generated "
            f"across **{summary['total_transactions']:,} transactions**."
        )
        st.write(
            f"Profit reached **${summary['total_profit']:,.0f}**, "
            f"representing a **{summary['profit_margin'] * 100:.1f}% margin**."
        )
        st.write(
            f"The platform covers **{summary['unique_customers']:,} customers** "
            f"and **{summary['unique_orders']:,} orders**."
        )

    with right:
        st.subheader("Key Indicators")
        st.metric("Average Order Value", f"${avg_order:,.2f}")
        st.metric("Predicted Churn Rate", f"{churn_rate * 100:.1f}%")

    st.divider()
    st.subheader("Customer Retention Overview")

    risk = pd.DataFrame({
        "Risk Level": ["HIGH", "MEDIUM", "LOW"],
        "Customers": [
            churn["high_risk_customers"],
            churn["medium_risk_customers"],
            churn["low_risk_customers"],
        ],
    })

    chart = (
        alt.Chart(risk)
        .mark_bar()
        .encode(
            x=alt.X("Risk Level:N", sort=["HIGH", "MEDIUM", "LOW"], title=None),
            y=alt.Y("Customers:Q", title="Customers"),
            tooltip=["Risk Level", "Customers"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)

    high = churn["high_risk_customers"]
    if high:
        st.warning(
            f"{high:,} customers are currently classified as high risk "
            "and may require retention actions."
        )
    else:
        st.success("No customers are currently classified as high risk.")

    with st.expander("Technical Architecture"):
        st.markdown(
            """
**Kafka → HDFS → Apache Spark → Machine Learning → FastAPI → Streamlit**

- **Kafka:** transaction ingestion
- **HDFS:** distributed data storage
- **Apache Spark:** ETL and feature engineering
- **Machine Learning:** churn prediction and segmentation
- **FastAPI:** analytics and ML serving
- **Streamlit:** interactive visualization
"""
        )


# SQL Analytics
elif page == "SQL Analytics":
    st.title("SQL Analytics")
    st.caption("Spark SQL results generated from analytics.sql")

    try:
        health = api("/sql-health", timeout=30)
    except RuntimeError as exc:
        show_api_error(exc)
        st.stop()

    if health["select_queries"] != health["expected_queries"]:
        st.error(
            f"analytics.sql mismatch: found {health['select_queries']} queries, "
            f"expected {health['expected_queries']}."
        )
        st.stop()

    st.info(
        "Loading Spark SQL analytics. The first load may take some time "
        "while Spark executes the queries."
    )

    with st.spinner("Running Spark SQL analytics... Please wait."):
        try:
            payload = sql_analytics()
        except RuntimeError as exc:
            show_api_error(exc)
            st.stop()

    queries = payload.get("queries", {})
    available = [(k, t) for k, t in SQL_QUERY_ORDER if k in queries]

    if not queries:
        st.warning("No SQL analytics results were returned by the backend.")
        st.stop()

    if not available:
        st.error("The backend returned SQL results, but none of the expected query keys were found.")
        st.caption(f"Returned keys: {', '.join(queries.keys())}")
        st.stop()

    labels = dict(available)
    selected_key = st.selectbox(
        "Select an analysis",
        list(labels),
        format_func=lambda key: labels[key],
    )

    result = queries[selected_key]
    st.subheader(result.get("title", labels[selected_key]))

    df = pd.DataFrame(result.get("rows", []))

    if df.empty:
        st.info("This query returned no rows.")
        st.stop()

    st.caption(f"{len(df):,} rows returned")
    st.dataframe(df, use_container_width=True, hide_index=True, height=420)

    numeric = df.select_dtypes(include="number").columns.tolist()

    if len(df.columns) >= 2 and numeric:
        value = numeric[-1]
        category = df.columns[0]

        if selected_key == "query_3":
            chart = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X(f"{category}:N", title=category),
                    y=alt.Y(f"{value}:Q", title=value),
                    tooltip=list(df.columns),
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)

        elif selected_key == "query_4":
            chart_df = df.copy()
            try:
                chart_df[category] = pd.to_datetime(chart_df[category])
                x_type = "T"
            except (ValueError, TypeError):
                x_type = "N"

            chart = (
                alt.Chart(chart_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X(f"{category}:{x_type}", title=category),
                    y=alt.Y(f"{value}:Q", title=value),
                    tooltip=list(df.columns),
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)

        elif selected_key in {"query_5", "query_10", "query_13"}:
            chart = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X(f"{category}:N", sort="-y", title=category),
                    y=alt.Y(f"{value}:Q", title=value),
                    tooltip=list(df.columns),
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)


# Customer Analytics
elif page == "Customer Analytics":
    st.title("Customer Analytics")
    st.caption("Customer risk and observed churn distribution")

    try:
        data = api("/customer-analytics")
    except RuntimeError as exc:
        show_api_error(exc)
        st.stop()

    risk = pd.DataFrame(data["risk_distribution"])
    churn = pd.DataFrame(data["churn_distribution"])

    cols = st.columns(3)
    for col, level in zip(cols, ["HIGH", "MEDIUM", "LOW"]):
        value = risk.loc[risk["RiskLevel"] == level, "count"].sum()
        col.metric(f"{level} Risk", f"{value:,}")

    st.divider()
    st.subheader("Customer Risk Distribution")

    risk_chart = (
        alt.Chart(risk)
        .mark_bar()
        .encode(
            x=alt.X("RiskLevel:N", sort=["HIGH", "MEDIUM", "LOW"]),
            y=alt.Y("count:Q", title="Customers"),
            tooltip=["RiskLevel", "count"],
        )
        .properties(height=350)
    )
    st.altair_chart(risk_chart, use_container_width=True)

    st.subheader("Observed Churn Distribution")

    churn["Status"] = churn["ChurnLabel"].map({
        1: "Churned",
        0: "Not Churned",
    })

    churn_chart = (
        alt.Chart(churn)
        .mark_bar()
        .encode(
            x="Status:N",
            y=alt.Y("count:Q", title="Customers"),
            tooltip=["Status", "count"],
        )
        .properties(height=350)
    )
    st.altair_chart(churn_chart, use_container_width=True)


# Churn Predictions
elif page == "Churn Predictions":
    st.title("Customer Churn Predictions")
    st.caption("ML-driven customer retention risk")

    try:
        data = api("/churn-summary")
    except RuntimeError as exc:
        show_api_error(exc)
        st.stop()

    total = data["total_customers"]
    predicted = data["predicted_churn_customers"]

    cols = st.columns(4)
    cols[0].metric("Customers", f"{total:,}")
    cols[1].metric("Predicted Churn", f"{predicted:,}")
    cols[2].metric(
        "Churn Rate",
        f"{predicted / total * 100:.1f}%" if total else "0%",
    )
    cols[3].metric(
        "Avg. Probability",
        f"{data['average_churn_probability'] * 100:.1f}%",
    )

    st.divider()

    risk = pd.DataFrame({
        "Risk": ["HIGH", "MEDIUM", "LOW"],
        "Customers": [
            data["high_risk_customers"],
            data["medium_risk_customers"],
            data["low_risk_customers"],
        ],
    })

    risk_chart = (
        alt.Chart(risk)
        .mark_bar()
        .encode(
            x=alt.X("Risk:N", sort=["HIGH", "MEDIUM", "LOW"]),
            y="Customers:Q",
            tooltip=["Risk", "Customers"],
        )
        .properties(height=350)
    )
    st.altair_chart(risk_chart, use_container_width=True)

    st.subheader("Top High-Risk Customers")

    try:
        df = pd.DataFrame(api("/high-risk-customers"))
    except RuntimeError as exc:
        show_api_error(exc)
        st.stop()

    if not df.empty:
        if "ChurnProbability" in df.columns:
            df["ChurnProbability"] = (
                df["ChurnProbability"] * 100
            ).round(2).astype(str) + "%"

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=450,
        )


# Model Performance
else:
    st.title("Machine Learning Performance")
    st.caption("Customer churn classification models")

    try:
        data = api("/model-performance")
    except RuntimeError as exc:
        show_api_error(exc)
        st.stop()

    st.success(f"Best Model: {data['best_model']}")

    df = pd.DataFrame(data["models"])
    display = df.copy()

    metrics = [
        "roc_auc", "f1", "precision", "recall",
        "churn_precision", "churn_recall", "churn_f1",
    ]

    for metric in metrics:
        display[metric] = (display[metric] * 100).round(2)

    st.dataframe(display, use_container_width=True, hide_index=True)

    chart_df = df.melt(
        id_vars="name",
        value_vars=["roc_auc", "churn_f1"],
        var_name="Metric",
        value_name="Score",
    )

    st.subheader("Model Comparison")

    model_chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x="name:N",
            y=alt.Y("Score:Q", scale=alt.Scale(domain=[0, 1])),
            color="Metric:N",
            tooltip=["name", "Metric", "Score"],
        )
        .properties(height=400)
    )
    st.altair_chart(model_chart, use_container_width=True)