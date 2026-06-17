import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyathena import connect
from pyathena.pandas.util import as_pandas

# --- Page Config ---
st.set_page_config(
    page_title="Global Partners Dashboard",
    page_icon="🌍",
    layout="wide"
)

# --- AWS Athena Connection ---
def get_connection():
    return connect(
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"],
        s3_staging_dir=st.secrets["ATHENA_S3_STAGING_DIR"]
    )

@st.cache_data(ttl=3600)
def run_query(query):
    conn = get_connection()
    return as_pandas(conn.cursor().execute(query))

# --- Sidebar ---
st.sidebar.title("Global Partners")
page = st.sidebar.radio(
    "Select Dashboard",
    ["Customer Lifetime Value", "Item Options", "Top Performing Locations"]
)

st.sidebar.markdown("---")
st.sidebar.caption("Data refreshed nightly at midnight UTC")


# =============================================
# PAGE 1: CUSTOMER LIFETIME VALUE
# =============================================
if page == "Customer Lifetime Value":
    st.title("👤 Customer Lifetime Value")
    st.markdown("Analysis of customer spend, order behavior, and CLV segments.")

    df = run_query("SELECT * FROM gpdb_gold.customer_lifetime_value")

    # --- KPI Metrics ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers",       f"{len(df):,}")
    col2.metric("Total Revenue",         f"${df['total_spend'].sum():,.2f}")
    col3.metric("Avg Order Value",       f"${df['avg_order_value'].mean():,.2f}")
    col4.metric("Avg Customer Lifespan", f"{df['customer_lifespan_days'].mean():,.0f} days")

    st.markdown("---")

    # --- CLV Segment Breakdown ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("CLV Segment Distribution")
        segment_counts = df.groupby("clv_segment").size().reset_index(name="count")
        fig = px.pie(
            segment_counts,
            values="count",
            names="clv_segment",
            color="clv_segment",
            color_discrete_map={"High": "#2ecc71", "Medium": "#f39c12", "Low": "#e74c3c"},
            hole=0.4
        )
        fig.update_layout(showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Spend by CLV Segment")
        segment_spend = df.groupby("clv_segment")["total_spend"].mean().reset_index()
        segment_spend.columns = ["clv_segment", "avg_spend"]
        segment_spend = segment_spend.sort_values("avg_spend", ascending=False)
        fig = px.bar(
            segment_spend,
            x="clv_segment",
            y="avg_spend",
            color="clv_segment",
            color_discrete_map={"High": "#2ecc71", "Medium": "#f39c12", "Low": "#e74c3c"},
            labels={"avg_spend": "Avg Total Spend ($)", "clv_segment": "Segment"}
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Order Frequency ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Avg Orders per Customer by Segment")
        segment_orders = df.groupby("clv_segment")["total_orders"].mean().reset_index()
        segment_orders.columns = ["clv_segment", "avg_orders"]
        fig = px.bar(
            segment_orders,
            x="clv_segment",
            y="avg_orders",
            color="clv_segment",
            color_discrete_map={"High": "#2ecc71", "Medium": "#f39c12", "Low": "#e74c3c"},
            labels={"avg_orders": "Avg Orders", "clv_segment": "Segment"}
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Days Between Orders by Segment")
        segment_cadence = df.groupby("clv_segment")["avg_days_between_orders"].mean().reset_index()
        segment_cadence.columns = ["clv_segment", "avg_days"]
        fig = px.bar(
            segment_cadence,
            x="clv_segment",
            y="avg_days",
            color="clv_segment",
            color_discrete_map={"High": "#2ecc71", "Medium": "#f39c12", "Low": "#e74c3c"},
            labels={"avg_days": "Avg Days Between Orders", "clv_segment": "Segment"}
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Currency Breakdown ---
    st.subheader("Total Revenue by Currency")
    currency_df = df.groupby("currency")["total_spend"].sum().reset_index()
    currency_df.columns = ["currency", "total_revenue"]
    currency_df = currency_df.sort_values("total_revenue", ascending=False)
    fig = px.bar(
        currency_df,
        x="currency",
        y="total_revenue",
        labels={"total_revenue": "Total Revenue", "currency": "Currency"}
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Raw Data Table ---
    st.subheader("Customer Data")
    segment_filter = st.multiselect(
        "Filter by Segment",
        options=df["clv_segment"].unique().tolist(),
        default=df["clv_segment"].unique().tolist()
    )
    filtered_df = df[df["clv_segment"].isin(segment_filter)]
    st.dataframe(filtered_df, use_container_width=True)


# =============================================
# PAGE 2: ITEM OPTIONS
# =============================================
elif page == "Item Options":
    st.title("🍽️ Item Options Analysis")
    st.markdown("Analysis of the most added and removed options per menu item.")

    df = run_query("SELECT * FROM gpdb_gold.item_options")

    # --- Clean column types ---
    df["total_added_count"]   = pd.to_numeric(df["total_added_count"],   errors="coerce").fillna(0).astype(int)
    df["total_removed_count"] = pd.to_numeric(df["total_removed_count"], errors="coerce").fillna(0).astype(int)
    


    # --- KPI Metrics ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Menu Items",    f"{df['item_name'].nunique():,}")
    col2.metric("Total Option Types",  f"{df['option_name'].nunique():,}")
    col3.metric("Total Options Added", f"{df['total_added_count'].sum():,}")

    st.markdown("---")

    # --- Item selector ---
    st.subheader("Option Analysis by Item")
    selected_item = st.selectbox(
        "Select a Menu Item",
        options=sorted(df["item_name"].dropna().unique().tolist())
    )

    item_df = df[df["item_name"] == selected_item].sort_values("total_added_count", ascending=False)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Most Added Options — {selected_item}")
        fig = px.bar(
            item_df.sort_values("total_added_count", ascending=True).tail(10),
            x="total_added_count",
            y="option_name",
            orientation="h",
            labels={"total_added_count": "Times Added", "option_name": "Option"},
            color_discrete_sequence=["#2ecc71"]
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader(f"Most Removed Options — {selected_item}")
        fig = px.bar(
            item_df.sort_values("total_removed_count", ascending=True).tail(10),
            x="total_removed_count",
            y="option_name",
            orientation="h",
            labels={"total_removed_count": "Times Removed", "option_name": "Option"},
            color_discrete_sequence=["#e74c3c"]
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Top 10 Most Added Options Across All Items ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Most Added Options (All Items)")
        top_added = df.groupby("option_name")["total_added_count"].sum().reset_index()
        top_added = top_added.sort_values("total_added_count", ascending=True).tail(10)
        fig = px.bar(
            top_added,
            x="total_added_count",
            y="option_name",
            orientation="h",
            labels={"total_added_count": "Times Added", "option_name": "Option"},
            color_discrete_sequence=["#2ecc71"]
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 10 Most Removed Options (All Items)")
        top_removed = df.groupby("option_name")["total_removed_count"].sum().reset_index()
        top_removed = top_removed.sort_values("total_removed_count", ascending=True).tail(10)
        fig = px.bar(
            top_removed,
            x="total_removed_count",
            y="option_name",
            orientation="h",
            labels={"total_removed_count": "Times Removed", "option_name": "Option"},
            color_discrete_sequence=["#e74c3c"]
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Raw Data Table ---
    st.subheader("Raw Data")
    st.dataframe(df, use_container_width=True)


# =============================================
# PAGE 3: TOP PERFORMING LOCATIONS
# =============================================
elif page == "Top Performing Locations":
    st.title("📍 Top Performing Locations")
    st.markdown("Restaurant performance ranked by total revenue within each currency.")

    df = run_query("SELECT * FROM gpdb_gold.top_performing_locations")

    # --- Currency Filter ---
    currencies = sorted(df["currency"].unique().tolist())
    selected_currency = st.selectbox("Select Currency", options=currencies)
    filtered_df = df[df["currency"] == selected_currency].sort_values("revenue_rank")

    # --- KPI Metrics ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Restaurants",  f"{len(filtered_df):,}")
    col2.metric("Total Revenue",      f"${filtered_df['total_revenue'].sum():,.2f}")
    col3.metric("Avg Order Value",    f"${filtered_df['avg_order_value'].mean():,.2f}")
    col4.metric("Avg Daily Orders",   f"{filtered_df['avg_daily_orders'].mean():,.1f}")

    st.markdown("---")

    # --- Top 10 Restaurants by Revenue ---
    st.subheader(f"Top 10 Restaurants by Revenue ({selected_currency})")
    top10_df = filtered_df.head(10)
    fig = px.bar(
        top10_df,
        x="restaurant_id",
        y="total_revenue",
        labels={"total_revenue": "Total Revenue", "restaurant_id": "Restaurant"},
        color="total_revenue",
        color_continuous_scale="Greens"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Avg Order Value — Top 10 ({selected_currency})")
        fig = px.bar(
            top10_df,
            x="restaurant_id",
            y="avg_order_value",
            labels={"avg_order_value": "Avg Order Value", "restaurant_id": "Restaurant"},
            color_discrete_sequence=["#3498db"]
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader(f"Avg Daily Orders — Top 10 ({selected_currency})")
        fig = px.bar(
            top10_df,
            x="restaurant_id",
            y="avg_daily_orders",
            labels={"avg_daily_orders": "Avg Daily Orders", "restaurant_id": "Restaurant"},
            color_discrete_sequence=["#9b59b6"]
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Revenue vs Avg Order Value Scatter ---
    st.subheader(f"Revenue vs Avg Order Value ({selected_currency})")
    fig = px.scatter(
        filtered_df,
        x="avg_order_value",
        y="total_revenue",
        hover_data=["restaurant_id", "total_orders"],
        labels={
            "avg_order_value": "Avg Order Value",
            "total_revenue": "Total Revenue"
        },
        color="total_revenue",
        color_continuous_scale="Greens"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Full Rankings Table ---
    st.subheader(f"Full Rankings ({selected_currency})")
    st.dataframe(
        filtered_df[[
            "revenue_rank", "restaurant_id", "total_revenue",
            "avg_order_value", "total_orders", "avg_daily_orders", "avg_weekly_orders"
        ]],
        use_container_width=True
    )