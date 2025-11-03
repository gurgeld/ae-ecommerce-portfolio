import duckdb
from pathlib import Path
import pandas as pd
import streamlit as st

# -----------------------------
# Paths & Connections
# -----------------------------
REPO_ROOT = Path(__file__).resolve().parent
DB_PATH = REPO_ROOT / "data" / "olist.duckdb"

@st.cache_resource(show_spinner=False)
def get_conn():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found at {DB_PATH}. Run the ingestion first: python src/elt/ingestor.py")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    return con

@st.cache_data(show_spinner=False)
def get_date_bounds():
    con = get_conn()
    q = """
        SELECT
          date_trunc('day', min(order_purchase_timestamp)) AS min_date,
          date_trunc('day', max(order_purchase_timestamp)) AS max_date
        FROM raw.orders
    """
    return con.execute(q).df().iloc[0]

@st.cache_data(show_spinner=False)
def get_geo_filters():
    con = get_conn()
    q = """
        SELECT DISTINCT customer_state AS state
        FROM raw.customers
        WHERE customer_state IS NOT NULL
        ORDER BY 1
    """
    return [r[0] for r in con.execute(q).fetchall()]

@st.cache_data(show_spinner=True)
def load_overview(start_date: str, end_date: str, states: list[str]):
    con = get_conn()
    states_filter = ",".join([f"'{s}'" for s in states]) if states else None

    base_cte = f"""
    WITH base AS (
        SELECT
            o.order_id,
            date_trunc('day', o.order_purchase_timestamp) AS dt,
            c.customer_state,
            oi.price + oi.freight_value AS revenue,
            oi.price AS item_value,
            oi.freight_value AS freight_value
        FROM raw.orders o
        JOIN raw.order_items oi USING(order_id)
        JOIN raw.customers c USING(customer_id)
        WHERE o.order_purchase_timestamp::date BETWEEN '{start_date}' AND '{end_date}'
        {f"AND c.customer_state IN ({states_filter})" if states_filter else ''}
    )
    """

    kpis_q = base_cte + """
    SELECT
        COUNT(DISTINCT order_id) AS orders,
        COUNT(*) AS items,
        SUM(revenue) AS revenue,
        AVG(revenue) AS avg_item_revenue,
        SUM(item_value) AS item_value,
        SUM(freight_value) AS freight_value
    FROM base
    """

    series_q = base_cte + """
    SELECT dt::date AS d, SUM(revenue) AS revenue, COUNT(DISTINCT order_id) AS orders
    FROM base
    GROUP BY 1
    ORDER BY 1
    """

    pm_q = f"""
    WITH payments AS (
      SELECT p.order_id, p.payment_type, p.payment_value
      FROM raw.order_payments p
    ), orders_in_range AS (
      SELECT DISTINCT o.order_id
      FROM raw.orders o
      WHERE o.order_purchase_timestamp::date BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT payment_type, SUM(payment_value) AS value
    FROM payments p
    JOIN orders_in_range o USING(order_id)
    GROUP BY 1
    ORDER BY 2 DESC
    """

    by_state_q = base_cte + """
    SELECT customer_state, SUM(revenue) AS revenue
    FROM base
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 27
    """

    kpis = con.execute(kpis_q).df().iloc[0].to_dict()
    series = con.execute(series_q).df()
    paymix = con.execute(pm_q).df()
    by_state = con.execute(by_state_q).df()
    return kpis, series, paymix, by_state

@st.cache_data(show_spinner=True)
def load_customers(start_date: str, end_date: str, states: list[str]):
    con = get_conn()
    states_filter = ",".join([f"'{s}'" for s in states]) if states else None
    q = f"""
    WITH orders AS (
        SELECT order_id, customer_id, date_trunc('day', order_purchase_timestamp) AS dt
        FROM raw.orders
        WHERE order_purchase_timestamp::date BETWEEN '{start_date}' AND '{end_date}'
    ), revenue AS (
        SELECT order_id, SUM(price + freight_value) AS revenue
        FROM raw.order_items
        GROUP BY 1
    )
    SELECT c.customer_unique_id,
           c.customer_city,
           c.customer_state,
           COUNT(DISTINCT o.order_id) AS orders,
           SUM(r.revenue) AS revenue
    FROM raw.customers c
    JOIN orders o USING(customer_id)
    JOIN revenue r USING(order_id)
    {f"WHERE c.customer_state IN ({states_filter})" if states_filter else ''}
    GROUP BY 1,2,3
    HAVING SUM(r.revenue) IS NOT NULL
    ORDER BY revenue DESC
    LIMIT 1000
    """
    return con.execute(q).df()

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="E-commerce Analytics (Olist)", layout="wide")
st.title("E-commerce Analytics — Olist")
st.caption("Source: Kaggle 'Brazilian E-Commerce Public Dataset by Olist'")

# Sidebar filters
bounds = get_date_bounds()
col1, col2 = st.sidebar.columns(2)
start_date = col1.date_input("Start", bounds["min_date"].date(), min_value=bounds["min_date"].date(), max_value=bounds["max_date"].date())
end_date = col2.date_input("End", bounds["max_date"].date(), min_value=bounds["min_date"].date(), max_value=bounds["max_date"].date())

states = st.sidebar.multiselect("Filter by State (UF)", get_geo_filters())

st.sidebar.info("Run ingestion first if data isn't available: `python src/elt/ingestor.py`\nThen build dbt models: `cd _dbt && dbt build`.")

# Tabs
overview_tab, customers_tab = st.tabs(["Overview", "Customers"])

with overview_tab:
    kpis, series, paymix, by_state = load_overview(str(start_date), str(end_date), states)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Revenue", f"R$ {kpis['revenue']:.0f}")
    m2.metric("Orders", f"{int(kpis['orders'])}")
    aov = (kpis['revenue'] / kpis['orders']) if kpis['orders'] else 0
    m3.metric("AOV", f"R$ {aov:.2f}")
    m4.metric("Freight Share", f"{(kpis['freight_value']/kpis['revenue']*100 if kpis['revenue'] else 0):.1f}%")

    c1, c2 = st.columns([2,1])
    with c1:
        st.subheader("Revenue over time")
        st.line_chart(series.set_index("d")["revenue"])  # Streamlit native chart

    with c2:
        st.subheader("Payment Mix")
        st.bar_chart(paymix.set_index("payment_type")["value"])  # native chart

    st.subheader("Revenue by State (Top 27)")
    st.bar_chart(by_state.set_index("customer_state")["revenue"])  # native chart

with customers_tab:
    df = load_customers(str(start_date), str(end_date), states)
    st.write("Top Customers by Revenue (sampled/limited)")
    st.dataframe(df, use_container_width=True)

st.caption("Built with Streamlit + DuckDB + dbt. © Your Name")
