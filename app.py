import os
import pandas as pd
import plotly.express as px
import streamlit as st

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
st.set_page_config(page_title="Olist Analytics", layout="wide")


@st.cache_data
def load(name):
    return pd.read_csv(os.path.join(OUT, name))


st.title("Olist E-Commerce Analytics")
st.caption("Orders from 2016 to 2018, analysed with Apache Spark")

tab_overview, tab_delivery = st.tabs(["Overview", "Delivery"])

with tab_overview:
    k = load("kpis.csv").iloc[0]
    c = st.columns(4)
    c[0].metric("Orders", f"{int(k.orders):,}")
    c[1].metric("Unique customers", f"{int(k.unique_customers):,}")
    c[2].metric("Revenue (BRL)", f"{k.revenue:,.0f}")
    c[3].metric("Average review", f"{k.avg_review:.2f} / 5")

    monthly = load("monthly.csv")
    left, right = st.columns(2)
    left.plotly_chart(px.bar(monthly, x="month", y="orders", title="Orders per month"))
    right.plotly_chart(px.line(monthly, x="month", y="revenue", markers=True,
                               title="Revenue per month (BRL)"))

    cat = load("category_revenue.csv").sort_values("revenue")
    st.plotly_chart(px.bar(cat, x="revenue", y="category", orientation="h",
                           title="Top 10 product categories by revenue (BRL)"))

with tab_delivery:
    k = load("kpis.csv").iloc[0]
    c = st.columns(2)
    c[0].metric("Average delivery time", f"{k.avg_delivery_days:.1f} days")
    c[1].metric("Late orders", f"{k.late_pct:.1f}%")

    rev = load("review_by_delivery.csv")
    fig = px.bar(rev, x="delivery_status", y="avg_review", text="avg_review",
                 title="Average review score: on time vs late")
    fig.update_yaxes(range=[0, 5])
    st.plotly_chart(fig)

    states = load("delivery_by_state.csv")
    states = states[states.orders >= 200].sort_values("late_pct", ascending=False)
    st.plotly_chart(px.bar(states, x="customer_state", y="late_pct",
                           title="Share of late deliveries by customer state (%)"))
    st.caption("States with fewer than 200 orders are hidden because their percentages "
               "are unreliable.")