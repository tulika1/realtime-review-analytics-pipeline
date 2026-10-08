"""ReviewLens dashboard: reads the published gold Delta tables (never silver/audit).

    streamlit run dashboard/app.py
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from reviewlens.rag import HashingEmbedder

GOLD = Path(os.environ.get("LAKE_PATH", "/opt/lake")) / "gold"
st.set_page_config(page_title="ReviewLens", layout="wide")


@st.cache_data(ttl=60)
def load(name: str) -> pd.DataFrame:
    try:
        return DeltaTable(str(GOLD / name)).to_pandas()
    except (TableNotFoundError, FileNotFoundError):
        return pd.DataFrame()


fct = load("fct_review")
health = load("mart_product_health_daily")
topics = load("mart_topic_issues")
quality = load("_quality_runs")

st.title("ReviewLens: product health from customer reviews")
if fct.empty:
    st.info("No published gold data yet. Start the producer, then trigger the "
            "`reviewlens_pipeline` DAG in Airflow (http://localhost:8080). "
            "The dashboard refreshes every minute.")
    st.stop()

# ---- headline tiles
c1, c2, c3, c4 = st.columns(4)
c1.metric("Live reviews", f"{len(fct):,}")
c2.metric("Average rating", f"{fct['rating'].mean():.2f}")
c3.metric("Average sentiment (-1 to 1)", f"{fct['sentiment_score'].mean():+.2f}")
c4.metric("Actionable reviews", f"{100 * fct['is_actionable'].mean():.1f}%")

# ---- product health
left, right = st.columns(2)
with left:
    st.subheader("Actionable reviews by product (%)")
    by_product = (fct.groupby("product_id")["is_actionable"].mean().mul(100).round(1)
                  .sort_values(ascending=False))
    st.bar_chart(by_product, y_label="% actionable", x_label="Product")
with right:
    st.subheader("Top defect topics (actionable mentions)")
    if not topics.empty:
        top = (topics.groupby("topic")["actionable_mentions"].sum()
               .sort_values(ascending=False).head(8))
        st.bar_chart(top, horizontal=True, x_label="Topic", y_label="Mentions")

st.subheader("Reviews per day")
daily = health.groupby("review_date")["reviews"].sum().sort_index()
st.line_chart(daily, y_label="Reviews", x_label="Date")

with st.expander("Daily product health table"):
    st.dataframe(health.sort_values(["review_date", "product_id"], ascending=[False, True]),
                 hide_index=True, use_container_width=True)

# ---- semantic search over published reviews
st.subheader("Search reviews")
query = st.text_input("Ask about a problem, e.g. 'battery stops charging'")
if query:
    emb = HashingEmbedder()
    q = emb.embed(query)
    scored = fct.assign(score=[sum(a * b for a, b in zip(q, emb.embed(t)))
                               for t in fct["review_text_redacted"]])
    st.dataframe(scored.nlargest(10, "score")[
        ["review_id", "product_id", "rating", "sentiment", "topics", "review_text_redacted"]],
        hide_index=True, use_container_width=True)

# ---- data quality history (ownership: show the gate, not just the data)
st.subheader("Data quality gate: recent runs")
if not quality.empty:
    latest = quality.sort_values("run_at", ascending=False).head(12)
    latest["status"] = latest["passed"].map({True: "✅ pass", False: "❌ fail"})
    st.dataframe(latest[["run_at", "name", "value", "threshold", "severity", "status",
                         "published"]], hide_index=True, use_container_width=True)
