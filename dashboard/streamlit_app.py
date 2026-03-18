import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from groq import Groq
from pydantic import BaseModel
from typing import List
import json
import os
import random
from datetime import datetime, timedelta

# --- Config ---
st.set_page_config(
    page_title="Smart DQ Assistant",
    page_icon="🔍",
    layout="wide"
)

os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")  # paste your key
client = Groq()

# --- Pydantic Models ---
class RootCauseAnalysis(BaseModel):
    failed_check: str
    column: str
    explanation: str
    confidence: float
    recommended_investigation: str
    severity: str

# --- Mock DQ Results (simulating pipeline output) ---
def get_mock_dq_results():
    checks = [
        {"check": "expect_not_null", "column": "payment_method", "status": "FAIL",
         "metric": 0.34, "timestamp": "2026-03-17 10:00:00", "batch": "batch_3"},
        {"check": "expect_unique", "column": "order_id", "status": "PASS",
         "metric": 0.0, "timestamp": "2026-03-17 10:00:00", "batch": "batch_3"},
        {"check": "expect_value_range", "column": "order_amount", "status": "PASS",
         "metric": 0.0, "timestamp": "2026-03-17 10:00:00", "batch": "batch_3"},
        {"check": "expect_accepted_values", "column": "product_category", "status": "PASS",
         "metric": 0.0, "timestamp": "2026-03-17 10:00:00", "batch": "batch_3"},
        {"check": "expect_not_null", "column": "order_id", "status": "PASS",
         "metric": 0.0, "timestamp": "2026-03-17 09:00:00", "batch": "batch_2"},
        {"check": "expect_not_null", "column": "payment_method", "status": "PASS",
         "metric": 0.01, "timestamp": "2026-03-17 09:00:00", "batch": "batch_2"},
        {"check": "expect_value_range", "column": "customer_age", "status": "PASS",
         "metric": 0.0, "timestamp": "2026-03-17 08:00:00", "batch": "batch_1"},
        {"check": "expect_accepted_values", "column": "delivery_status", "status": "FAIL",
         "metric": 0.07, "timestamp": "2026-03-17 08:00:00", "batch": "batch_1"},
    ]
    return pd.DataFrame(checks)

# --- Failure trend data ---
def get_failure_trend():
    dates = pd.date_range(end=datetime.today(), periods=14)
    return pd.DataFrame({
        "date": dates,
        "pass_rate": [random.uniform(0.85, 1.0) for _ in range(14)]
    })

# --- LLM Root Cause ---
def explain_failure(failed_check, column, current_stats, previous_stats):
    prompt = f"""You are a senior data engineer performing root cause analysis.

FAILED CHECK: {failed_check}
COLUMN: {column}
PREVIOUS STATS: {json.dumps(previous_stats)}
CURRENT STATS: {json.dumps(current_stats)}

Return ONLY JSON:
{{
  "failed_check": "{failed_check}",
  "column": "{column}",
  "explanation": "detailed explanation",
  "confidence": 0.85,
  "recommended_investigation": "specific steps",
  "severity": "HIGH"
}}"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    raw = response.choices[0].message.content.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    return RootCauseAnalysis.model_validate_json(raw)

# --- LLM Rule Generator ---
def generate_rules(profile_text):
    prompt = f"""You are a data quality engineer.
Given this data profile, suggest DQ rules.
Profile: {profile_text}

ALLOWED RULE TYPES: not_null, value_range, unique, accepted_values, null_rate

Return ONLY JSON:
{{
  "rules": [
    {{
      "column": "column_name",
      "rule_type": "rule_type",
      "parameters": {{}},
      "reasoning": "why this rule"
    }}
  ]
}}"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    return response.choices[0].message.content.strip()

# =====================
# DASHBOARD UI
# =====================

st.title("🔍 Smart Data Quality Assistant")
st.caption("AI-powered data quality monitoring for e-commerce pipelines")

# --- Top Metrics ---
df = get_mock_dq_results()
total = len(df)
passed = len(df[df["status"] == "PASS"])
failed = len(df[df["status"] == "FAIL"])
pass_rate = round(passed / total * 100, 1)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Checks", total)
col2.metric("Passed", passed)
col3.metric("Failed", failed, delta=f"-{failed}", delta_color="inverse")
col4.metric("Pass Rate", f"{pass_rate}%")

st.divider()

# --- Two column layout ---
left, right = st.columns(2)

# Panel 1 — DQ Check History
with left:
    st.subheader("📋 DQ Check History")
    def color_status(val):
        color = "green" if val == "PASS" else "red"
        return f"color: {color}; font-weight: bold"
    styled = df.style.applymap(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True)

# Panel 2 — Failure Trend
with right:
    st.subheader("📈 Pass Rate Trend (14 days)")
    trend_df = get_failure_trend()
    fig = px.line(
        trend_df, x="date", y="pass_rate",
        labels={"pass_rate": "Pass Rate", "date": "Date"}
    )
    fig.update_layout(yaxis_tickformat=".0%", height=300)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Panel 3 — Top Failing Columns
st.subheader("⚠️ Failing Checks")
failed_df = df[df["status"] == "FAIL"]
if len(failed_df) > 0:
    st.dataframe(failed_df, use_container_width=True)
else:
    st.success("No failures detected")

st.divider()

# Panel 4 — LLM Explanation Viewer
st.subheader("🤖 LLM Root Cause Analysis")
failed_checks = df[df["status"] == "FAIL"]

if len(failed_checks) > 0:
    selected = st.selectbox(
        "Select a failed check to analyze:",
        options=failed_checks.apply(
            lambda r: f"{r['batch']} | {r['column']} | {r['check']}", axis=1
        ).tolist()
    )

    if st.button("🔍 Analyze Root Cause"):
        with st.spinner("Analyzing with AI..."):
            parts = selected.split(" | ")
            column = parts[1]
            check = parts[2]

            current = {"null_rate": 0.34, "batch_date": "2026-03-17", "total_rows": 10000}
            previous = {"null_rate": 0.01, "batch_date": "2026-03-16", "total_rows": 9800}

            analysis = explain_failure(check, column, current, previous)

            severity_color = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
            icon = severity_color.get(analysis.severity, "⚪")

            st.markdown(f"### {icon} Severity: {analysis.severity}")
            st.markdown(f"**Confidence:** {analysis.confidence:.0%}")
            st.markdown("**Explanation:**")
            st.info(analysis.explanation)
            st.markdown("**Recommended Investigation:**")
            st.warning(analysis.recommended_investigation)
else:
    st.success("No failures to analyze")

st.divider()

# Panel 5 — Rule Generator
st.subheader("⚙️ LLM Rule Generator")
st.caption("Paste a data profile and get AI-suggested DQ rules")

default_profile = """{
  "order_amount": {"min": 10, "max": 5000, "null_rate": 0.0},
  "payment_method": {"top_values": ["UPI", "Card", "COD"], "null_rate": 0.15},
  "product_category": {"top_values": ["Electronics", "Clothing"], "null_rate": 0.0}
}"""

profile_input = st.text_area("Data Profile (JSON):", value=default_profile, height=150)

if st.button("🧠 Generate Rules"):
    with st.spinner("Generating rules with AI..."):
        raw_rules = generate_rules(profile_input)
        try:
            rules_json = json.loads(raw_rules)
            rules_df = pd.DataFrame(rules_json["rules"])
            st.dataframe(rules_df, use_container_width=True)
        except:
            st.code(raw_rules)