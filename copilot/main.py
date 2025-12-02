import os
import logging
import sqlite3
from datetime import date, timedelta
from typing import Optional, Tuple, Dict, Any
import json
import urllib.parse

import msal
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from urllib.parse import quote
from openai import AzureOpenAI
from functools import lru_cache
import time

CACHE_TTL = 3600  # cache for 5 minutes
_last_load_time = 0
_cached_connection = None

# ------------------------------------------------------
# Load ENV
# ------------------------------------------------------
load_dotenv()

# ------------------------------------------------------
# Logging
# ------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------
# ENV CONFIG
# ------------------------------------------------------
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")

TABLE_COMPANY = os.getenv("TABLE_COMPANY", "mserp_ledgerbientities")
TABLE_VENDTRANS = os.getenv("TABLE_VENDTRANS", "mserp_vendtransbientity")
TABLE_VENDTABLE = os.getenv("TABLE_VENDTABLE", "mserp_vendtablebientity")
TABLE_PARTYTABLE = os.getenv("TABLE_PARTYTABLE", "mserp_dirpartytablebientity")

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, DATAVERSE_URL,
            AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY]):
    logger.warning("Some required environment variables are missing.")

# ------------------------------------------------------
# Azure OpenAI client
# ------------------------------------------------------
aoai_client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)

# ------------------------------------------------------
# Intent + Month Classifier (UPDATED WITH NEW SCENARIOS + ROBUST JSON)
# ------------------------------------------------------
def build_trend_html_table(df):
    """
    Builds a clean, spacious HTML table for purchasing/payment trend.
    Compatible with Copilot Studio and Outlook-style renderers.
    """

    def fmt(v):
        """Format numbers into K/M for display."""
        v = float(v)
        if abs(v) >= 1_000_000:
            return f"{v/1_000_000:.2f}M"
        elif abs(v) >= 1_000:
            return f"{v/1_000:.2f}K"
        return f"{v:.2f}"

    # Start table
    html = """
<b>Your trend data:</b><br><br>

<table border="1" cellpadding="10" cellspacing="0"
       style="border-collapse: collapse; text-align: left; width:100%; font-size:14px;">
<tr>
    <th style="padding: 10px; min-width:140px;">Month</th>
    <th style="padding: 10px; min-width:160px;">Purchases</th>
    <th style="padding: 10px; min-width:160px;">Payments</th>
</tr>
"""

    # Add rows
    for _, row in df.iterrows():
        month = row.get("month", "")
        purchases = fmt(row.get("purchases", 0))
        payments = fmt(row.get("payments", 0))

        html += f"""
<tr>
    <td style="padding: 10px;">{month}</td>
    <td style="padding: 10px;">{purchases}</td>
    <td style="padding: 10px;">{payments}</td>
</tr>
"""

    html += "</table><br><br>"

    return html
def build_html_table_generic(table_data: list):
    """
    Dynamically builds an HTML table for any table data (any columns).
    """
    if not table_data:
        return ""

    # extract column names
    columns = table_data[0].keys()

    html = """
<b>Your data:</b><br><br>
<table border="1" cellpadding="8" cellspacing="0"
       style="border-collapse: collapse; text-align: left; width:100%; font-size:14px;">
<tr>
"""
    for col in columns:
        html += f"<th style='padding:8px'>{col}</th>"
    html += "</tr>"

    for row in table_data:
        html += "<tr>"
        for col in columns:
            html += f"<td style='padding:8px'>{row[col]}</td>"
        html += "</tr>"

    html += "</table><br><br>"

    return html



def classify_with_llm(query: str) -> Tuple[str, Optional[str]]:
    """
    LLM classifier:
      - Maps ANY payables question to a scenario
      - Extracts a month if present (YYYY-MM) or null
      - Robust JSON parsing (handles ```json blocks, extra text, etc.)
    """

    today = date.today()
    system_msg = f"""
You are an intent classifier for a payables analytics engine.
Today is {today.isoformat()}.

You MUST return ONLY a JSON object.
STRICT RULES:
- Output must start with '{{' and end with '}}'
- NO explanations, NO markdown, NO code fences
- JSON keys must be exactly: "scenario" and "month".

JSON FORMAT:
{{
  "scenario": "<one of the allowed scenarios>",
  "month": "<YYYY-MM or null>"
}}

Allowed scenarios:
  - aging                  (vendor aging buckets)
  - aging_vendor           (same as aging; vendor-wise)
  - trend                  (6-month purchasing & payment trend)
  - expected               (expected payments THIS WEEK)
  - expected_month         (expected payments for a specific month or 'this month')
  - expected_company       (expected payments THIS WEEK, company & vendor-wise)
  - expected_company_month (expected payments for a specific month or 'this month', company & vendor-wise)
  - outstanding_current    (outstanding payments for the CURRENT month)
  - outstanding_month      (outstanding payments for a specific month, e.g. July 2025)
  - total_payables_vendor  (total payables by vendor)
  - top_customers          (top customers by transaction amount, overall or for a month)
  - vendor_summary         (invoices, payments, net balance per vendor)
  - balance_by_vendor      (net balance per vendor)
  - outstanding_vendor_wise (outstanding totals per vendor, no aging buckets)

Month extraction rules:
- If user says "this month", "current month", map to:
    * outstanding_current (no month value needed) for OUTSTANDING questions
    * expected_month with month = current YYYY-MM for EXPECTED questions
- If user mentions a specific month name (jan, feb, march, july, etc.)
  or "for July 2024", "in March 2023":
    → use scenario:
       * outstanding_month for OUTSTANDING questions
       * expected_month / expected_company_month for EXPECTED questions
       * top_customers for TOP queries
    → set "month" to that calendar month in "YYYY-MM".
- If user asks about "this week", "current week":
    → use "expected" or "expected_company" for expected payments.
    → "month" should be null.
- If no month is mentioned and no 'this month' context:
    → "month": null.

VERY IMPORTANT:
- Do NOT invent new scenario names.
- Scenario must be EXACTLY one of the allowed ones.
- For example:
    "top customers in January" → {{ "scenario": "top_customers", "month": "2025-01" }}
    "outstanding for July 2023" → {{ "scenario": "outstanding_month", "month": "2023-07" }}
    "expected payments for this month by company" → {{ "scenario": "expected_company_month", "month": "<current YYYY-MM>" }}
    "expected vendor payments this week" → {{ "scenario": "expected", "month": null }}

Output ONLY the JSON. No extra text.
"""

    try:
        resp = aoai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": query},
            ],
            max_tokens=120,
            temperature=0,
        )

        raw = resp.choices[0].message.content or ""
        logger.info("Classifier LLM raw output: %s", raw)

        raw = raw.strip()

        # Handle ```json ... ``` or ``` ... ```
        if raw.startswith("```"):
            raw = raw.replace("```json", "")
            raw = raw.replace("```", "")
            raw = raw.strip()

        # Keep only content between first '{' and last '}'
        first = raw.find("{")
        last = raw.rfind("}")
        if first != -1 and last != -1:
            raw = raw[first:last+1].strip()

        # Parse JSON
        result = json.loads(raw)

        scenario = result.get("scenario")
        month = result.get("month")

        allowed = {
            "aging",
            "aging_vendor",
            "trend",
            "expected",
            "expected_month",
            "expected_company",
            "expected_company_month",
            "outstanding_current",
            "outstanding_month",
            "total_payables_vendor",
            "top_customers",
            "vendor_summary",
            "balance_by_vendor",
            "outstanding_vendor_wise",
        }

        if scenario not in allowed:
            logger.warning("Invalid scenario from LLM: %s", scenario)
            scenario = "aging"

        # Normalize month format if provided
        if month:
            try:
                year, mon = month.split("-")
                mon = int(mon)
                month = f"{int(year):04d}-{mon:02d}"
            except Exception:
                logger.warning("Invalid month format from LLM: %s", month)
                month = None

        return scenario, month

    except Exception as e:
        logger.exception("Classifier error: %s", e)
        # safe default: aging, no month
        return "aging", None

# ------------------------------------------------------
# MSAL AUTH HELPERS
# ------------------------------------------------------
_msal_app: Optional[msal.ConfidentialClientApplication] = None


def get_msal_app() -> msal.ConfidentialClientApplication:
    global _msal_app
    if _msal_app is None:
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        _msal_app = msal.ConfidentialClientApplication(
            CLIENT_ID,
            authority=authority,
            client_credential=CLIENT_SECRET,
        )
    return _msal_app


def get_access_token() -> str:
    app = get_msal_app()
    scope = f"{DATAVERSE_URL}/.default"
    token_result = app.acquire_token_silent([scope], account=None)
    if not token_result:
        token_result = app.acquire_token_for_client(scopes=[scope])
    if "access_token" not in token_result:
        raise RuntimeError(f"Failed to get access token: {token_result}")
    return token_result["access_token"]

# ------------------------------------------------------
# DATAVERSE → PANDAS
# ------------------------------------------------------
def dataverse_get_table(table_name: str, select: Optional[str] = None) -> pd.DataFrame:
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Prefer": "odata.maxpagesize=5000",
    }

    base_url = f"{DATAVERSE_URL}/api/data/v9.2/{table_name}"
    url = base_url
    if select:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}$select={select}"

    rows = []
    while url:
        logger.info("Fetching Dataverse: %s", url)
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    df = pd.DataFrame(rows)

    # Normalize date columns to YYYY-MM-DD string for SQLite compatibility
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    return df

# ------------------------------------------------------
# PANDAS → SQLITE
# ------------------------------------------------------
def load_to_sqlite(df: pd.DataFrame, name: str, conn: sqlite3.Connection) -> None:
    df.to_sql(name, conn, if_exists="replace", index=False)

def build_sqlite_database_cached():
    global _last_load_time, _cached_connection

    # ------------------------------------------
    # 1. If cached DB exists → check if alive
    # ------------------------------------------
    if _cached_connection is not None:
        try:
            # If this runs, connection is alive
            _cached_connection.execute("SELECT 1")

            # Check TTL
            if time.time() - _last_load_time < CACHE_TTL:
                return _cached_connection

            # TTL expired → rebuild cache
            logger.info("SQLite cache expired — rebuilding...")
        except Exception:
            # Connection is dead/closed → rebuild
            logger.warning("Cached SQLite connection is closed — rebuilding...")
        # Reset before rebuilding
        _cached_connection = None

    # ------------------------------------------
    # 2. Build fresh SQLite in-memory DB
    # ------------------------------------------
    logger.info("Refreshing Dataverse → SQLite cache...")

    conn = sqlite3.connect("cache.db", check_same_thread=False)


    df_ledger = dataverse_get_table(TABLE_COMPANY)
    df_vendtrans = dataverse_get_table(TABLE_VENDTRANS)
    df_vendtable = dataverse_get_table(TABLE_VENDTABLE)
    df_party = dataverse_get_table(TABLE_PARTYTABLE)

    load_to_sqlite(df_ledger, "ledger", conn)
    load_to_sqlite(df_vendtrans, "vendtrans", conn)
    load_to_sqlite(df_vendtable, "vendtable", conn)
    load_to_sqlite(df_party, "party", conn)

    # ------------------------------------------
    # 3. Store cache + timestamp
    # ------------------------------------------
    _cached_connection = conn
    _last_load_time = time.time()

    return conn



# ------------------------------------------------------
# QUICKCHART HELPER
# ------------------------------------------------------
def make_chart_url(chart_type: str, labels: list, values: list) -> str:
    config = {
        "type": chart_type,   # "pie", "bar", "line"
        "data": {
            "labels": labels,
            "datasets": [{
                "label": "Total Outstanding",
                "data": values
            }]
        }
    }
    return "https://quickchart.io/chart?c=" + urllib.parse.quote(str(config))


# ------------------------------------------------------
# SQL MODULES - EXISTING
# ------------------------------------------------------
def sql_aging_vendor(conn: sqlite3.Connection, chart_type: str):
    """
    Vendor Aging Report (Vendor-wise)
    Buckets: 0–30, 31–60, 61–90, Over 90 days
    Using vendtrans table.
    """
    q = """
    SELECT
        mserp_accountnum AS vendor,
        mserp_duedate     AS due_date,
        (mserp_amountmst - mserp_settleamountmst) AS outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0;
    """
    df = pd.read_sql_query(q, conn)

    # If no data
    if df.empty:
        text = "📊 Vendor Aging Report (Vendor-wise):\n\nNo outstanding invoices."
        tables = {"aging_vendor": []}
        chart_url = make_chart_url(chart_type, [], [])
        return text, tables, chart_url

    # Clean date
    df = df.dropna(subset=["due_date"])
    df["due_date"] = pd.to_datetime(df["due_date"], errors="coerce")
    df = df.dropna(subset=["due_date"])

    # Calculate days overdue
    today_ts = pd.Timestamp.today().normalize()
    df["days_overdue"] = (today_ts - df["due_date"]).dt.days

    # Bucketing
    def bucket(days: float) -> str:
        if days <= 30:
            return "0–30 days"
        elif days <= 60:
            return "31–60 days"
        elif days <= 90:
            return "61–90 days"
        else:
            return "Over 90 days"

    df["bucket"] = df["days_overdue"].apply(bucket)

    # Group vendor-wise
    grouped = df.groupby(["vendor", "bucket"], as_index=False)["outstanding"].sum()

    # Text output
    lines = ["📊 Vendor Aging Report (Vendor-wise):"]
    for vendor, sub in grouped.groupby("vendor"):
        lines.append(f"\n{vendor}:")
        for _, row in sub.iterrows():
            lines.append(f"  • {row['bucket']}: {row['outstanding']:,.0f}")
    text = "\n".join(lines)

    # Chart: bucket totals
    bucket_totals = (
        df.groupby("bucket")["outstanding"]
        .sum()
        .reindex(["0–30 days", "31–60 days", "61–90 days", "Over 90 days"], fill_value=0)
    )

    labels = bucket_totals.index.tolist()
    values = bucket_totals.tolist()

    chart_url = make_chart_url(chart_type, labels, values)

    tables = {"aging_vendor": grouped.to_dict(orient="records")}
    return text, tables, chart_url


def sql_trend(conn: sqlite3.Connection, chart_type: str):
    """
    Purchasing & payment trend over last 6 months based on transaction date.
    Negative amounts → purchases, positive → payments.
    """
    q = """
    SELECT 
        strftime('%Y-%m', mserp_transdate) AS month,
        SUM(CASE WHEN mserp_amountmst < 0 THEN -mserp_amountmst ELSE 0 END) AS purchases,
        SUM(CASE WHEN mserp_amountmst > 0 THEN  mserp_amountmst ELSE 0 END) AS payments
    FROM vendtrans
    WHERE date(mserp_transdate) >= date('now','start of month','-5 month')
    GROUP BY month
    ORDER BY month;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["month"].tolist()
    purchases = df["purchases"].tolist()
    payments = df["payments"].tolist()

    # We still use purchases as base; if you want multi-series charts,
    # you can extend make_chart_url to accept datasets.
    chart_url = make_chart_url(chart_type, labels, purchases)

    lines = ["📊 Purchasing & Payment Trend (Last 6 Months, KRW):"]
    for _, row in df.iterrows():
        lines.append(
            f"{row['month']} → Purchases: {row['purchases']:,.0f}  |  Payments: {row['payments']:,.0f}"
        )
    text = "\n".join(lines)

    tables = {"trend": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_expected_payments(conn: sqlite3.Connection, chart_type: str):
    """
    Expected payments for this week (all companies, daily).
    """
    today = date.today()
    start = today - timedelta(days=today.weekday())          # Monday
    end = start + timedelta(days=6)                          # Sunday

    q = f"""
    SELECT
        mserp_duedate AS due_date,
        SUM(mserp_amountmst - mserp_settleamountmst) AS total_outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
      AND date(mserp_duedate) BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    GROUP BY mserp_duedate
    ORDER BY mserp_duedate;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["due_date"].tolist()
    data = df["total_outstanding"].tolist()

    chart_url = make_chart_url(
        chart_type,
        labels,
        data,
    )

    lines = ["📅 Expected Vendor Payments — This Week (KRW):"]
    for d, v in zip(labels, data):
        lines.append(f"{d}: {v:,.0f}")
    text = "\n".join(lines)

    tables = {"expected": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_expected_payments_by_company(conn: sqlite3.Connection, chart_type: str):
    """
    Expected payments for this week:
      - Company-wise + vendor-wise (mserp_dataareaid + mserp_accountnum) by day.
      - Table: detailed rows (company, vendor, due_date, total_outstanding).
      - Chart: summarized per company per date.
    """
    today = date.today()
    start = today - timedelta(days=today.weekday())   # Monday
    end = start + timedelta(days=6)                   # Sunday

    q = f"""
    SELECT
        mserp_dataareaid AS company,
        mserp_accountnum AS vendor,
        mserp_duedate    AS due_date,
        SUM(mserp_amountmst - mserp_settleamountmst) AS total_outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
      AND date(mserp_duedate) BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
    GROUP BY mserp_dataareaid, mserp_accountnum, mserp_duedate
    ORDER BY mserp_duedate, mserp_dataareaid, mserp_accountnum;
    """
    df = pd.read_sql_query(q, conn)

    if df.empty:
        text = (
            "📅 Expected Vendor Payments — This Week (Company & Vendor-wise, KRW):\n\n"
            "No outstanding payments for this week."
        )
        tables = {"expected_company": []}
        chart_url = make_chart_url(chart_type, [], [])
        return text, tables, chart_url

    # Chart: company-level totals per date
    agg = (
        df.groupby(["due_date", "company"], as_index=False)["total_outstanding"]
        .sum()
    )

    pivot = agg.pivot_table(
        index="due_date",
        columns="company",
        values="total_outstanding",
        aggfunc="sum",
        fill_value=0.0,
    ).sort_index()

    labels = pivot.index.tolist()
    # Use first company series for chart base
    first_company = pivot.columns[0]
    chart_url = make_chart_url(chart_type, labels, pivot[first_company].tolist())

    # Text summary: by date → company → vendor
    lines = ["📅 Expected Vendor Payments — This Week (Company & Vendor-wise, KRW):"]
    for due_date, sub_date in df.groupby("due_date"):
        lines.append(f"\n{due_date}:")
        for company, sub_comp in sub_date.groupby("company"):
            total_company = sub_comp["total_outstanding"].sum()
            lines.append(f"  • {company}: {total_company:,.0f}")
            for _, row in sub_comp.iterrows():
                lines.append(
                    f"      - {row['vendor']}: {row['total_outstanding']:,.0f}"
                )

    text = "\n".join(lines)

    tables = {"expected_company": df.to_dict(orient="records")}
    return text, tables, chart_url


# ------------------------------------------------------
# NEW: EXPECTED PAYMENTS FOR A SPECIFIC MONTH
# ------------------------------------------------------
def _month_start_end(month_yyyy_mm: str) -> Tuple[date, date]:
    year_str, mon_str = month_yyyy_mm.split("-")
    year = int(year_str)
    mon = int(mon_str)
    start_date = date(year, mon, 1)
    if mon == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, mon + 1, 1)
    end_date = next_month - timedelta(days=1)
    return start_date, end_date


def sql_expected_payments_month(conn: sqlite3.Connection, chart_type: str, month_yyyy_mm: str):
    """
    Expected payments for a specific month (all companies, daily).
    """
    start_date, end_date = _month_start_end(month_yyyy_mm)

    q = f"""
    SELECT
        mserp_duedate AS due_date,
        SUM(mserp_amountmst - mserp_settleamountmst) AS total_outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
      AND date(mserp_duedate) BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
    GROUP BY mserp_duedate
    ORDER BY mserp_duedate;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["due_date"].tolist()
    data = df["total_outstanding"].tolist()

    chart_url = make_chart_url(chart_type, labels, data)

    lines = [f"📅 Expected Vendor Payments — {month_yyyy_mm} (KRW):"]
    for d, v in zip(labels, data):
        lines.append(f"{d}: {v:,.0f}")
    text = "\n".join(lines)

    tables = {"expected": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_expected_payments_by_company_month(conn: sqlite3.Connection, chart_type: str, month_yyyy_mm: str):
    """
    Expected payments for a specific month:
      - Company-wise + vendor-wise (mserp_dataareaid + mserp_accountnum) by day.
    """
    start_date, end_date = _month_start_end(month_yyyy_mm)

    q = f"""
    SELECT
        mserp_dataareaid AS company,
        mserp_accountnum AS vendor,
        mserp_duedate    AS due_date,
        SUM(mserp_amountmst - mserp_settleamountmst) AS total_outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
      AND date(mserp_duedate) BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
    GROUP BY mserp_dataareaid, mserp_accountnum, mserp_duedate
    ORDER BY mserp_duedate, mserp_dataareaid, mserp_accountnum;
    """
    df = pd.read_sql_query(q, conn)

    if df.empty:
        text = (
            f"📅 Expected Vendor Payments — {month_yyyy_mm} (Company & Vendor-wise, KRW):\n\n"
            "No outstanding payments for this period."
        )
        tables = {"expected_company": []}
        chart_url = make_chart_url(chart_type, [], [])
        return text, tables, chart_url

    # Chart: company-level totals per date
    agg = (
        df.groupby(["due_date", "company"], as_index=False)["total_outstanding"]
        .sum()
    )

    pivot = agg.pivot_table(
        index="due_date",
        columns="company",
        values="total_outstanding",
        aggfunc="sum",
        fill_value=0.0,
    ).sort_index()

    labels = pivot.index.tolist()
    first_company = pivot.columns[0]
    chart_url = make_chart_url(chart_type, labels, pivot[first_company].tolist())

    lines = [f"📅 Expected Vendor Payments — {month_yyyy_mm} (Company & Vendor-wise, KRW):"]
    for due_date, sub_date in df.groupby("due_date"):
        lines.append(f"\n{due_date}:")
        for company, sub_comp in sub_date.groupby("company"):
            total_company = sub_comp["total_outstanding"].sum()
            lines.append(f"  • {company}: {total_company:,.0f}")
            for _, row in sub_comp.iterrows():
                lines.append(
                    f"      - {row['vendor']}: {row['total_outstanding']:,.0f}"
                )

    text = "\n".join(lines)
    tables = {"expected_company": df.to_dict(orient="records")}
    return text, tables, chart_url


# ------------------------------------------------------
# OUTSTANDING MONTH FUNCTIONS (existing)
# ------------------------------------------------------
def sql_outstanding_this_month(conn: sqlite3.Connection, chart_type: str):
    """
    Outstanding vendor payments for the CURRENT month.
    """
    q = """
    SELECT
        mserp_duedate AS due_date,
        SUM(mserp_amountmst - mserp_settleamountmst) AS outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
      AND date(mserp_duedate) BETWEEN date('now','start of month')
                                  AND date('now','start of month','+1 month','-1 day')
    GROUP BY mserp_duedate
    ORDER BY mserp_duedate;
    """
    df = pd.read_sql_query(q, conn)
    df["outstanding"] = df["outstanding"].fillna(0)

    total_outstanding = float(df["outstanding"].sum()) if not df.empty else 0.0
    labels = df["due_date"].tolist()
    data = df["outstanding"].tolist()

    chart_url = make_chart_url(
        chart_type,
        labels,
        data,
    )

    today = date.today()
    start_date = today.replace(day=1)
    if today.month == 12:
        next_month = date(today.year + 1, 1, 1)
    else:
        next_month = date(today.year, today.month + 1, 1)
    end_date = next_month - timedelta(days=1)

    lines = [
        f"📅 Outstanding Vendor Payments — This Month ({start_date.isoformat()} → {end_date.isoformat()}):",
        "",
        f"Total Outstanding: {total_outstanding:,.0f} KRW",
    ]
    if not df.empty:
        lines.append("")
        lines.append("Per-day breakdown:")
        for d, v in zip(labels, data):
            lines.append(f"{d} → {v:,.0f}")

    text = "\n".join(lines)
    tables = {"outstanding_month": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_outstanding_for_month(conn: sqlite3.Connection, month_yyyy_mm: str, chart_type: str):
    """
    Outstanding vendor payments for a specific calendar month (YYYY-MM).
    """
    year_str, mon_str = month_yyyy_mm.split("-")
    year = int(year_str)
    mon = int(mon_str)

    start_date = date(year, mon, 1)
    if mon == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, mon + 1, 1)
    end_date = next_month - timedelta(days=1)

    q = f"""
    SELECT
        mserp_duedate AS due_date,
        SUM(mserp_amountmst - mserp_settleamountmst) AS outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
      AND date(mserp_duedate) BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
    GROUP BY mserp_duedate
    ORDER BY mserp_duedate;
    """
    df = pd.read_sql_query(q, conn)
    df["outstanding"] = df["outstanding"].fillna(0)

    total_outstanding = float(df["outstanding"].sum()) if not df.empty else 0.0
    labels = df["due_date"].tolist()
    data = df["outstanding"].tolist()

    chart_url = make_chart_url(
        chart_type,
        labels,
        data,
    )

    lines = [
        f"📅 Outstanding Vendor Payments — {month_yyyy_mm}:",
        "",
        f"Total Outstanding: {total_outstanding:,.0f} KRW",
    ]
    if not df.empty:
        lines.append("")
        lines.append("Per-day breakdown:")
        for d, v in zip(labels, data):
            lines.append(f"{d} → {v:,.0f}")

    text = "\n".join(lines)
    tables = {"outstanding_month": df.to_dict(orient="records")}
    return text, tables, chart_url

# ------------------------------------------------------
# SQL MODULES - NEW SCENARIOS (existing in your file)
# ------------------------------------------------------
def sql_total_payables_vendor(conn: sqlite3.Connection, chart_type: str):
    """
    Total payables by vendor (sum of outstanding per vendor).
    """
    q = """
    SELECT 
        mserp_accountnum AS vendor,
        SUM(mserp_amountmst - mserp_settleamountmst) AS total_outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
    GROUP BY mserp_accountnum
    ORDER BY total_outstanding DESC;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["vendor"].tolist()
    values = df["total_outstanding"].tolist()
    chart_url = make_chart_url(chart_type, labels, values)

    lines = ["📊 Total Payables by Vendor:"]
    for _, r in df.iterrows():
        lines.append(f"{r['vendor']} → {r['total_outstanding']:,.0f}")
    text = "\n".join(lines)

    tables = {"total_payables_vendor": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_top_customers(conn, chart_type, month=None):
    if month:
        q = f"""
        SELECT 
            mserp_accountnum AS customer,
            SUM(
                CASE WHEN mserp_amountmst < 0 THEN -mserp_amountmst
                     ELSE  mserp_amountmst END
            ) AS total_amount
        FROM vendtrans
        WHERE strftime('%Y-%m', mserp_transdate) = '{month}'
        GROUP BY mserp_accountnum
        ORDER BY total_amount DESC
        LIMIT 10;
        """
    else:
        q = """
        SELECT 
            mserp_accountnum AS customer,
            SUM(
                CASE WHEN mserp_amountmst < 0 THEN -mserp_amountmst
                     ELSE  mserp_amountmst END
            ) AS total_amount
        FROM vendtrans
        GROUP BY mserp_accountnum
        ORDER BY total_amount DESC
        LIMIT 10;
        """

    df = pd.read_sql_query(q, conn)

    labels = df["customer"].tolist()
    values = df["total_amount"].tolist()

    chart_url = make_chart_url(chart_type, labels, values)

    lines = [f"🏆 Top 10 Customers{' for ' + month if month else ''}:"]

    for _, r in df.iterrows():
        lines.append(f"{r['customer']} → {r['total_amount']:,.0f}")

    return "\n".join(lines), {"top_customers": df.to_dict(orient="records")}, chart_url


def sql_vendor_summary(conn: sqlite3.Connection, chart_type: str):
    """
    Vendor summary: invoices, payments, net balance.
    Negative amounts = invoices, positive = payments.
    """
    q = """
    SELECT
        mserp_accountnum AS vendor,
        SUM(CASE WHEN mserp_amountmst < 0 THEN -mserp_amountmst ELSE 0 END) AS invoices,
        SUM(CASE WHEN mserp_amountmst > 0 THEN  mserp_amountmst ELSE 0 END) AS payments,
        SUM(mserp_amountmst) AS net_balance
    FROM vendtrans
    GROUP BY mserp_accountnum
    ORDER BY net_balance DESC;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["vendor"].tolist()
    values = df["net_balance"].tolist()
    chart_url = make_chart_url(chart_type, labels, values)

    lines = ["📘 Vendor Summary (Invoices | Payments | Net Balance):"]
    for _, r in df.iterrows():
        lines.append(
            f"{r['vendor']} → Invoices: {r['invoices']:,.0f}, "
            f"Payments: {r['payments']:,.0f}, Net: {r['net_balance']:,.0f}"
        )
    text = "\n".join(lines)

    tables = {"vendor_summary": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_balance_by_vendor(conn: sqlite3.Connection, chart_type: str):
    """
    Vendor net balance (sum of mserp_amountmst).
    """
    q = """
    SELECT
        mserp_accountnum AS vendor,
        SUM(mserp_amountmst) AS net_balance
    FROM vendtrans
    GROUP BY mserp_accountnum
    ORDER BY net_balance DESC;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["vendor"].tolist()
    values = df["net_balance"].tolist()
    chart_url = make_chart_url(chart_type, labels, values)

    lines = ["💰 Vendor Net Balance (Positive = Payable, Negative = Credit):"]
    for _, r in df.iterrows():
        lines.append(f"{r['vendor']} → {r['net_balance']:,.0f}")
    text = "\n".join(lines)

    tables = {"balance_by_vendor": df.to_dict(orient="records")}
    return text, tables, chart_url


def sql_outstanding_vendor_wise(conn: sqlite3.Connection, chart_type: str):
    """
    Outstanding vendor-wise totals without aging buckets.
    """
    q = """
    SELECT
        mserp_accountnum AS vendor,
        SUM(mserp_amountmst - mserp_settleamountmst) AS outstanding
    FROM vendtrans
    WHERE (mserp_amountmst - mserp_settleamountmst) > 0
    GROUP BY mserp_accountnum
    ORDER BY outstanding DESC;
    """
    df = pd.read_sql_query(q, conn)

    labels = df["vendor"].tolist()
    values = df["outstanding"].tolist()
    chart_url = make_chart_url(chart_type, labels, values)

    lines = ["🧾 Outstanding Vendor-wise Totals:"]
    for _, r in df.iterrows():
        lines.append(f"{r['vendor']} → {r['outstanding']:,.0f}")
    text = "\n".join(lines)

    tables = {"outstanding_vendor_wise": df.to_dict(orient="records")}
    return text, tables, chart_url
def format_millions(value: float) -> str:
    if value >= 1_000_000:
        return f"{value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"{value/1_000:.2f}K"
    else:
        return f"{value:.2f}"

def summarize_business_output(query, scenario, month, df):
    """
    Business summary generator:
    Converts raw table output into a clean CFO-level narrative.
    """
 
    # -----------------------------------------
    # 1. SYSTEM PROMPT (UPDATED WITH • BULLETS)
    # -----------------------------------------
    system_msg = """
You are a senior finance analyst.
 
Write a VERY SHORT, professional CFO-level summary using ONLY bullet points.
 
STRICT RULES:
- Use ONLY this bullet style: •
- Every bullet MUST be on its own line.
- After each bullet, you MUST insert a newline (\\n) so bullets never appear in the same line.
- Maximum 4–5 bullet points.
- Do NOT write long paragraphs.
- Do NOT repeat table values already shown above.
- Do NOT mention SQL, JSON, rows, datasets, or 'table above'.
- Use concise CFO-style financial language:
  liquidity risk, cashflow pressure, exposure, payment gaps, deferred liabilities,
  volatility, stabilization, spending spikes.
 
FORMAT (must match exactly):
 
• Insight 1  
• Insight 2  
• Insight 3  
• Insight 4  
• Insight 5 (optional)
 
Each bullet must start with "• " at the beginning of a new line.
Never place two bullets in the same line.
Never combine multiple insights in one bullet.
"""
 
    # -----------------------------------------
    # 2. Build HTML Trend Table (only for TREND)
    # -----------------------------------------
    trend_table = ""
    if scenario == "trend" and df is not None and not df.empty:
        trend_table = build_trend_html_table(df)
 
    # -----------------------------------------
    # 3. Prepare user payload
    # -----------------------------------------
    preview = df.to_dict(orient="records") if df is not None else []
 
    payload = {
        "query": query,
        "scenario": scenario,
        "month": month,
        "trend_table": bool(trend_table),
        "data_preview": preview
    }
 
    # -----------------------------------------
    # 4. LLM CALL — Generate Summary
    # -----------------------------------------
    try:
        resp = aoai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": json.dumps(payload)}
            ],
            max_tokens=250,
            temperature=0,
        )
 
        summary = resp.choices[0].message.content.strip()
 
        # -----------------------------------------
        # 5. FINAL OUTPUT — attach HTML table
        # -----------------------------------------
 
        # 5A — trend uses trend HTML
        if scenario == "trend" and trend_table:
            return trend_table + summary
 
        # 5B — all other scenarios use generic HTML
        if df is not None and not df.empty:
            generic_html = build_html_table_generic(preview)
            return generic_html + summary
 
        # fallback
        return summary
 
    except Exception as e:
        logger.error("Business summary LLM error: %s", e)
        return "Summary unavailable."

# ------------------------------------------------------
# FASTAPI APP
# ------------------------------------------------------
app = FastAPI(title="Payables Intelligence Engine (Dataverse + Azure OpenAI + SQLite)")

# ------------------------------------------------------
# STARTUP EVENT → Warm-up Dataverse + SQLite Cache
# ------------------------------------------------------
@app.on_event("startup")
def warm_up_sqlite_cache():
    """
    Runs once when FastAPI server starts.
    Preloads Dataverse tables → builds in-memory SQLite → stores cache.
    This removes 1.5–2.5 seconds delay for the first request.
    """
    try:
        logger.info("🚀 Warming up Dataverse → SQLite cache at startup...")
        conn = build_sqlite_database_cached()

        # simple ping to ensure DB is active
        conn.execute("SELECT 1")
        logger.info("✅ Warm-up complete! Cache is ready.")

    except Exception as e:
        logger.error("❌ Warm-up failed: %s", e)



class QueryRequest(BaseModel):
    query: str

@app.post("/llm/query")
def llm_query(req: QueryRequest):
    scenario, month_str = classify_with_llm(req.query)
    logger.info("Detected scenario: %s, month: %s", scenario, month_str)

    try:
        conn = build_sqlite_database_cached()


        chart_type = "bar"   # default

        # ----------------- AGING -----------------
        if scenario in ("aging", "aging_vendor"):
            chart_type = "pie"
            text, tables, chart_url = sql_aging_vendor(conn, chart_type)

        # ----------------- TREND -------------------
        elif scenario == "trend":
            text, tables, chart_url = sql_trend(conn, chart_type)

        # ---------------- EXPECTED (THIS WEEK) -----------------
        elif scenario == "expected" and not month_str:
            chart_type = "line"
            text, tables, chart_url = sql_expected_payments(conn, chart_type)

        # ---------------- EXPECTED (SPECIFIC MONTH) ------------
        elif scenario == "expected_month" and month_str:
            chart_type = "line"
            text, tables, chart_url = sql_expected_payments_month(conn, chart_type, month_str)

        # ------------- EXPECTED COMPANY (THIS WEEK) -----------
        elif scenario == "expected_company" and not month_str:
            chart_type = "line"
            text, tables, chart_url = sql_expected_payments_by_company(conn, chart_type)

        # ------------- EXPECTED COMPANY (SPECIFIC MONTH) ------
        elif scenario == "expected_company_month" and month_str:
            chart_type = "line"
            text, tables, chart_url = sql_expected_payments_by_company_month(conn, chart_type, month_str)

        # -------- OUTSTANDING MONTH LOGIC ----------
        elif scenario == "outstanding_current":
            text, tables, chart_url = sql_outstanding_this_month(conn, chart_type)

        elif scenario == "outstanding_month" and month_str:
            text, tables, chart_url = sql_outstanding_for_month(conn, month_str, chart_type)

        # ------------ TOTAL PAYABLES ---------------
        elif scenario == "total_payables_vendor":
            text, tables, chart_url = sql_total_payables_vendor(conn, chart_type)

        # -------------- TOP CUSTOMERS --------------
        elif scenario == "top_customers":
            # Month-aware
            text, tables, chart_url = sql_top_customers(conn, chart_type, month_str)

        # -------------- VENDOR SUMMARY -------------
        elif scenario == "vendor_summary":
            text, tables, chart_url = sql_vendor_summary(conn, chart_type)

        # ------------ BALANCE BY VENDOR ------------
        elif scenario == "balance_by_vendor":
            text, tables, chart_url = sql_balance_by_vendor(conn, chart_type)

        # -------- OUTSTANDING VENDOR-WISE ----------
        elif scenario == "outstanding_vendor_wise":
            text, tables, chart_url = sql_outstanding_vendor_wise(conn, chart_type)

        else:
            text = "Query not supported."
            tables = {}
            chart_url = None

  

       # Generate business-level summary
        df_all = pd.DataFrame()
        for tbl in tables.values():
            df_all = pd.concat([df_all, pd.DataFrame(tbl)], ignore_index=True)

        business_summary = summarize_business_output(
            req.query, scenario, month_str, df_all
        )

        return {
            "scenario": "payables",
            "response": business_summary,
            "tables": tables,
            "chart_url": chart_url,
        }


    except Exception as e:
        logger.exception("Error during SQL execution")
        raise HTTPException(status_code=500, detail=str(e))
