import os
import logging
import sqlite3
from datetime import date, timedelta
from typing import Optional, Dict, Any, List
import json
import urllib.parse
import math
import time

import msal
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from openai import AzureOpenAI

# ======================================================
# CONFIG & GLOBALS
# ======================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

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
AZURE_OPENAI_EMBEDDING_DEPLOYMENT = os.getenv(
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-large"
)

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, DATAVERSE_URL,
            AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY]):
    logger.warning("Some required environment variables are missing.")

aoai_client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)

# Dataverse → SQLite cache (1-hour TTL)
CACHE_TTL = 3600  # seconds
_last_load_time = 0.0
_cached_connection: Optional[sqlite3.Connection] = None

# RAG cache
_rag_docs: List[str] = []
_rag_embeddings: List[List[float]] = []
_rag_built = False

_msal_app: Optional[msal.ConfidentialClientApplication] = None

# ======================================================
# MSAL AUTH
# ======================================================

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

# ======================================================
# DATAVERSE → PANDAS
# ======================================================

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
        resp = requests.get(url, headers=headers, timeout=300)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    df = pd.DataFrame(rows)

    # Normalize date columns to YYYY-MM-DD for SQLite
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    return df

# ======================================================
# PANDAS → SQLITE (CACHED)
# ======================================================

def load_to_sqlite(df: pd.DataFrame, name: str, conn: sqlite3.Connection) -> None:
    df.to_sql(name, conn, if_exists="replace", index=False)


def build_sqlite_database_cached() -> sqlite3.Connection:
    """
    Build or reuse an SQLite DB from Dataverse, cached for CACHE_TTL seconds.
    """
    global _last_load_time, _cached_connection

    # Reuse if alive and TTL not expired
    if _cached_connection is not None:
        try:
            _cached_connection.execute("SELECT 1")
            if time.time() - _last_load_time < CACHE_TTL:
                return _cached_connection
            logger.info("SQLite cache expired — rebuilding...")
        except Exception:
            logger.warning("Cached SQLite connection dead — rebuilding...")
        _cached_connection = None

    logger.info("Refreshing Dataverse → SQLite cache...")
    conn = sqlite3.connect("payables_cache.db", check_same_thread=False)

    df_ledger = dataverse_get_table(TABLE_COMPANY)
    df_vendtrans = dataverse_get_table(TABLE_VENDTRANS)
    df_vendtable = dataverse_get_table(TABLE_VENDTABLE)
    df_party = dataverse_get_table(TABLE_PARTYTABLE)

    load_to_sqlite(df_ledger, "ledger", conn)
    load_to_sqlite(df_vendtrans, "vendtrans", conn)
    load_to_sqlite(df_vendtable, "vendtable", conn)
    load_to_sqlite(df_party, "party", conn)

    _cached_connection = conn
    _last_load_time = time.time()
    return conn

# ======================================================
# EMBEDDINGS & RAG
# ======================================================

def embed_texts(texts: List[str]) -> List[List[float]]:
    """
    Uses Azure OpenAI embedding deployment to embed a list of texts.
    """
    if not texts:
        return []
    resp = aoai_client.embeddings.create(
        model=AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
        input=texts,
    )
    return [d.embedding for d in resp.data]


def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)


def build_schema_doc(conn: sqlite3.Connection) -> str:
    """
    Introspect SQLite and build a schema description document for RAG.
    """
    tables = ["vendtrans", "ledger", "vendtable", "party"]
    lines = ["SQLite schema overview:"]
    for tbl in tables:
        try:
            df_info = pd.read_sql_query(f"PRAGMA table_info({tbl});", conn)
            if df_info.empty:
                continue
            lines.append(f"\nTable: {tbl}")
            for _, row in df_info.iterrows():
                col_name = row["name"]
                col_type = row["type"]
                lines.append(f"  - {col_name} ({col_type})")
        except Exception as e:
            logger.warning("Schema introspection failed for %s: %s", tbl, e)
    return "\n".join(lines)

def get_business_rules_doc() -> str:
    """
    Finance/ERP business rules for interpretation.
    """
    return """
Finance business rules for payables (vendtrans):

- mserp_amountmst:
    * Negative values represent purchases / invoices (money owed to vendors).
    * Positive values represent payments made to vendors.

- mserp_settleamountmst:
    * Amount already settled (paid) against an invoice.

- Outstanding amount:
    outstanding = mserp_amountmst - mserp_settleamountmst

- mserp_transdate:
    * Transaction posting date.

- mserp_duedate:
    * Due date of the vendor invoice or payable.

- mserp_accountnum:
    * Vendor ID.

- mserp_dataareaid:
    * Company / legal entity.

Key analytic views:

- Vendor aging:
    Group outstanding invoices by vendor and days overdue (aging buckets)
    using days_overdue = julianday('now') - julianday(mserp_duedate).

- Expected payments:
    Sum outstanding by mserp_duedate over a period (this week, this month, or a specific date range).

- Trends:
    Group by strftime('%Y-%m', mserp_transdate) to get monthly purchases and payments.

- Top vendors by exposure:
    Sum outstanding amount per vendor and sort descending.

Always use SQLite-compatible functions:
- strftime('%Y-%m', mserp_transdate)
- strftime('%Y-%m', mserp_duedate)
- date(mserp_duedate)
- julianday(date_column)
"""


def get_sql_examples_doc() -> str:
    """
    Example NL questions and SQL patterns for RAG to learn from.
    """
    return """
Example 1: Vendor aging buckets (dynamic aging)
Question: "Show me vendor-wise aging buckets for all outstanding invoices."
SQL:
SELECT
    mserp_accountnum AS vendor,
    CASE
        WHEN julianday('now') - julianday(mserp_duedate) <= 30 THEN '0-30 days'
        WHEN julianday('now') - julianday(mserp_duedate) <= 60 THEN '31-60 days'
        WHEN julianday('now') - julianday(mserp_duedate) <= 90 THEN '61-90 days'
        ELSE '>90 days'
    END AS aging_bucket,
    SUM(mserp_amountmst - mserp_settleamountmst) AS outstanding
FROM vendtrans
WHERE (mserp_amountmst - mserp_settleamountmst) > 0
GROUP BY mserp_accountnum, aging_bucket
ORDER BY vendor, aging_bucket;

Example 2: Monthly purchases and payments trend
Question: "Show last 6 months purchasing and payment trend."
SQL:
SELECT
    strftime('%Y-%m', mserp_transdate) AS month,
    SUM(CASE WHEN mserp_amountmst < 0 THEN -mserp_amountmst ELSE 0 END) AS purchases,
    SUM(CASE WHEN mserp_amountmst > 0 THEN  mserp_amountmst ELSE 0 END) AS payments
FROM vendtrans
GROUP BY month
ORDER BY month;

Example 3: This week expected payments by due date (daily)
Question: "What are the expected payments for this week by due date?" or "금주 지급예상액을 일별로 보여줘."
SQL:
SELECT
    mserp_duedate AS due_date,
    SUM(mserp_amountmst - mserp_settleamountmst) AS expected_payment
FROM vendtrans
WHERE (mserp_amountmst - mserp_settleamountmst) > 0
  AND date(mserp_duedate) BETWEEN date('now','weekday 1') AND date('now','weekday 7')
GROUP BY mserp_duedate
ORDER BY mserp_duedate;

Example 4: Top vendors by outstanding exposure
Question: "Show top 10 vendors by outstanding payables."
SQL:
SELECT
    mserp_accountnum AS vendor,
    SUM(mserp_amountmst - mserp_settleamountmst) AS outstanding
FROM vendtrans
WHERE (mserp_amountmst - mserp_settleamountmst) > 0
GROUP BY mserp_accountnum
ORDER BY outstanding DESC
LIMIT 10;

Example 5: Outstanding for a specific month
Question: "Total outstanding per day for July 2025."
SQL:
SELECT
    mserp_duedate AS due_date,
    SUM(mserp_amountmst - mserp_settleamountmst) AS outstanding
FROM vendtrans
WHERE (mserp_amountmst - mserp_settleamountmst) > 0
  AND strftime('%Y-%m', mserp_duedate) = '2025-07'
GROUP BY mserp_duedate
ORDER BY mserp_duedate;
Example 6: Total number of vendors (Vendor Master)
Question: "Total number of vendors" or "How many vendors exist?"
SQL:
SELECT
    distinct(mserp_accountnum) AS total_vendors
FROM vendtable;

"""


def build_rag_index(conn: sqlite3.Connection):
    """
    Build RAG documents + embeddings once per process.
    """
    global _rag_docs, _rag_embeddings, _rag_built
    if _rag_built:
        return

    logger.info("Building RAG index (schema + rules + examples)...")

    schema_doc = build_schema_doc(conn)
    rules_doc = get_business_rules_doc()
    examples_doc = get_sql_examples_doc()

    _rag_docs = [schema_doc, rules_doc, examples_doc]
    _rag_embeddings = embed_texts(_rag_docs)
    _rag_built = True
    logger.info("RAG index built with %d documents", len(_rag_docs))


def get_rag_context(conn: sqlite3.Connection, query: str, top_k: int = 3) -> str:
    """
    Retrieve top_k RAG documents most relevant to the query.
    """
    build_rag_index(conn)
    if not _rag_docs:
        return ""

    q_emb = embed_texts([query])[0]
    scored = []
    for i, emb in enumerate(_rag_embeddings):
        sim = cosine_similarity(q_emb, emb)
        scored.append((sim, i))
    scored.sort(reverse=True)
    chosen = [_rag_docs[i] for _, i in scored[:top_k]]
    return "\n\n---\n\n".join(chosen)
# ======================================================
# QUICKCHART & HTML TABLE
# ======================================================

def make_chart_url(chart_type: str, labels: List[str], values: List[float]) -> Optional[str]:
    if not labels or not values:
        return None
    config = {
        "type": chart_type,   # "pie", "bar", "line"
        "data": {
            "labels": labels,
            "datasets": [{
                "label": "Value",
                "data": values
            }]
        }
    }
    return "https://quickchart.io/chart?c=" + urllib.parse.quote(str(config))


def build_html_table_generic(df: pd.DataFrame) -> str:
    """
    Build a generic HTML table for any DataFrame.
    """
    if df is None or df.empty:
        return "<i>No data available.</i><br><br>"

    columns = df.columns.tolist()
    rows = df.to_dict(orient="records")

    html = """
<b>Your data:</b><br><br>
<table border="1" cellpadding="8" cellspacing="0"
       style="border-collapse: collapse; text-align: left; width:100%; font-size:14px;">
<tr>
"""
    for col in columns:
        html += f"<th style='padding:8px'>{col}</th>"
    html += "</tr>"

    for row in rows:
        html += "<tr>"
        for col in columns:
            val = row.get(col, "")
            html += f"<td style='padding:8px'>{val}</td>"
        html += "</tr>"

    html += "</table><br><br>"
    return html

# ======================================================
# BUSINESS SUMMARY (CFO-LEVEL BULLETS)
# ======================================================

def summarize_business_output(query: str, scenario: str, month: Optional[str], df: pd.DataFrame) -> str:
    """
    Generate a short CFO-level bullet summary from the data.
    Output remains in English for consistency, even if the query is in Korean.
    """
    system_msg = """
You are a senior finance analyst.

Write a VERY SHORT, professional CFO-level summary using ONLY bullet points.

STRICT RULES:
- Use ONLY this bullet style: •
- Maximum 4–5 bullet points.
- Do NOT write long paragraphs.
- Do NOT repeat or restate every table value.
- Do NOT mention SQL, rows, JSON, datasets, or internal systems.
- Do NOT say "see above" or "in the table".
- Use concise financial language:
  liquidity risk, cashflow pressure, exposure, deferred liabilities,
  payment gaps, spending spikes, stabilization, volatility.

Your output MUST follow this exact format:

• Insight 1  
• Insight 2  
• Insight 3  
• Insight 4  
• Insight 5 (optional)

Use the numbers from data_preview only when needed to support insights.
Never invent numbers.
"""

    preview = df.to_dict(orient="records") if df is not None else []
    payload = {
        "query": query,
        "scenario": scenario,
        "month": month,
        "data_preview": preview,
    }

    try:
        resp = aoai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": json.dumps(payload)},
            ],
            max_tokens=250,
            temperature=0,
        )
        summary = resp.choices[0].message.content.strip()
        summary = summary.replace("•", "<br>•")
        summary = summary.replace("<br><br>", "<br>")
        return summary
    except Exception as e:
        logger.error("Business summary LLM error: %s", e)
        return "Summary unavailable."
# ======================================================
# SQL GENERATION VIA RAG + LLM (MULTI-SCENARIO)
# ======================================================

def generate_sql_with_llm(query: str, rag_context: str) -> List[Dict[str, Any]]:
    """
    Use RAG context + LLM to generate one or more SQLite SQL queries.
    Returns a list of dicts, each with:
      - name (scenario key)
      - sql (str)
      - chart_type (str: 'bar' | 'line' | 'pie')
      - table_name (str)  -> key for JSON 'tables'
    """
    today = date.today().isoformat()

    system_msg = """
You are an expert SQL generator for a payables analytics engine.

LANGUAGE:
- User questions may be written in ANY language (e.g., English or Korean).
- Always understand the meaning and generate SQL in English.
- SQL keywords, column names, and aliases MUST be in English.

DATABASE:
- You are querying an SQLite database populated from a Dynamics 365 / Dataverse instance.
- Use ONLY the tables and columns described in the context.
- The primary transactional table is 'vendtrans' (vendor transaction lines).

IMPORTANT RULES:
- Generate valid SQLite SQL (NOT T-SQL, NOT Dataverse OData).
- Use functions supported by SQLite:
  * strftime('%Y-%m', column)
  * strftime('%Y-%W', column)
  * date(column)
  * julianday(column)
- For dates, you can use constraints like:
  * strftime('%Y-%m', mserp_transdate) = 'YYYY-MM'
  * strftime('%Y-%m', mserp_duedate)  = 'YYYY-MM'
  * date(mserp_duedate) BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'

FINANCE LOGIC:
- mserp_amountmst < 0: purchases/invoices (use ABS when summing if you want positive values).
- mserp_amountmst > 0: payments.
- Outstanding = mserp_amountmst - mserp_settleamountmst.
- mserp_accountnum: vendor/customer ID.
- mserp_dataareaid: company/legal entity.

EXPECTED PAYMENTS (지급예정, expected payments, payables):
- Always interpret as OUTSTANDING amounts still to be paid, based on DUE DATE.
- Use:
    outstanding = mserp_amountmst - mserp_settleamountmst
- Filter:
    (mserp_amountmst - mserp_settleamountmst) > 0
- Aggregate with a clear alias, for example:
    SUM(mserp_amountmst - mserp_settleamountmst) AS expected_payment

DATE & PERIOD INTERPRETATION (Korean + English):

- "금주", "이번 주", "이번주", "this week", "current week":
    → This week's Monday to Sunday based on mserp_duedate:
      date(mserp_duedate) BETWEEN date('now','weekday 1') AND date('now','weekday 7')

- "지난주", "저번 주", "last week":
    → Previous week Monday to Sunday:
      date(mserp_duedate) BETWEEN date('now','weekday 1','-7 days')
                             AND date('now','weekday 7','-7 days')

- "다음주", "다음 주", "next week":
    → Next week Monday to Sunday:
      date(mserp_duedate) BETWEEN date('now','weekday 1','+7 days')
                             AND date('now','weekday 7','+7 days')

- "이번 달", "이번달", "this month":
    → Current month:
      strftime('%Y-%m', mserp_duedate) = strftime('%Y-%m','now')

- "지난 달", "지난달", "last month":
    → Previous month:
      strftime('%Y-%m', mserp_duedate) = strftime('%Y-%m','now','-1 month')

GROUPING WORDS:
- "일별", "날짜별", "daily", "per day", "by date":
    → Group ONLY by due date:
      GROUP BY mserp_duedate

- "월별", "monthly", "by month":
    → Group by month:
      GROUP BY strftime('%Y-%m', mserp_duedate)

- "주별", "weekly", "by week":
    → Group by week:
      GROUP BY strftime('%Y-%W', mserp_duedate)

- "벤더별", "거래처별", "업체별", "vendor-wise", "by vendor":
    → Group by vendor:
      GROUP BY mserp_accountnum

DYNAMIC AGING LOGIC:
- When the question mentions aging ("aging", "채무연령", "연령분석", "Aging Report"):
    - Compute days_overdue as:
        days_overdue = julianday('now') - julianday(mserp_duedate)
    - Use a CASE expression to build aging buckets.
    - You may choose bucket ranges dynamically (for example):
        CASE
          WHEN days_overdue <= 30 THEN '0-30 days'
          WHEN days_overdue <= 60 THEN '31-60 days'
          WHEN days_overdue <= 90 THEN '61-90 days'
          ELSE '>90 days'
        END AS aging_bucket
    - Group by vendor and aging_bucket:
        GROUP BY mserp_accountnum, aging_bucket

MULTI-SCENARIO HANDLING:
- The user may ask for MORE THAN ONE analytic view in a single question.
  Examples:
    * "Vendor aging report AND this week's expected payments"
    * "벤더별 채무연령분석과 금주 지급예상액"
- In such cases, generate MULTIPLE queries, one for each scenario.
- Typical scenario names:
    - "vendor_aging_report" (aging buckets by vendor)
    - "weekly_expected_payments" (this week's expected payments by due date)
    - "monthly_expected_payments"
    - "top_vendors_outstanding"
- Return 1–3 queries depending on the question.

CHART RULES:
- When the question clearly implies a time series (trend, over months, by date, by week, by day):
    → chart_type = "line"
- When the question compares categories (vendors, companies, aging buckets):
    → chart_type = "bar" or "pie"
- If unsure, prefer "bar" for categories and "line" for date/time series.

OUTPUT FORMAT:
You MUST output ONLY a JSON object with this exact structure:

{
  "queries": [
    {
      "name": "<short_scenario_name>",
      "sql": "<SQL query to answer that scenario>",
      "chart_type": "<one of: 'bar', 'line', 'pie'>",
      "table_name": "<table key for JSON output>"
    },
    ...
  ]
}

RULES:
- No explanations.
- No markdown.
- No comments.
- SQL MUST be syntactically valid for SQLite.
- Use clear column aliases (e.g., vendor, due_date, month, aging_bucket, expected_payment, outstanding).
- When the question asks for both vendor aging and weekly expected payments, include TWO queries:
    1) vendor_aging_report
    2) weekly_expected_payments
"""

    user_content = {
        "today": today,
        "natural_language_query": query,
        "context": rag_context,
    }

    try:
        resp = aoai_client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": json.dumps(user_content)},
            ],
            max_tokens=700,
            temperature=0,
        )
        raw = resp.choices[0].message.content or ""
        raw = raw.strip()

        # strip code fences if present
        if raw.startswith("```"):
            raw = raw.replace("```json", "").replace("```", "").strip()

        first = raw.find("{")
        last = raw.rfind("}")
        if first != -1 and last != -1:
            raw = raw[first:last+1]

        obj = json.loads(raw)

        # New format: {"queries": [ ... ]}
        queries = obj.get("queries")
        if queries is None:
            # Backward-compat: old single-query format
            sql = obj.get("sql")
            if not sql:
                raise ValueError("No SQL generated from LLM")
            chart_type = obj.get("chart_type", "bar")
            if chart_type not in ("bar", "line", "pie"):
                chart_type = "bar"
            table_name = obj.get("table_name", "rows")
            name = obj.get("name", table_name)
            queries = [{
                "name": name,
                "sql": sql,
                "chart_type": chart_type,
                "table_name": table_name,
            }]

        # Normalize and validate
        normalized: List[Dict[str, Any]] = []
        for q in queries:
            sql = q.get("sql")
            if not sql:
                continue
            chart_type = q.get("chart_type", "bar")
            if chart_type not in ("bar", "line", "pie"):
                chart_type = "bar"
            table_name = q.get("table_name") or q.get("name") or "rows"
            name = q.get("name") or table_name
            normalized.append({
                "name": name,
                "sql": sql,
                "chart_type": chart_type,
                "table_name": table_name,
            })

        if not normalized:
            raise ValueError("No valid SQL queries generated from LLM")

        return normalized

    except Exception as e:
        logger.exception("Error generating SQL with LLM: %s", e)
        raise
# ======================================================
# FASTAPI APP
# ======================================================

app = FastAPI(title="Payables Intelligence Engine (RAG + Dataverse + Azure OpenAI + SQLite)")

# ======================================================
# STARTUP EVENT – WARM UP CACHE + RAG
# ======================================================

@app.on_event("startup")
def warm_up_cache_and_rag():
    try:
        logger.info("🚀 Starting warm-up: Dataverse → SQLite + RAG index...")

        # 1. Build SQLite cache (will fetch all Dataverse tables)
        conn = build_sqlite_database_cached()
        logger.info("✅ Dataverse → SQLite cache built successfully.")

        # 2. Build RAG index (schema + rules + examples)
        build_rag_index(conn)
        logger.info("✅ RAG index ready.")

        logger.info("🔥 Warm-up completed successfully!")

    except Exception as e:
        logger.error(f"❌ Warm-up failed: {e}")


class QueryRequest(BaseModel):
    query: str


@app.post("/llm/query")
def llm_query(req: QueryRequest):
    try:
        # 1) Build / reuse SQLite DB
        conn = build_sqlite_database_cached()

        # 2) Get RAG context for this query
        rag_context = get_rag_context(conn, req.query)

        # 3) Ask LLM to generate one or more SQL plans
        plans = generate_sql_with_llm(req.query, rag_context)

        tables: Dict[str, Any] = {}
        charts: Dict[str, str] = {}
        all_dfs: List[pd.DataFrame] = []
        html_sections: List[str] = []

        for plan in plans:
            sql = plan["sql"]
            chart_type = plan["chart_type"]
            table_name = plan["table_name"]
            scenario_name = plan["name"]

            logger.info("Generated SQL (%s): %s", scenario_name, sql.replace("\n", " "))

            # 4) Execute SQL
            df = pd.read_sql_query(sql, conn)
            all_dfs.append(df)

            # 5) Build chart data (first col = labels, first numeric col = values)
            labels: List[str] = []
            values: List[float] = []

            if not df.empty:
                cols = df.columns.tolist()
                if cols:
                    label_col = cols[0]
                    value_col = None

                    # find first numeric column after label
                    for col in cols[1:]:
                        if pd.api.types.is_numeric_dtype(df[col]):
                            value_col = col
                            break

                    # fallback: if only one numeric col or mis-detected
                    if value_col is None and len(cols) > 1:
                        value_col = cols[1]

                    labels = df[label_col].astype(str).tolist()
                    if value_col is not None:
                        values = df[value_col].astype(float).tolist()

            chart_url = make_chart_url(chart_type, labels, values)

            if chart_url is None:
                chart_url = (
                    "https://quickchart.io/chart?c="
                    "{type:'bar',data:{labels:[''],datasets:[{label:'Chart is not generated for this query',data:[0]}]},"
                    "options:{scales:{y:{display:false},x:{display:false}},plugins:{legend:{labels:{fontSize:18}}}}}"
                )



            # 6) Build tables payload (key: table_name)
            tables[table_name] = df.to_dict(orient="records")

            # 7) Store chart with scenario-based key
            charts_key = f"{table_name}_chart"
            charts[charts_key] = chart_url

            # 8) Build HTML table section
            html_sections.append(f"<h3>{scenario_name}</h3>")
            html_sections.append(build_html_table_generic(df))

        # Combine all dataframes for summary
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
        else:
            combined_df = pd.DataFrame()

        # 9) Business summary
        business_summary = summarize_business_output(req.query, "general", None, combined_df)

        # 10) Attach HTML sections + bullet summary
        html_table = "".join(html_sections) if html_sections else "<i>No data returned.</i><br><br>"
        response_body = html_table + business_summary

        # For backward compatibility: expose first chart as chart_url (string)
        first_chart_url = next(iter(charts.values()), "No chart generated")

        return {
            "scenario": "payables",
            "response": response_body,
            "tables": tables,
            "charts": charts,
            "chart_url": first_chart_url,
        }

    except Exception as e:
        logger.exception("Error during /llm/query")
        raise HTTPException(status_code=500, detail=str(e))
