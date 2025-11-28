import os
import requests
import msal
import pandas as pd
import sqlite3
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from openai import AzureOpenAI
from datetime import datetime, timedelta
import logging

# =====================================================
# Logging Setup
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="\n==== LOG | %(asctime)s | %(levelname)s ====\n%(message)s\n",
)
logger = logging.getLogger("payables_backend")

# =====================================================
# Load Environment Variables
# =====================================================
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")

TABLE_COMPANY = os.getenv("TABLE_COMPANY")
TABLE_VENDTRANS = os.getenv("TABLE_VENDTRANS")
TABLE_VENDTABLE = os.getenv("TABLE_VENDTABLE")
TABLE_PARTYTABLE = os.getenv("TABLE_PARTYTABLE")

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

client_aoai = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)

# =====================================================
# Authenticate to Dataverse
# =====================================================
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = [f"{DATAVERSE_URL}/.default"]

logger.info("Authenticating to Dataverse via MSAL...")
msal_app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    client_credential=CLIENT_SECRET,
)

token_result = msal_app.acquire_token_for_client(scopes=SCOPE)
if "access_token" not in token_result:
    logger.error(f"Token fetch failed: {token_result}")
    raise Exception("Token fetch failed", token_result)

ACCESS_TOKEN = token_result["access_token"]
logger.info("✅ Access token acquired successfully.")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json",
}

# =====================================================
# Fetch all Dataverse Pages
# =====================================================
def fetch_all(url: str):
    logger.info(f"Fetching data from Dataverse: {url}")
    rows = []
    while True:
        res = requests.get(url, headers=HEADERS)
        res.raise_for_status()
        data = res.json()
        batch = data.get("value", [])
        rows.extend(batch)
        logger.info(f"  Retrieved {len(batch)} rows (cumulative {len(rows)})")
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link
        logger.info("  Following @odata.nextLink to next page...")
    logger.info(f"Total rows fetched: {len(rows)}")
    return rows


# =====================================================
# Global Pandas DataFrames (for payables logic)
# =====================================================
DF_COMPANY = None
DF_VENDTRANS = None
DF_VENDTABLE = None
DF_PARTY = None


# =====================================================
# Load All Tables into Thread-Safe SQLite + Pandas
# =====================================================
def load_all_tables():
    """
    Fetch Dataverse tables, normalize JOIN keys, store into:
    - global Pandas DataFrames (for payables logic)
    - in-memory SQLite (for NL→SQL engine)
    """
    global DF_COMPANY, DF_VENDTRANS, DF_VENDTABLE, DF_PARTY

    logger.info("🚀 Loading all tables from Dataverse...")

    df_company = pd.DataFrame(
        fetch_all(f"{DATAVERSE_URL}/api/data/v9.2/{TABLE_COMPANY}?$top=5000")
    )
    df_vendtrans = pd.DataFrame(
        fetch_all(f"{DATAVERSE_URL}/api/data/v9.2/{TABLE_VENDTRANS}?$top=5000")
    )
    df_vendtable = pd.DataFrame(
        fetch_all(f"{DATAVERSE_URL}/api/data/v9.2/{TABLE_VENDTABLE}?$top=5000")
    )
    df_party = pd.DataFrame(
        fetch_all(f"{DATAVERSE_URL}/api/data/v9.2/{TABLE_PARTYTABLE}?$top=5000")
    )

    logger.info(
        f"Raw counts - Company: {len(df_company)}, "
        f"VendTrans: {len(df_vendtrans)}, VendTable: {len(df_vendtable)}, "
        f"DirParty: {len(df_party)}"
    )

    # Drop unnamed columns if any
    for name, df in [
        ("Company", df_company),
        ("VendTrans", df_vendtrans),
        ("VendTable", df_vendtable),
        ("DirPartyTable", df_party),
    ]:
        before = df.shape[1]
        df.drop(
            columns=[c for c in df.columns if "Unnamed" in c],
            inplace=True,
            errors="ignore",
        )
        after = df.shape[1]
        if before != after:
            logger.info(f"Dropped unnamed columns in {name}: {before} -> {after}")

    logger.info("🔑 Normalizing join keys for all tables...")

    # --------- NORMALIZE JOIN KEYS ---------
    # Company code
    if "mserp_name" in df_company.columns:
        df_company["mserp_name"] = (
            df_company["mserp_name"].astype(str).str.strip().str.upper()
        )

    # VendTrans: dataareaid + accountnum
    if "mserp_dataareaid" in df_vendtrans.columns:
        df_vendtrans["mserp_dataareaid"] = (
            df_vendtrans["mserp_dataareaid"].astype(str).str.strip().str.upper()
        )
    if "mserp_accountnum" in df_vendtrans.columns:
        df_vendtrans["mserp_accountnum"] = (
            df_vendtrans["mserp_accountnum"].astype(str).str.strip()
        )

    # VendTable: dataareaid + accountnum + party
    if "mserp_dataareaid" in df_vendtable.columns:
        df_vendtable["mserp_dataareaid"] = (
            df_vendtable["mserp_dataareaid"].astype(str).str.strip().str.upper()
        )
    if "mserp_accountnum" in df_vendtable.columns:
        df_vendtable["mserp_accountnum"] = (
            df_vendtable["mserp_accountnum"].astype(str).str.strip()
        )
    if "mserp_party" in df_vendtable.columns:
        df_vendtable["mserp_party"] = (
            df_vendtable["mserp_party"].astype(str).str.strip()
        )

    # DirPartyTable: sourcekey
    if "mserp_sourcekey" in df_party.columns:
        df_party["mserp_sourcekey"] = (
            df_party["mserp_sourcekey"].astype(str).str.strip()
        )

    # Store in globals for Pandas-based payables logic
    DF_COMPANY = df_company.copy()
    DF_VENDTRANS = df_vendtrans.copy()
    DF_VENDTABLE = df_vendtable.copy()
    DF_PARTY = df_party.copy()

    logger.info("✅ Global DataFrames stored for payables logic.")
    logger.info(
        f"DF sizes - Company: {len(DF_COMPANY)}, VendTrans: {len(DF_VENDTRANS)}, "
        f"VendTable: {len(DF_VENDTABLE)}, DirParty: {len(DF_PARTY)}"
    )

    # THREAD-SAFE connection for NL2SQL engine
    conn = sqlite3.connect(":memory:", check_same_thread=False)

    # Insert normalized data into SQLite
    logger.info("💾 Inserting normalized tables into in-memory SQLite...")
    df_company.to_sql("Company", conn, if_exists="replace", index=False)
    df_vendtrans.to_sql("VendTrans", conn, if_exists="replace", index=False)
    df_vendtable.to_sql("VendTable", conn, if_exists="replace", index=False)
    df_party.to_sql("DirPartyTable", conn, if_exists="replace", index=False)
    logger.info("✅ All tables inserted into SQLite successfully.")

    return conn


sqlite_conn = load_all_tables()

# =====================================================
# Payables Core Logic (Pandas only)
# =====================================================

def _ensure_loaded():
    if any(df is None for df in [DF_COMPANY, DF_VENDTRANS, DF_VENDTABLE, DF_PARTY]):
        logger.error("Global DataFrames not loaded correctly.")
        raise RuntimeError("Global DataFrames not loaded. Check load_all_tables().")


def build_payables_base_df(base_date: datetime | None = None) -> pd.DataFrame:
    """
    Build the fully joined AP dataset with computed flags and aging info.
    """
    _ensure_loaded()
    if base_date is None:
        base_date = datetime.utcnow()

    logger.info(f"🧮 Building payables base DataFrame for base_date={base_date.date()}")

    # Copy raw data
    company = DF_COMPANY.copy()
    vendtrans = DF_VENDTRANS.copy()
    vendtable = DF_VENDTABLE.copy()
    dirparty = DF_PARTY.copy()

    logger.info(
        f"Raw DF sizes - Company: {len(company)}, VendTrans: {len(vendtrans)}, "
        f"VendTable: {len(vendtable)}, DirParty: {len(dirparty)}"
    )

    # Defensive normalization
    company["mserp_name"] = company["mserp_name"].astype(str).str.strip().str.upper()
    vendtrans["mserp_dataareaid"] = (
        vendtrans["mserp_dataareaid"].astype(str).str.strip().str.upper()
    )
    vendtable["mserp_dataareaid"] = (
        vendtable["mserp_dataareaid"].astype(str).str.strip().str.upper()
    )

    vendtrans["mserp_accountnum"] = vendtrans["mserp_accountnum"].astype(str).str.strip()
    vendtable["mserp_accountnum"] = vendtable["mserp_accountnum"].astype(str).str.strip()
    vendtable["mserp_party"] = vendtable["mserp_party"].astype(str).str.strip()
    dirparty["mserp_sourcekey"] = dirparty["mserp_sourcekey"].astype(str).str.strip()

    # Convert date columns in VendTrans
    for col in ["mserp_transdate", "mserp_duedate", "mserp_closed"]:
        if col in vendtrans.columns:
            vendtrans[col] = pd.to_datetime(vendtrans[col], errors="coerce")

    # Rename company columns
    company = company.rename(
        columns={
            "mserp_name": "CompanyCode",
            "mserp_description": "CompanyName",
            "mserp_accountingcurrency": "CompanyCurrency",
        }
    )

    # ---- Step 1: Company ↔ VendTrans ----
    merged = company.merge(
        vendtrans,
        left_on="CompanyCode",
        right_on="mserp_dataareaid",
        how="inner",
    )
    logger.info(f"After Company↔VendTrans join: {len(merged)} rows")

    # ---- Step 2: VendTrans ↔ VendTable ----
    merged = merged.merge(
        vendtable,
        on=["mserp_accountnum", "mserp_dataareaid"],
        how="inner",
        suffixes=("", "_VendTable"),
    )
    logger.info(f"After VendTrans↔VendTable join: {len(merged)} rows")

    # ---- Step 3: VendTable ↔ DirParty (Vendor Name) ----
    merged = merged.merge(
        dirparty,
        left_on="mserp_party",
        right_on="mserp_sourcekey",
        how="inner",
        suffixes=("", "_DirParty"),
    )
    logger.info(f"After VendTable↔DirParty join: {len(merged)} rows")

    # Vendor name from DirPartyTable.mserp_name
    if "mserp_name" in merged.columns:
        merged = merged.rename(columns={"mserp_name": "VendorName"})
    merged["VendorCode"] = merged["mserp_accountnum"]

    # Exclude system company DAT
    before_dat = len(merged)
    merged = merged[merged["CompanyCode"] != "DAT"]
    after_dat = len(merged)
    if before_dat != after_dat:
        logger.info(f"Filtered out DAT company rows: {before_dat} -> {after_dat}")

    # ---- Add payables core logic ----
    merged = add_payables_logic(merged)
    merged = add_aging_buckets(merged, base_date)

    logger.info(f"✅ Final payables base DF rows: {len(merged)}")
    return merged


def add_payables_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add core AP fields:
    - amount_mst, settle_amount_mst
    - balance_mst (매입지급잔액)
    - is_purchase, is_payment
    - closed_valid (정산완료일 존재)
    """
    df = df.copy()

    for col in ["mserp_amountmst", "mserp_settleamountmst"]:
        if col not in df.columns:
            logger.error(f"Column {col} missing in AP dataset.")
            raise ValueError(f"Column {col} missing in AP dataset.")

    df["amount_mst"] = pd.to_numeric(df["mserp_amountmst"], errors="coerce").fillna(0)
    df["settle_amount_mst"] = pd.to_numeric(
        df["mserp_settleamountmst"], errors="coerce"
    ).fillna(0)

    # 매입지급잔액 = (amount - settle) * -1
    df["balance_mst"] = (df["amount_mst"] - df["settle_amount_mst"]) * -1

    # Purchase vs Payment flags
    df["is_purchase"] = df["amount_mst"] < 0  # 매입
    df["is_payment"] = df["amount_mst"] > 0   # 지급

    sentinel = pd.Timestamp(1900, 1, 1)
    df["closed_valid"] = df["mserp_closed"].notna() & (df["mserp_closed"] > sentinel)

    return df


def add_aging_buckets(df: pd.DataFrame, base_date: datetime) -> pd.DataFrame:
    """
    Add aging buckets based on DueDate vs base_date.

    Buckets (for open, purchase transactions):
    - OVERDUE: DueDate < base_date
    - 0-30:    0 <= days_to_due <= 30
    - 31-60:   31 <= days_to_due <= 60
    - 61-90:   61 <= days_to_due <= 90
    - 90+:     days_to_due > 90
    """
    df = df.copy()

    if "mserp_duedate" not in df.columns:
        df["aging_bucket"] = None
        return df

    base_d = pd.Timestamp(base_date.date())

    df["days_to_due"] = (
        df["mserp_duedate"].dt.normalize() - base_d
    ).dt.days

    df["is_overdue"] = df["days_to_due"] < 0

    def bucket(days):
        if pd.isna(days):
            return None
        d = int(days)
        if d < 0:
            return "OVERDUE"
        if 0 <= d <= 30:
            return "0-30"
        if 31 <= d <= 60:
            return "31-60"
        if 61 <= d <= 90:
            return "61-90"
        if d > 90:
            return "90+"
        return None

    open_purchase_mask = (
        df["is_purchase"]
        & (~df["closed_valid"])
        & df["mserp_duedate"].notna()
    )

    df.loc[open_purchase_mask, "aging_bucket"] = df.loc[
        open_purchase_mask, "days_to_due"
    ].apply(bucket)

    return df


def get_overdue_summary(df: pd.DataFrame, base_date: datetime) -> dict:
    """
    Overdue summary:
    - Only purchase, open, with DueDate < base_date.
    """
    base_d = pd.Timestamp(base_date.date())
    mask = (
        df["is_purchase"]
        & (~df["closed_valid"])
        & df["mserp_duedate"].notna()
        & (df["mserp_duedate"] < base_d)
    )
    overdue_dues = df.loc[mask, "mserp_duedate"]
    overdue_balance = df.loc[mask, "balance_mst"].sum()

    logger.info(
        f"Overdue summary as of {base_d.date()}: count={int(mask.sum())}, "
        f"total_balance={float(overdue_balance)}"
    )

    return {
        "base_date": str(base_d.date()),
        "overdue_count": int(mask.sum()),
        "min_overdue_duedate": (
            overdue_dues.min().strftime("%Y-%m-%d") if not overdue_dues.empty else None
        ),
        "max_overdue_duedate": (
            overdue_dues.max().strftime("%Y-%m-%d") if not overdue_dues.empty else None
        ),
        "total_overdue_balance_mst": float(overdue_balance),
    }


def get_week_range(base_date: datetime):
    """
    Get Sunday–Saturday week range containing base_date.
    """
    # Monday=0 ... Sunday=6
    weekday = base_date.weekday()
    # Previous Sunday (as "week start")
    sunday = base_date - timedelta(days=(weekday + 1) % 7)
    saturday = sunday + timedelta(days=6)
    return sunday.date(), saturday.date()


def get_weekly_expected_payments(df: pd.DataFrame, base_date: datetime) -> pd.DataFrame:
    """
    금주 지급액:
    - 기준: DueDate in this week (Sunday–Saturday)
    - 대상: 매입 (amount < 0), open (not closed_valid)
    - 금액: 매입지급잔액 (balance_mst)
    """
    week_start, week_end = get_week_range(base_date)
    logger.info(
        f"Calculating weekly expected payments for week {week_start} to {week_end}"
    )

    mask = (
        df["is_purchase"]
        & (~df["closed_valid"])
        & df["mserp_duedate"].notna()
        & (df["mserp_duedate"].dt.date >= week_start)
        & (df["mserp_duedate"].dt.date <= week_end)
    )

    weekly = (
        df.loc[mask]
        .assign(DueDateDate=lambda x: x["mserp_duedate"].dt.date)
        .groupby(
            [
                "CompanyCode",
                "CompanyName",
                "CompanyCurrency",
                "DueDateDate",
            ],
            as_index=False,
        )["balance_mst"]
        .sum()
        .rename(
            columns={
                "DueDateDate": "DueDate",
                "balance_mst": "WeeklyExpectedPaymentMST",
            }
        )
    )

    logger.info(f"Weekly expected payments rows: {len(weekly)}")
    return weekly


def get_company_aging_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by Company & aging bucket and sum balance_mst.
    Only open purchase items.
    """
    mask = (
        df["is_purchase"]
        & (~df["closed_valid"])
        & df["mserp_duedate"].notna()
        & df["aging_bucket"].notna()
    )

    grouped = (
        df.loc[mask]
        .groupby(
            ["CompanyCode", "CompanyName", "CompanyCurrency", "aging_bucket"],
            as_index=False,
        )["balance_mst"]
        .sum()
        .rename(
            columns={
                "aging_bucket": "Bucket",
                "balance_mst": "AmountMST",
            }
        )
        .sort_values(["CompanyCode", "Bucket"])
    )

    logger.info(f"Company aging summary rows: {len(grouped)}")
    return grouped


# =====================================================
# Request Model
# =====================================================
class Query(BaseModel):
    question: str


# =====================================================
# SQL Generator Prompt (SQLite-aware, balance_mst-aware)
# =====================================================
SQL_PROMPT = """
You are an expert SQL generator for financial data.

The database is **SQLite**. You MUST generate SQL that is valid for SQLite.

Date rules:
- Use only SQLite date functions:
  - DATE('now')
  - DATE('now', '+30 days')
  - DATE('now', '-30 days')
  - DATE('now', '+60 days')
  - DATE('now', '+90 days')
  - DATE(column_name)
- For date ranges, use:
  - BETWEEN DATE('now', '-30 days') AND DATE('now')      -- last 30 days (past)
  - BETWEEN DATE('now') AND DATE('now', '+30 days')      -- next 30 days (future)
- You MAY use JULIANDAY() if you need date differences.
- Do NOT use SQL Server / T-SQL functions:
  - DATEADD, DATEDIFF, GETDATE, CURRENT_TIMESTAMP + interval, DAY(), MONTH(), YEAR()

IMPORTANT interpretation:
- If the user says: "within 30 days as of the current date"
  → interpret as the **last 30 days up to today**:
    DATE(column) BETWEEN DATE('now', '-30 days') AND DATE('now').
- If the user says: "within the last 30 days", "past 30 days"
  → also use DATE('now', '-30 days') to DATE('now').
- If the user explicitly says: "within the next 30 days", "upcoming 30 days"
  → use DATE('now') to DATE('now', '+30 days').

Payables amount rule (VERY IMPORTANT):
- For ANY question about:
  - payables, outstanding amounts, balances, vendor balance, company balance,
  - aging, overdue, amounts due within/past X days, due dates,
  you MUST use the following expression as the amount in base currency:

  (VendTrans.mserp_amountmst - VendTrans.mserp_settleamountmst) * -1

- When you aggregate this, ALWAYS wrap it with COALESCE(SUM(...), 0) so that the result is never NULL.

  Example:
  COALESCE(SUM((VendTrans.mserp_amountmst - VendTrans.mserp_settleamountmst) * -1), 0) AS TotalPayables

Open purchase filter (for payables and amounts due):
- Always restrict to open purchase items:
  VendTrans.mserp_amountmst < 0
  AND (VendTrans.mserp_closed IS NULL OR VendTrans.mserp_closed < '1900-01-01')

Tables you can use (already loaded in SQLite):
- Company
- VendTrans
- VendTable
- DirPartyTable

Standard join (keys are normalized to UPPERCASE already):

SELECT
    Company.mserp_name AS CorporateCode,
    Company.mserp_description AS CorporateName,
    Company.mserp_accountingcurrency,
    VendTrans.mserp_accountnum AS VendorCode,
    VendTrans.mserp_currencycode AS Currency,
    VendTrans.mserp_amountcur AS Amount_FCY,
    VendTrans.mserp_amountmst AS Amount_KRW,
    (VendTrans.mserp_amountcur - VendTrans.mserp_settleamountcur) AS Balance_FCY,
    (VendTrans.mserp_amountmst - VendTrans.mserp_settleamountmst) AS Balance_KRW,
    VendTrans.mserp_transdate,
    VendTrans.mserp_duedate,
    VendTrans.mserp_closed,
    DirPartyTable.mserp_name AS VendorName
FROM Company
JOIN VendTrans
    ON VendTrans.mserp_dataareaid = Company.mserp_name
JOIN VendTable
    ON VendTable.mserp_accountnum = VendTrans.mserp_accountnum
   AND VendTable.mserp_dataareaid = VendTrans.mserp_dataareaid
JOIN DirPartyTable
    ON DirPartyTable.mserp_sourcekey = VendTable.mserp_party
WHERE Company.mserp_name <> 'DAT';

Example for this specific question:

"Total amount with a due date within 30 days as of the current date"
→ interpret as **last 30 days**:

SELECT
    COALESCE(SUM((VendTrans.mserp_amountmst - VendTrans.mserp_settleamountmst) * -1), 0)
        AS TotalAmountDueLast30Days
FROM Company
JOIN VendTrans
    ON VendTrans.mserp_dataareaid = Company.mserp_name
JOIN VendTable
    ON VendTable.mserp_accountnum = VendTrans.mserp_accountnum
   AND VendTable.mserp_dataareaid = VendTrans.mserp_dataareaid
JOIN DirPartyTable
    ON DirPartyTable.mserp_sourcekey = VendTable.mserp_party
WHERE Company.mserp_name <> 'DAT'
  AND VendTrans.mserp_amountmst < 0
  AND (VendTrans.mserp_closed IS NULL OR VendTrans.mserp_closed < '1900-01-01')
  AND DATE(VendTrans.mserp_duedate)
      BETWEEN DATE('now','-30 days') AND DATE('now');

Rules:
- Return ONLY pure SQL text (no markdown, no ```sql).
- Do NOT hallucinate column names.
"""

# =====================================================
# SQL Cleanup
# =====================================================
def clean_sql(sql: str) -> str:
    return (
        sql.replace("```sql", "")
        .replace("```", "")
        .replace("`", "")
        .strip()
    )


# =====================================================
# SQL Generation
# =====================================================
def generate_sql(question: str) -> str:
    logger.info("🎯 Generating SQL from NL question using Azure OpenAI...")
    response = client_aoai.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": SQL_PROMPT},
            {"role": "user", "content": question},
        ],
    )
    sql = response.choices[0].message.content
    sql_clean = clean_sql(sql)
    logger.info(f"Generated SQL:\n{sql_clean}")
    return sql_clean


# =====================================================
# Natural Language Summary Prompt
# =====================================================
SUMMARY_PROMPT = """
You are an expert financial data analyst.

Given:
- The user's natural language question.
- The SQL query result as JSON records.

Produce a clear, concise natural-language summary that:
- Mentions vendor or company names when relevant.
- Explains totals (e.g., total amount within the last 30 days).
- Mentions currencies (FCY / KRW) where available.
- Is suitable as a chatbot answer.

If the total is 0 or the result is empty, clearly say that there are no applicable amounts for that condition.

Do NOT return code, tables, or SQL. Just plain sentences or bullet points.
"""

# =====================================================
# Generate Natural Language Summary
# =====================================================
def generate_summary(question: str, df: pd.DataFrame) -> str:
    data_json = df.to_dict(orient="records")

    logger.info("🧾 Calling AOAI to generate natural language summary...")
    response = client_aoai.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": SUMMARY_PROMPT},
            {"role": "user", "content": f"User Question: {question}"},
            {
                "role": "user",
                "content": f"SQL Output Data (list of records): {data_json}",
            },
        ],
    )
    summary = response.choices[0].message.content.strip()
    logger.info(f"Summary generated:\n{summary}")
    return summary


# =====================================================
# FastAPI App
# =====================================================
app = FastAPI()


@app.post("/chat")
def chat(q: Query):
    logger.info("==========================================")
    logger.info("🚀 New /chat Request")
    logger.info(f"Question: {q.question}")
    logger.info("==========================================")

    # 1) NL -> SQL
    sql_query = generate_sql(q.question)

    try:
        # 2) Execute SQL
        logger.info("📌 Executing SQL in SQLite...")
        logger.info(f"SQL:\n{sql_query}")
        df = pd.read_sql_query(sql_query, sqlite_conn)
        logger.info(f"📌 SQL Execution Complete. Returned Rows: {len(df)}")

        # 3) Generate natural-language summary
        logger.info("📌 Generating Summary...")
        if not df.empty:
            logger.info("📌 Sample data (first 5 rows):")
            logger.info(df.head().to_string())
        else:
            logger.info("📌 DataFrame is empty. No rows to preview.")

        summary = generate_summary(q.question, df)

        return {
            "question": q.question,
            "sql": sql_query,
            "rows": len(df),
            "summary": summary,
            "data": df.to_dict(orient="records"),
        }

    except Exception as e:
        logger.error("❌ SQL Execution Failed!")
        logger.error(str(e))

        return {
            "error": str(e),
            "sql_generated": sql_query,
        }


# =====================================================
# New Payables Endpoints (Pandas-based)
# =====================================================

@app.get("/payables/overdue")
def payables_overdue(base_date: str | None = None):
    """
    Overdue summary as of base_date (YYYY-MM-DD). If not provided, uses today's date (UTC).
    """
    if base_date:
        base_dt = datetime.fromisoformat(base_date)
    else:
        base_dt = datetime.utcnow()

    logger.info(f"🌐 /payables/overdue called with base_date={base_dt.date()}")

    df = build_payables_base_df(base_dt)
    summary = get_overdue_summary(df, base_dt)
    return summary


@app.get("/payables/weekly")
def payables_weekly(base_date: str | None = None):
    """
    금주 지급예상액 (weekly expected payments) per company & day within the week of base_date.
    base_date format: YYYY-MM-DD, default = today (UTC).
    """
    if base_date:
        base_dt = datetime.fromisoformat(base_date)
    else:
        base_dt = datetime.utcnow()

    logger.info(f"🌐 /payables/weekly called with base_date={base_dt.date()}")

    df = build_payables_base_df(base_dt)
    weekly_df = get_weekly_expected_payments(df, base_dt)
    week_start, week_end = get_week_range(base_dt)
    return {
        "base_date": str(base_dt.date()),
        "week_start": str(week_start),
        "week_end": str(week_end),
        "rows": len(weekly_df),
        "data": weekly_df.to_dict(orient="records"),
    }


@app.get("/payables/aging/company")
def payables_aging_company(base_date: str | None = None):
    """
    Company-wise aging summary:
    CompanyCode / CompanyName / CompanyCurrency / Bucket / AmountMST
    Buckets: OVERDUE, 0-30, 31-60, 61-90, 90+
    """
    if base_date:
        base_dt = datetime.fromisoformat(base_date)
    else:
        base_dt = datetime.utcnow()

    logger.info(f"🌐 /payables/aging/company called with base_date={base_dt.date()}")

    df = build_payables_base_df(base_dt)
    aging_df = get_company_aging_summary(df)
    return {
        "base_date": str(base_dt.date()),
        "rows": len(aging_df),
        "data": aging_df.to_dict(orient="records"),
    }
