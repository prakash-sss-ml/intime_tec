import os
import json
import urllib.parse
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv

# ===============================================================
# 1. ENV + CONSTANTS
# ===============================================================

load_dotenv()

TENANT_ID = os.getenv("TENANT_ID", "").strip()
CLIENT_ID = os.getenv("CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "").strip()
DATAVERSE_URL = os.getenv("DATAVERSE_URL", "").strip()

# BI entity names – must match Dataverse table names
TABLE_LEDGER = "mserp_ledgerbientities"
TABLE_VENDTRANS = "mserp_vendtransbientities"
TABLE_VENDTABLE = "mserp_vendtablebientities"
TABLE_DIRPARTY = "mserp_dirpartytablebientities"

TABLE_CUSTTRANS = "mserp_custtransbientities"
TABLE_CUSTTABLE = "mserp_custtablebientities"

# Korea time offset
KOREA_UTC_OFFSET_HOURS = 9

CACHE = {
    "access_token": None,
    "access_token_expires": None,
    "payables_view": None,
    "payables_built_at": None,
    "receivables_view": None,
    "receivables_built_at": None,
}

app = FastAPI()


# ===============================================================
# 2. BASE MODELS
# ===============================================================

class QueryRequest(BaseModel):
    query: str


class UnpaidVendTransQuery(BaseModel):
    page_number: int = 1
    page_size: int = 50
    due_date_from: date | None = None
    due_date_to: date | None = None


# ===============================================================
# 3. JSON SANITIZER (Fixes NaN / inf errors)
# ===============================================================

def safe_json(obj):
    """Recursively convert NaN / Infinity → None to make FastAPI JSON-safe."""
    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_json(v) for v in obj]
    elif isinstance(obj, float):
        if obj != obj:  # NaN
            return None
        if obj in (float("inf"), float("-inf")):
            return None
        return obj
    return obj


def build_json_response(
    scenario: str,
    text: str,
    tables: dict | None = None,
    chart_url: str | None = None,
):
    """
    Unified envelope. Every /llm/query response uses this:

    {
      "scenario": "...",
      "response": "text...",
      "tables": {...},   # possibly with empty table(s)
      "chart_url": "..." or null
    }
    """
    payload = {
        "scenario": scenario,
        "response": text,
        "tables": tables or {},
        "chart_url": chart_url,
    }
    return safe_json(payload)


# ===============================================================
# 4. BASIC HELPERS
# ===============================================================

def clean_float(v):
    """Ensure float is valid JSON (no NaN, no inf)."""
    try:
        if v is None:
            return 0.0
        f = float(v)
        if f != f:          # NaN
            return 0.0
        if f in (float("inf"), float("-inf")):
            return 0.0
        return f
    except:
        return 0.0


def parse_date_safe(v):
    """Normalize Dataverse date/datetime to Python date."""
    if v is None:
        return None
    try:
        if isinstance(v, str):
            if v.endswith("Z"):
                v = v.replace("Z", "+00:00")
            return datetime.fromisoformat(v).date()
        return pd.to_datetime(v).date()
    except:
        return None


def now_korea():
    return datetime.utcnow() + timedelta(hours=KOREA_UTC_OFFSET_HOURS)


def fmt_amount(v):
    v = clean_float(v)
    try:
        return f"{int(round(v)):,.0f}"
    except:
        return "0"


# ===============================================================
# 5. AUTH + DATAVERSE FETCH (Generic)
# ===============================================================

def get_access_token():
    from msal import ConfidentialClientApplication

    now = datetime.utcnow()
    if CACHE["access_token"] and CACHE["access_token_expires"]:
        if now < CACHE["access_token_expires"]:
            return CACHE["access_token"]

    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scope = [f"{DATAVERSE_URL}/.default"]

    app_msal = ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET,
    )

    token = app_msal.acquire_token_for_client(scopes=scope)
    if "access_token" not in token:
        raise Exception("Token fetch failed:", token)

    CACHE["access_token"] = token["access_token"]
    CACHE["access_token_expires"] = now + timedelta(minutes=55)
    return token["access_token"]


def dataverse_get_table(table_name, select=None, filter_=None, max_pages=50):
    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }

    base = f"{DATAVERSE_URL}/api/data/v9.2/{table_name}"
    params = {}
    if select:
        params["$select"] = select
    if filter_:
        params["$filter"] = filter_

    rows = []
    next_url = base
    page = 0

    while next_url and page < max_pages:
        resp = requests.get(next_url, headers=headers, params=params if page == 0 else None)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("value", []))
        next_url = data.get("@odata.nextLink")
        page += 1

    return pd.DataFrame(rows)
# ===============================================================
# 6. BUILD PAYABLES VIEW (AP)
# ===============================================================

def build_payables_view() -> pd.DataFrame:
    """
    Payables (Vendor) view:

    - Purchases: Amount_KRW < 0  (mserp_amountmst)
    - Payments (for trend): Settle_KRW > 0 (mserp_settleamountmst, SettlementDate)
    - Outstanding_KRW = (Amount_KRW - Settle_KRW) * -1
    """
    print("📥 Fetching PAYABLES data from Dataverse...")

    # 1) Ledger (Company)
    ledger = dataverse_get_table(
        TABLE_LEDGER,
        select="mserp_name,mserp_description,mserp_accountingcurrency",
    ).rename(
        columns={
            "mserp_name": "CorporateCode",
            "mserp_description": "CorporateName",
            "mserp_accountingcurrency": "CorporateCurrency",
        }
    )

    # 2) VendTrans (AP transactions)
    vendtrans = dataverse_get_table(
        TABLE_VENDTRANS,
        select=(
            "mserp_vendtransbientityid,"
            "mserp_accountnum,"
            "mserp_currencycode,"
            "mserp_amountcur,"
            "mserp_settleamountcur,"
            "mserp_amountmst,"
            "mserp_settleamountmst,"
            "mserp_transdate,"
            "mserp_duedate,"
            "mserp_closed,"
            "mserp_dataareaid"
        ),
    )

    vendtrans["Amount_FCY"] = vendtrans["mserp_amountcur"].apply(clean_float)
    vendtrans["Amount_KRW"] = vendtrans["mserp_amountmst"].apply(clean_float)
    vendtrans["Settle_FCY"] = vendtrans["mserp_settleamountcur"].apply(clean_float)
    vendtrans["Settle_KRW"] = vendtrans["mserp_settleamountmst"].apply(clean_float)

    vendtrans["TransactionDate"] = vendtrans["mserp_transdate"].apply(parse_date_safe)
    vendtrans["DueDate"] = vendtrans["mserp_duedate"].apply(parse_date_safe)
    vendtrans["SettlementDate"] = vendtrans["mserp_closed"].apply(parse_date_safe)

    def classify_ap_type(row: pd.Series) -> str:
        amt = clean_float(row.get("Amount_KRW"))
        if amt < 0:
            return "Purchase"
        if amt > 0:
            return "Payment"
        return "Other"

    vendtrans["Type"] = vendtrans.apply(classify_ap_type, axis=1)

    # 3) VendTable
    vendtable = dataverse_get_table(
        TABLE_VENDTABLE,
        select="mserp_accountnum,mserp_party,mserp_dataareaid",
    )

    # 4) DirParty (Vendor names)
    dirparty = dataverse_get_table(
        TABLE_DIRPARTY,
        select="mserp_sourcekey,mserp_name",
    ).rename(
        columns={
            "mserp_sourcekey": "PartyKey",
            "mserp_name": "VendorName",
        }
    )

    vendtable = vendtable.merge(
        dirparty,
        how="left",
        left_on="mserp_party",
        right_on="PartyKey",
    )

    # Join VendTrans -> VendTable
    joined = vendtrans.merge(
        vendtable,
        how="left",
        on=["mserp_accountnum", "mserp_dataareaid"],
    )

    # Join Company
    joined = joined.merge(
        ledger,
        how="left",
        left_on="mserp_dataareaid",
        right_on="CorporateCode",
    )

    # Fallback: if join failed, use raw dataareaid as corporate code
    joined.loc[joined["CorporateCode"].isna(), "CorporateCode"] = joined.loc[
        joined["CorporateCode"].isna(), "mserp_dataareaid"
    ]

    # Exclude DAT company
    joined = joined[joined["CorporateCode"] != "DAT"]

    # Dedupe by BI entity ID to avoid duplicate sums
    if "mserp_vendtransbientityid" in joined.columns:
        joined = joined.drop_duplicates(subset=["mserp_vendtransbientityid"])

    joined["Outstanding_KRW"] = (
        (joined["Amount_KRW"] - joined["Settle_KRW"]).apply(clean_float) * -1.0
    )

    joined["Balance_FCY"] = joined["Amount_FCY"] - joined["Settle_FCY"]
    joined["Balance_KRW"] = joined["Amount_KRW"] - joined["Settle_KRW"]

    print(f"✅ Payables view built. Rows: {len(joined)}")
    return joined


def get_payables_view() -> pd.DataFrame:
    if CACHE["payables_view"] is not None:
        return CACHE["payables_view"]  # type: ignore[return-value]
    df = build_payables_view()
    CACHE["payables_view"] = df
    CACHE["payables_built_at"] = datetime.utcnow()
    return df


# ===============================================================
# 7. BUILD RECEIVABLES VIEW (AR)
# ===============================================================

def build_receivables_view() -> pd.DataFrame:
    """
    Receivables (Customer) view:

    - Sales: Amount_KRW > 0
    - Collection: Amount_KRW < 0
    - Outstanding_KRW = (Amount_KRW - Settle_KRW) * -1
    """
    print("📥 Fetching RECEIVABLES data from Dataverse...")

    ledger = dataverse_get_table(
        TABLE_LEDGER,
        select="mserp_name,mserp_description,mserp_accountingcurrency",
    ).rename(
        columns={
            "mserp_name": "CorporateCode",
            "mserp_description": "CorporateName",
            "mserp_accountingcurrency": "CorporateCurrency",
        }
    )

    custtrans = dataverse_get_table(
        TABLE_CUSTTRANS,
        select=(
            "mserp_custtransbientityid,"
            "mserp_accountnum,"
            "mserp_currencycode,"
            "mserp_amountcur,"
            "mserp_settleamountcur,"
            "mserp_amountmst,"
            "mserp_settleamountmst,"
            "mserp_transdate,"
            "mserp_duedate,"
            "mserp_closed,"
            "mserp_dataareaid"
        ),
    )

    custtrans["Amount_FCY"] = custtrans["mserp_amountcur"].apply(clean_float)
    custtrans["Amount_KRW"] = custtrans["mserp_amountmst"].apply(clean_float)
    custtrans["Settle_FCY"] = custtrans["mserp_settleamountcur"].apply(clean_float)
    custtrans["Settle_KRW"] = custtrans["mserp_settleamountmst"].apply(clean_float)

    custtrans["TransactionDate"] = custtrans["mserp_transdate"].apply(parse_date_safe)
    custtrans["DueDate"] = custtrans["mserp_duedate"].apply(parse_date_safe)
    custtrans["SettlementDate"] = custtrans["mserp_closed"].apply(parse_date_safe)

    def classify_ar_type(row: pd.Series) -> str:
        amt = clean_float(row.get("Amount_KRW"))
        if amt > 0:
            return "Sales"
        if amt < 0:
            return "Collection"
        return "Other"

    custtrans["Type"] = custtrans.apply(classify_ar_type, axis=1)

    custtable = dataverse_get_table(
        TABLE_CUSTTABLE,
        select="mserp_accountnum,mserp_party,mserp_dataareaid",
    )

    dirparty = dataverse_get_table(
        TABLE_DIRPARTY,
        select="mserp_sourcekey,mserp_name",
    ).rename(
        columns={
            "mserp_sourcekey": "PartyKey",
            "mserp_name": "CustomerName",
        }
    )

    custtable = custtable.merge(
        dirparty,
        how="left",
        left_on="mserp_party",
        right_on="PartyKey",
    )

    joined = custtrans.merge(
        custtable,
        how="left",
        on=["mserp_accountnum", "mserp_dataareaid"],
    )

    joined = joined.merge(
        ledger,
        how="left",
        left_on="mserp_dataareaid",
        right_on="CorporateCode",
    )

    joined.loc[joined["CorporateCode"].isna(), "CorporateCode"] = joined.loc[
        joined["CorporateCode"].isna(), "mserp_dataareaid"
    ]

    joined = joined[joined["CorporateCode"] != "DAT"]

    if "mserp_custtransbientityid" in joined.columns:
        joined = joined.drop_duplicates(subset=["mserp_custtransbientityid"])

    joined["Outstanding_KRW"] = (
        (joined["Amount_KRW"] - joined["Settle_KRW"]).apply(clean_float) * -1.0
    )

    joined["Balance_FCY"] = joined["Amount_FCY"] - joined["Settle_FCY"]
    joined["Balance_KRW"] = joined["Amount_KRW"] - joined["Settle_KRW"]

    print(f"✅ Receivables view built. Rows: {len(joined)}")
    return joined


def get_receivables_view() -> pd.DataFrame:
    if CACHE["receivables_view"] is not None:
        return CACHE["receivables_view"]  # type: ignore[return-value]
    df = build_receivables_view()
    CACHE["receivables_view"] = df
    CACHE["receivables_built_at"] = datetime.utcnow()
    return df

# ===============================================================
# 8. PAYABLES ANALYTICS (AP)
# ===============================================================

def payables_trend_last_6_months(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    six_months_ago = today - timedelta(days=180)

    df_pur = df[df["TransactionDate"].notna()].copy()
    df_pur = df_pur[
        (df_pur["TransactionDate"] >= six_months_ago)
        & (df_pur["TransactionDate"] <= today)
    ]
    pur = df_pur[df_pur["Amount_KRW"] < 0].copy()
    if not pur.empty:
        pur["Month"] = pur["TransactionDate"].apply(lambda d: d.strftime("%Y-%m"))
        purchases_month = (
            pur.groupby("Month")["Amount_KRW"]
            .apply(lambda s: sum(abs(clean_float(v)) for v in s))
            .to_dict()
        )
    else:
        purchases_month = {}

    df_pay = df[df["SettlementDate"].notna()].copy()
    df_pay = df_pay[
        (df_pay["SettlementDate"] >= six_months_ago)
        & (df_pay["SettlementDate"] <= today)
    ]
    pay = df_pay[df_pay["Settle_KRW"] > 0].copy()
    if not pay.empty:
        pay["Month"] = pay["SettlementDate"].apply(lambda d: d.strftime("%Y-%m"))
        payments_month = (
            pay.groupby("Month")["Settle_KRW"]
            .apply(lambda s: sum(clean_float(v) for v in s))
            .to_dict()
        )
    else:
        payments_month = {}

    months = sorted(set(purchases_month.keys()) | set(payments_month.keys()))
    if not months:
        return {"trend": [], "chart_data": {}}

    trend = []
    purchases_series = []
    payments_series = []
    prev_pur = None
    prev_pay = None

    for m in months:
        p_val = clean_float(purchases_month.get(m, 0.0))
        pay_val = clean_float(payments_month.get(m, 0.0))

        purchases_series.append(p_val)
        payments_series.append(pay_val)

        pm = ((p_val - prev_pur) / prev_pur * 100) if prev_pur not in (None, 0) else None
        pym = ((pay_val - prev_pay) / prev_pay * 100) if prev_pay not in (None, 0) else None

        trend.append(
            {
                "month": m,
                "purchases": p_val,
                "payments": pay_val,
                "purchase_mom": pm,
                "payment_mom": pym,
            }
        )

        prev_pur = p_val
        prev_pay = pay_val

    chart_data = {
        "labels": months,
        "purchases": purchases_series,
        "payments": payments_series,
    }
    return {"trend": trend, "chart_data": chart_data}


def payables_aging_report(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    invoices = df[
        (df["Amount_KRW"] < 0) & (df["Outstanding_KRW"] > 0) & df["DueDate"].notna()
    ].copy()

    rows = []
    for _, r in invoices.iterrows():
        due = r["DueDate"]
        if due is None:
            continue
        days = (today - due).days
        if days < 0:
            continue

        if days <= 30:
            bucket = "0-30"
        elif days <= 60:
            bucket = "31-60"
        elif days <= 90:
            bucket = "61-90"
        else:
            bucket = "90+"

        rows.append(
            {
                "corporate": r.get("CorporateCode", ""),
                "corporate_name": r.get("CorporateName", ""),
                "vendor_code": r.get("mserp_accountnum", ""),
                "vendor_name": r.get("VendorName", ""),
                "due_date": due.isoformat(),
                "outstanding_krw": clean_float(r["Outstanding_KRW"]),
                "aging_days": days,
                "bucket": bucket,
            }
        )

    if not rows:
        return {"aging": [], "bucket_summary": {}}

    df_age = pd.DataFrame(rows)
    bucket_summary = df_age.groupby("bucket")["outstanding_krw"].sum().to_dict()
    return {"aging": rows, "bucket_summary": bucket_summary}


def payables_weekly_expected(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    week_start = today - timedelta(days=today.weekday())  # Monday
    week_end = week_start + timedelta(days=6)

    invoices = df[
        (df["Amount_KRW"] < 0)
        & (df["Outstanding_KRW"] > 0)
        & df["DueDate"].notna()
    ].copy()

    invoices = invoices[
        (invoices["DueDate"] >= week_start) & (invoices["DueDate"] <= week_end)
    ]

    if invoices.empty:
        return {"summary": {}, "details": []}

    invoices["DueDateStr"] = invoices["DueDate"].apply(lambda d: d.strftime("%Y-%m-%d"))
    summary = invoices.groupby("DueDateStr")["Outstanding_KRW"].sum().to_dict()

    details = [
        {
            "corporate": r.get("CorporateCode", ""),
            "vendor_code": r.get("mserp_accountnum", ""),
            "vendor_name": r.get("VendorName", ""),
            "due_date": r["DueDate"].isoformat(),
            "outstanding_krw": clean_float(r["Outstanding_KRW"]),
        }
        for _, r in invoices.iterrows()
    ]
    return {"summary": summary, "details": details}


def payables_monthly_outstanding(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    start = today.replace(day=1)
    if start.month == 12:
        next_month_start = date(start.year + 1, 1, 1)
    else:
        next_month_start = date(start.year, start.month + 1, 1)
    end = next_month_start - timedelta(days=1)

    invoices = df[
        (df["Amount_KRW"] < 0)
        & (df["Outstanding_KRW"] > 0)
        & df["DueDate"].notna()
    ].copy()

    invoices = invoices[
        (invoices["DueDate"] >= start) & (invoices["DueDate"] <= end)
    ]

    if invoices.empty:
        return {
            "start": str(start),
            "end": str(end),
            "total_outstanding": 0.0,
            "details": [],
        }

    total_outstanding = float(invoices["Outstanding_KRW"].sum())

    details = [
        {
            "corporate": r.get("CorporateCode", ""),
            "vendor_code": r.get("mserp_accountnum", ""),
            "vendor_name": r.get("VendorName", ""),
            "due_date": r["DueDate"].isoformat(),
            "outstanding_krw": clean_float(r["Outstanding_KRW"]),
        }
        for _, r in invoices.sort_values("DueDate").iterrows()
    ]

    return {
        "start": str(start),
        "end": str(end),
        "total_outstanding": total_outstanding,
        "details": details,
    }


# ===============================================================
# 9. RECEIVABLES ANALYTICS (AR)
# ===============================================================

def receivables_trend_last_6_months(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    six_months_ago = today - timedelta(days=180)

    df = df[df["TransactionDate"].notna()].copy()
    df = df[(df["TransactionDate"] >= six_months_ago) & (df["TransactionDate"] <= today)]

    if df.empty:
        return {"trend": [], "chart_data": {}}

    df["Month"] = df["TransactionDate"].apply(lambda d: d.strftime("%Y-%m"))

    sales = df[df["Amount_KRW"] > 0].copy()
    coll = df[df["Amount_KRW"] < 0].copy()

    sales_month = (
        sales.groupby("Month")["Amount_KRW"]
        .apply(lambda s: sum(clean_float(v) for v in s))
        .to_dict()
    )
    coll_month = (
        coll.groupby("Month")["Amount_KRW"]
        .apply(lambda s: sum(abs(clean_float(v)) for v in s))
        .to_dict()
    )

    months = sorted(set(sales_month.keys()) | set(coll_month.keys()))
    if not months:
        return {"trend": [], "chart_data": {}}

    trend = []
    sales_series = []
    coll_series = []
    prev_sales = None
    prev_coll = None

    for m in months:
        s_val = clean_float(sales_month.get(m, 0.0))
        c_val = clean_float(coll_month.get(m, 0.0))

        sales_series.append(s_val)
        coll_series.append(c_val)

        sm = ((s_val - prev_sales) / prev_sales * 100) if prev_sales not in (None, 0) else None
        cm = ((c_val - prev_coll) / prev_coll * 100) if prev_coll not in (None, 0) else None

        trend.append(
            {
                "month": m,
                "sales": s_val,
                "collections": c_val,
                "sales_mom": sm,
                "collections_mom": cm,
            }
        )

        prev_sales = s_val
        prev_coll = c_val

    chart_data = {
        "labels": months,
        "sales": sales_series,
        "collections": coll_series,
    }
    return {"trend": trend, "chart_data": chart_data}


def receivables_aging_report(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    invoices = df[
        (df["Amount_KRW"] > 0) & (df["Outstanding_KRW"] > 0) & df["DueDate"].notna()
    ].copy()

    rows = []
    for _, r in invoices.iterrows():
        due = r["DueDate"]
        if due is None:
            continue
        days = (today - due).days
        if days < 0:
            continue

        if days <= 30:
            bucket = "0-30"
        elif days <= 60:
            bucket = "31-60"
        elif days <= 90:
            bucket = "61-90"
        else:
            bucket = "90+"

        rows.append(
            {
                "corporate": r.get("CorporateCode", ""),
                "corporate_name": r.get("CorporateName", ""),
                "customer_code": r.get("mserp_accountnum", ""),
                "customer_name": r.get("CustomerName", ""),
                "due_date": due.isoformat(),
                "outstanding_krw": clean_float(r["Outstanding_KRW"]),
                "aging_days": days,
                "bucket": bucket,
            }
        )

    if not rows:
        return {"aging": [], "bucket_summary": {}}

    df_age = pd.DataFrame(rows)
    bucket_summary = df_age.groupby("bucket")["outstanding_krw"].sum().to_dict()
    return {"aging": rows, "bucket_summary": bucket_summary}


def receivables_weekly_expected(df: pd.DataFrame) -> dict:
    today = now_korea().date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)

    invoices = df[
        (df["Amount_KRW"] > 0)
        & (df["Outstanding_KRW"] > 0)
        & df["DueDate"].notna()
    ].copy()

    invoices = invoices[
        (invoices["DueDate"] >= week_start) & (invoices["DueDate"] <= week_end)
    ]

    if invoices.empty:
        return {"summary": {}, "details": []}

    invoices["DueDateStr"] = invoices["DueDate"].apply(lambda d: d.strftime("%Y-%m-%d"))
    summary = invoices.groupby("DueDateStr")["Outstanding_KRW"].sum().to_dict()

    details = [
        {
            "corporate": r.get("CorporateCode", ""),
            "customer_code": r.get("mserp_accountnum", ""),
            "customer_name": r.get("CustomerName", ""),
            "due_date": r["DueDate"].isoformat(),
            "outstanding_krw": clean_float(r["Outstanding_KRW"]),
        }
        for _, r in invoices.iterrows()
    ]
    return {"summary": summary, "details": details}

# ===============================================================
# 10. INTENT DETECTION
# ===============================================================

def is_trend_query(q: str) -> bool:
    ql = q.lower()
    keys = [
        "trend",
        "last 6 months",
        "last six months",
        "6 months",
        "six months",
        "최근 6개월",
        "추이",
        "분석해줘",
    ]
    return any(k in ql for k in keys)


def is_aging_query(q: str) -> bool:
    ql = q.lower()
    keys = [
        "vendor aging",
        "customer aging",
        "aging report",
        "ageing report",
        "aging analysis",
        "채무연령",
        "채권연령",
        "연령분석",
        "aging",
    ]
    return any(k in ql for k in keys)


def is_weekly_payments_query(q: str) -> bool:
    ql = q.lower()
    keys = [
        "this week",
        "this week's payments",
        "this week’s payments",
        "weekly expected payments",
        "expected payments for this week",
        "금주 지급예상",
        "금주 지급",
        "이번주 지급",
    ]
    return any(k in ql for k in keys)


def is_weekly_collections_query(q: str) -> bool:
    ql = q.lower()
    keys = [
        "this week's collections",
        "weekly expected collections",
        "금주 수금예상",
        "금주 수금",
        "이번주 수금",
    ]
    return any(k in ql for k in keys)


def is_monthly_outstanding_query(q: str) -> bool:
    ql = q.lower()
    keys = [
        "this month",
        "current month",
        "for this month",
        "outstanding payments",
        "monthly outstanding",
        "이번 달",
        "당월",
    ]
    return any(k in ql for k in keys)


def detect_scenario(query: str) -> str:
    q = query.lower()

    payables_words = [
        "payables",
        "vendor",
        "supplier",
        "매입",
        "지급",
        "채무",
        "매입처",
        "거래처",
    ]
    receivables_words = [
        "receivables",
        "customer",
        "매출",
        "수금",
        "채권",
        "매출처",
    ]

    if any(w in q for w in payables_words):
        return "payables"
    if any(w in q for w in receivables_words):
        return "receivables"
    return "payables"


# ===============================================================
# 11. TEXT BUILDERS
# ===============================================================

def build_ap_trend_text(trend_result: dict) -> tuple[str, str | None]:
    trend = trend_result.get("trend", [])
    chart_data = trend_result.get("chart_data", {})

    if not trend:
        return ("There is no purchasing or payment data available for the last 6 months.", None)

    lines = ["📊 Purchasing & Payment Trend (Last 6 Months, KRW):", ""]
    for item in trend:
        m = item["month"]
        p = fmt_amount(item["purchases"])
        pay = fmt_amount(item["payments"])
        pm = item["purchase_mom"]
        pym = item["payment_mom"]
        pm_str = f"{pm:+.1f}%" if pm is not None else "N/A"
        pym_str = f"{pym:+.1f}%" if pym is not None else "N/A"
        lines.append(
            f"{m} → Purchases: {p}  |  Payments: {pay}  "
            f"(Purch MoM: {pm_str}, Pay MoM: {pym_str})"
        )

    labels = chart_data.get("labels", [])
    purchases = chart_data.get("purchases", [])
    payments = chart_data.get("payments", [])

    chart_config = {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [
                {"label": "Purchases (KRW)", "data": purchases},
                {"label": "Payments (KRW)", "data": payments},
            ],
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "Purchasing & Payment Trend (Last 6 Months)",
                }
            },
            "scales": {"y": {"beginAtZero": True}},
        },
    }

    encoded = urllib.parse.quote(json.dumps(chart_config), safe="")
    chart_url = f"https://quickchart.io/chart?c={encoded}"
    return "\n".join(lines), chart_url


def build_ar_trend_text(trend_result: dict) -> tuple[str, str | None]:
    trend = trend_result.get("trend", [])
    chart_data = trend_result.get("chart_data", {})

    if not trend:
        return ("There is no sales or collection data available for the last 6 months.", None)

    lines = ["📊 Sales & Collection Trend (Last 6 Months, KRW):", ""]
    for item in trend:
        m = item["month"]
        s = fmt_amount(item["sales"])
        c = fmt_amount(item["collections"])
        sm = item["sales_mom"]
        cm = item["collections_mom"]
        sm_str = f"{sm:+.1f}%" if sm is not None else "N/A"
        cm_str = f"{cm:+.1f}%" if cm is not None else "N/A"
        lines.append(
            f"{m} → Sales: {s}  |  Collections: {c}  "
            f"(Sales MoM: {sm_str}, Collections MoM: {cm_str})"
        )

    labels = chart_data.get("labels", [])
    sales = chart_data.get("sales", [])
    colls = chart_data.get("collections", [])

    chart_config = {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [
                {"label": "Sales (KRW)", "data": sales},
                {"label": "Collections (KRW)", "data": colls},
            ],
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "Sales & Collection Trend (Last 6 Months)",
                }
            },
            "scales": {"y": {"beginAtZero": True}},
        },
    }

    encoded = urllib.parse.quote(json.dumps(chart_config), safe="")
    chart_url = f"https://quickchart.io/chart?c={encoded}"
    return "\n".join(lines), chart_url


def build_ap_aging_text(aging_result: dict) -> str:
    aging = aging_result.get("aging", [])
    buckets = aging_result.get("bucket_summary", {})
    if not aging:
        return "There are no open vendor invoices for aging analysis."

    lines = ["📊 Vendor Aging Report (Payables, KRW Outstanding by Bucket):", ""]
    for b in ["0-30", "31-60", "61-90", "90+"]:
        if b in buckets:
            lines.append(f"{b} days: {fmt_amount(buckets[b])}")
    lines.append("")
    lines.append("Sample invoices:")
    for row in aging[:15]:
        lines.append(
            f"[{row['corporate']}] {row['vendor_name']} ({row['vendor_code']}) → "
            f"Due: {row['due_date']}, Days: {row['aging_days']}, "
            f"Outstanding: {fmt_amount(row['outstanding_krw'])}, Bucket: {row['bucket']}"
        )
    return "\n".join(lines)


def build_ar_aging_text(aging_result: dict) -> str:
    aging = aging_result.get("aging", [])
    buckets = aging_result.get("bucket_summary", {})
    if not aging:
        return "There are no open customer invoices for aging analysis."

    lines = ["📊 Customer Aging Report (Receivables, KRW Outstanding by Bucket):", ""]
    for b in ["0-30", "31-60", "61-90", "90+"]:
        if b in buckets:
            lines.append(f"{b} days: {fmt_amount(buckets[b])}")
    lines.append("")
    lines.append("Sample invoices:")
    for row in aging[:15]:
        lines.append(
            f"[{row['corporate']}] {row['customer_name']} ({row['customer_code']}) → "
            f"Due: {row['due_date']}, Days: {row['aging_days']}, "
            f"Outstanding: {fmt_amount(row['outstanding_krw'])}, Bucket: {row['bucket']}"
        )
    return "\n".join(lines)


def build_weekly_payables_text(weekly_result: dict) -> str:
    summary = weekly_result.get("summary", {})
    details = weekly_result.get("details", [])
    if not summary:
        return "There are no expected vendor payments for this week."

    lines = ["📅 Expected Vendor Payments — This Week (KRW):", ""]
    for day, amt in sorted(summary.items()):
        lines.append(f"{day}: {fmt_amount(amt)}")
    lines.append("")
    lines.append("Sample details:")
    for row in details[:15]:
        lines.append(
            f"[{row['corporate']}] {row['vendor_name']} ({row['vendor_code']}) → "
            f"Due: {row['due_date']}, Outstanding: {fmt_amount(row['outstanding_krw'])}"
        )
    return "\n".join(lines)


def build_weekly_receivables_text(weekly_result: dict) -> str:
    summary = weekly_result.get("summary", {})
    details = weekly_result.get("details", [])
    if not summary:
        return "There are no expected customer collections for this week."

    lines = ["📅 Expected Customer Collections — This Week (KRW):", ""]
    for day, amt in sorted(summary.items()):
        lines.append(f"{day}: {fmt_amount(amt)}")
    lines.append("")
    lines.append("Sample details:")
    for row in details[:15]:
        lines.append(
            f"[{row['corporate']}] {row['customer_name']} ({row['customer_code']}) → "
            f"Due: {row['due_date']}, Outstanding: {fmt_amount(row['outstanding_krw'])}"
        )
    return "\n".join(lines)


def build_monthly_outstanding_text(result: dict) -> str:
    start = result.get("start")
    end = result.get("end")
    total = result.get("total_outstanding", 0.0)
    details = result.get("details", [])

    if not details:
        return (
            f"📅 Outstanding Vendor Payments — This Month ({start} → {end}):\n\n"
            "There are no outstanding vendor payments with a due date in this month."
        )

    lines = [
        f"📅 Outstanding Vendor Payments — This Month ({start} → {end}):",
        "",
        f"Total Outstanding: {fmt_amount(total)} KRW",
        "",
        "Sample invoices:",
    ]
    for row in details[:15]:
        lines.append(
            f"[{row['corporate']}] {row['vendor_name']} ({row['vendor_code']}) → "
            f"Due: {row['due_date']}, Outstanding: {fmt_amount(row['outstanding_krw'])}"
        )
    return "\n".join(lines)


# ===============================================================
# 12. FASTAPI ROUTES (LLM + TABLE OUTPUT)
# ===============================================================

@app.post("/llm/query")
def llm_query(req: QueryRequest):
    q = req.query.strip()
    scenario = detect_scenario(q)

    # -------------------- PAYABLES --------------------
    if scenario == "payables":
        df = get_payables_view()

        # 1) 6-month trend
        if is_trend_query(q):
            res = payables_trend_last_6_months(df)
            text, chart_url = build_ap_trend_text(res)

            trend_rows = res.get("trend", [])
            # Always include trend table (Option A: empty rows if no data)
            tables = {
                "trend": {
                    "type": "table",
                    "columns": [
                        "month",
                        "purchases_krw",
                        "payments_krw",
                        "purchase_mom_pct",
                        "payment_mom_pct",
                    ],
                    "rows": [
                        [
                            item.get("month"),
                            float(clean_float(item.get("purchases", 0.0))),
                            float(clean_float(item.get("payments", 0.0))),
                            item.get("purchase_mom"),
                            item.get("payment_mom"),
                        ]
                        for item in trend_rows
                    ],
                }
            }

            return build_json_response(
                scenario="payables",
                text=text,
                tables=tables,
                chart_url=chart_url,
            )

        # 2) Combined aging + weekly
        aging_flag = is_aging_query(q)
        weekly_flag = is_weekly_payments_query(q)

        if aging_flag and weekly_flag:
            aging_res = payables_aging_report(df)
            weekly_res = payables_weekly_expected(df)

            combined_text = (
                build_ap_aging_text(aging_res)
                + "\n\n"
                + build_weekly_payables_text(weekly_res)
            )

            aging_rows = aging_res.get("aging", [])
            weekly_summary = weekly_res.get("summary", {})
            weekly_details = weekly_res.get("details", [])

            tables = {
                "aging": {
                    "type": "table",
                    "columns": [
                        "corporate",
                        "corporate_name",
                        "vendor_code",
                        "vendor_name",
                        "due_date",
                        "outstanding_krw",
                        "aging_days",
                        "bucket",
                    ],
                    "rows": [
                        [
                            row.get("corporate"),
                            row.get("corporate_name"),
                            row.get("vendor_code"),
                            row.get("vendor_name"),
                            row.get("due_date"),
                            float(clean_float(row.get("outstanding_krw"))),
                            row.get("aging_days"),
                            row.get("bucket"),
                        ]
                        for row in aging_rows
                    ],
                },
                "weekly_summary": {
                    "type": "table",
                    "columns": ["date", "outstanding_krw"],
                    "rows": [
                        [day, float(clean_float(amt))]
                        for day, amt in sorted(weekly_summary.items())
                    ],
                },
                "weekly_details": {
                    "type": "table",
                    "columns": [
                        "corporate",
                        "vendor_code",
                        "vendor_name",
                        "due_date",
                        "outstanding_krw",
                    ],
                    "rows": [
                        [
                            row.get("corporate"),
                            row.get("vendor_code"),
                            row.get("vendor_name"),
                            row.get("due_date"),
                            float(clean_float(row.get("outstanding_krw"))),
                        ]
                        for row in weekly_details
                    ],
                },
            }

            return build_json_response(
                scenario="payables",
                text=combined_text,
                tables=tables,
                chart_url=None,
            )

        # 3) Monthly outstanding
        if is_monthly_outstanding_query(q):
            res = payables_monthly_outstanding(df)
            text = build_monthly_outstanding_text(res)

            details = res.get("details", [])

            tables = {
                "monthly_outstanding": {
                    "type": "table",
                    "columns": [
                        "corporate",
                        "vendor_code",
                        "vendor_name",
                        "due_date",
                        "outstanding_krw",
                    ],
                    "rows": [
                        [
                            row.get("corporate"),
                            row.get("vendor_code"),
                            row.get("vendor_name"),
                            row.get("due_date"),
                            float(clean_float(row.get("outstanding_krw"))),
                        ]
                        for row in details
                    ],
                }
            }

            return build_json_response(
                scenario="payables",
                text=text,
                tables=tables,
                chart_url=None,
            )

        # 4) Weekly expected payments only
        if is_weekly_payments_query(q):
            res = payables_weekly_expected(df)
            text = build_weekly_payables_text(res)

            summary = res.get("summary", {})
            details = res.get("details", [])

            tables = {
                "weekly_summary": {
                    "type": "table",
                    "columns": ["date", "outstanding_krw"],
                    "rows": [
                        [day, float(clean_float(amt))]
                        for day, amt in sorted(summary.items())
                    ],
                },
                "weekly_details": {
                    "type": "table",
                    "columns": [
                        "corporate",
                        "vendor_code",
                        "vendor_name",
                        "due_date",
                        "outstanding_krw",
                    ],
                    "rows": [
                        [
                            row.get("corporate"),
                            row.get("vendor_code"),
                            row.get("vendor_name"),
                            row.get("due_date"),
                            float(clean_float(row.get("outstanding_krw"))),
                        ]
                        for row in details
                    ],
                },
            }

            return build_json_response(
                scenario="payables",
                text=text,
                tables=tables,
                chart_url=None,
            )

        # 5) Aging only
        if is_aging_query(q):
            res = payables_aging_report(df)
            text = build_ap_aging_text(res)

            aging_rows = res.get("aging", [])

            tables = {
                "aging": {
                    "type": "table",
                    "columns": [
                        "corporate",
                        "corporate_name",
                        "vendor_code",
                        "vendor_name",
                        "due_date",
                        "outstanding_krw",
                        "aging_days",
                        "bucket",
                    ],
                    "rows": [
                        [
                            row.get("corporate"),
                            row.get("corporate_name"),
                            row.get("vendor_code"),
                            row.get("vendor_name"),
                            row.get("due_date"),
                            float(clean_float(row.get("outstanding_krw"))),
                            row.get("aging_days"),
                            row.get("bucket"),
                        ]
                        for row in aging_rows
                    ],
                }
            }

            return build_json_response(
                scenario="payables",
                text=text,
                tables=tables,
                chart_url=None,
            )

        # 6) Fallback
        fallback_text = (
            "Your query was recognized as payables-related.\n\n"
            "You can ask things like:\n"
            "- '최근 6개월간 매입과 지급 추이를 분석해줘'\n"
            "- '법인별 또는 거래처별 채무연령분석을 해줘'\n"
            "- '법인의 금주 지급예상액을 일별로 작성해줘'\n"
            "- '당월(이번 달) 미지급잔액을 알려줘'\n"
        )
        return build_json_response(
            scenario="payables",
            text=fallback_text,
            tables={},
            chart_url=None,
        )

    # -------------------- RECEIVABLES --------------------
    df = get_receivables_view()

    # 1) 6-month trend
    if is_trend_query(q):
        res = receivables_trend_last_6_months(df)
        text, chart_url = build_ar_trend_text(res)

        trend_rows = res.get("trend", [])

        tables = {
            "trend": {
                "type": "table",
                "columns": [
                    "month",
                    "sales_krw",
                    "collections_krw",
                    "sales_mom_pct",
                    "collections_mom_pct",
                ],
                "rows": [
                    [
                        row.get("month"),
                        float(clean_float(row.get("sales", 0.0))),
                        float(clean_float(row.get("collections", 0.0))),
                        row.get("sales_mom"),
                        row.get("collections_mom"),
                    ]
                    for row in trend_rows
                ],
            }
        }

        return build_json_response(
            scenario="receivables",
            text=text,
            tables=tables,
            chart_url=chart_url,
        )

    # 2) Weekly expected collections only
    if is_weekly_collections_query(q):
        res = receivables_weekly_expected(df)
        text = build_weekly_receivables_text(res)

        summary = res.get("summary", {})
        details = res.get("details", [])

        tables = {
            "weekly_summary": {
                "type": "table",
                "columns": ["date", "outstanding_krw"],
                "rows": [
                    [day, float(clean_float(amt))]
                    for day, amt in sorted(summary.items())
                ],
            },
            "weekly_details": {
                "type": "table",
                "columns": [
                    "corporate",
                    "customer_code",
                    "customer_name",
                    "due_date",
                    "outstanding_krw",
                ],
                "rows": [
                    [
                        row.get("corporate"),
                        row.get("customer_code"),
                        row.get("customer_name"),
                        row.get("due_date"),
                        float(clean_float(row.get("outstanding_krw"))),
                    ]
                    for row in details
                ],
            },
        }

        return build_json_response(
            scenario="receivables",
            text=text,
            tables=tables,
            chart_url=None,
        )

    # 3) Aging only (receivables)
    if is_aging_query(q):
        res = receivables_aging_report(df)
        text = build_ar_aging_text(res)

        aging_rows = res.get("aging", [])

        tables = {
            "aging": {
                "type": "table",
                "columns": [
                    "corporate",
                    "corporate_name",
                    "customer_code",
                    "customer_name",
                    "due_date",
                    "outstanding_krw",
                    "aging_days",
                    "bucket",
                ],
                "rows": [
                    [
                        row.get("corporate"),
                        row.get("corporate_name"),
                        row.get("customer_code"),
                        row.get("customer_name"),
                        row.get("due_date"),
                        float(clean_float(row.get("outstanding_krw"))),
                        row.get("aging_days"),
                        row.get("bucket"),
                    ]
                    for row in aging_rows
                ],
            }
        }

        return build_json_response(
            scenario="receivables",
            text=text,
            tables=tables,
            chart_url=None,
        )

    # 4) Fallback receivables
    fallback_text = (
        "Your query was recognized as receivables-related.\n\n"
        "You can ask things like:\n"
        "- '법인별 채권연령분석과 금주 수금예상액을 작성해줘'\n"
        "- '최근 6개월간 매출과 수금 추이를 분석해줘'\n"
    )
    return build_json_response(
        scenario="receivables",
        text=fallback_text,
        tables={},
        chart_url=None,
    )

# ===============================================================
# 13. UNPAID INVOICES API (GRID/TABLE)
# ===============================================================

@app.post("/payables/unpaid-invoices")
def unpaid_invoices_api(query: UnpaidVendTransQuery):
    df = get_payables_view()
    if df.empty:
        return safe_json({
            "page_number": query.page_number,
            "page_size": query.page_size,
            "total_count": 0,
            "records": [],
        })

    today_korea = now_korea().date()

    inv = df[(df["Amount_KRW"] < 0)].copy()
    inv = inv[inv["Outstanding_KRW"] > 0]
    inv = inv[inv["DueDate"].notna()]
    inv = inv[inv["DueDate"] <= today_korea]

    if query.due_date_from:
        inv = inv[inv["DueDate"] >= query.due_date_from]
    if query.due_date_to:
        inv = inv[inv["DueDate"] <= query.due_date_to]

    if inv.empty:
        return safe_json({
            "page_number": query.page_number,
            "page_size": query.page_size,
            "total_count": 0,
            "records": [],
        })

    inv = inv.sort_values("DueDate", ascending=False)
    total_count = len(inv)

    start_idx = (query.page_number - 1) * query.page_size
    end_idx = start_idx + query.page_size
    page_df = inv.iloc[start_idx:end_idx].copy()

    records = []
    for _, row in page_df.iterrows():
        records.append(
            {
                "corporate_code": row.get("CorporateCode"),
                "corporate_name": row.get("CorporateName"),
                "corporate_currency": row.get("CorporateCurrency"),
                "vendor_code": row.get("mserp_accountnum"),
                "vendor_name": row.get("VendorName"),
                "currency": row.get("mserp_currencycode"),
                "amount_fcy": clean_float(row.get("Amount_FCY")),
                "amount_krw": clean_float(row.get("Amount_KRW")),
                "balance_fcy": clean_float(row.get("Balance_FCY")),
                "balance_krw": clean_float(row.get("Balance_KRW")),
                "outstanding_krw": clean_float(row.get("Outstanding_KRW")),
                "transaction_date": row["TransactionDate"].isoformat()
                if row.get("TransactionDate") is not None else None,
                "due_date": row["DueDate"].isoformat()
                if row.get("DueDate") is not None else None,
                "settlement_date": row["SettlementDate"].isoformat()
                if row.get("SettlementDate") is not None else None,
            }
        )

    return safe_json({
        "page_number": query.page_number,
        "page_size": query.page_size,
        "total_count": total_count,
        "records": records,
    })
