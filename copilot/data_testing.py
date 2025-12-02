from msal import ConfidentialClientApplication
import pandas as pd
import requests
import os, json
from dotenv import load_dotenv
 
load_dotenv()
 
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
TABLE_NAME = os.getenv("TABLE_COMPANY")
 
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = [f"{DATAVERSE_URL}/.default"]
 
app = ConfidentialClientApplication(
    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
)
 
token = app.acquire_token_for_client(scopes=SCOPE)
access_token = token["access_token"]
 
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json"
}
 
# ---------------------------------------
# 🔥 Pagination Logic
# ---------------------------------------
url = f"{DATAVERSE_URL}/api/data/v9.2/{TABLE_NAME}"
all_rows = []
 
while url:
    res = requests.get(url, headers=headers)
    data = res.json()
 
    # Add current page rows
    all_rows.extend(data.get("value", []))
 
    # Check for next page
    url = data.get("@odata.nextLink", None)
 
# Convert to DataFrame
df = pd.DataFrame(all_rows)
 
print("Total rows fetched:", len(df))
# print(df.head())
# print(df.columns.tolist())
# print(df['mserp_transdate'])
df.to_csv('ledger.csv')

vend_trans=pd.read_csv(r"vend_trans.csv")
ledger=pd.read_csv(r"ledger.csv")
# print(vend_trans.head())
# print(ledger.head())

merge=pd.merge(vend_trans,ledger,how="inner",left_on="mserp_dataareaid",right_on="mserp_name")
print(len(merge))
