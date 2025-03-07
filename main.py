import gspread
import pandas as pd
import requests
import json
from urllib.parse import quote_plus
import os
from google.oauth2 import service_account

# Load the credentials from the environment variable
service_account_info = json.loads(os.getenv('GOOGLE_SHEETS_CREDENTIALS'))

# Use the credentials
credentials = service_account.Credentials.from_service_account_info(service_account_info)
gc = gspread.authorize(credentials)

# Spreadsheet and OCID details
SPREADSHEET_NAME = "Find a Tender Data"  # Replace with your actual sheet name
SERVICE_ACCOUNT_FILE = "find-a-tender-script-75ef0d877e25.json"  # Keep this as is

# Authenticate with Google Sheets
sh = gc.open(SPREADSHEET_NAME)

# Load OCIDs from the "OCIDs" sheet
ocid_sheet = sh.worksheet("OCIDs")
ocid_list = ocid_sheet.col_values(1)  # Reads all OCIDs from column A
ocid_list = [ocid for ocid in ocid_list if ocid.strip()]  # Remove empty values

# Define API URL
API_BASE_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages/"

# Store results
results = []

for ocid in ocid_list:
    # Ensure OCID is URL-encoded
    encoded_ocid = quote_plus(ocid.strip())
    response = requests.get(API_BASE_URL + encoded_ocid)
    
    if response.status_code == 200:
        data = response.json()
        try:
            release = data["releases"][0]  # Assuming we are always working with the first release
            
            tender_info = {
                "OCID": release.get("ocid", "N/A"),
                "ID": release.get("id", "N/A"),
                "Tender ID": release.get("tender", {}).get("id", "N/A"),
                "Tender Title": release.get("tender", {}).get("title", "N/A"),
                "Tender Description": release.get("tender", {}).get("description", "N/A"),
                "Tender Status": release.get("tender", {}).get("status", "N/A"),
                "Tender Value Amount": release.get("tender", {}).get("value", {}).get("amount", "N/A"),
                "Tender Value Currency": release.get("tender", {}).get("value", {}).get("currency", "N/A"),
                "Procurement Method": release.get("tender", {}).get("procurementMethod", "
