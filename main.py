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
                "Procurement Method": release.get("tender", {}).get("procurementMethod", "N/A"),
                "Procurement Method Details": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                "Main Procurement Category": release.get("tender", {}).get("mainProcurementCategory", "N/A"),
                "Tender Period End Date": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                "Tender Period Start Date": release.get("tender", {}).get("tenderPeriod", {}).get("startDate", "N/A"),
                "Enquiry Period End Date": release.get("tender", {}).get("enquiryPeriod", {}).get("endDate", "N/A"),
                "Tender Submission Method": release.get("tender", {}).get("submissionMethodDetails", "N/A"),
                "Tender Submission Terms": release.get("tender", {}).get("submissionTerms", {}).get("electronicSubmissionPolicy", "N/A"),
                "Tender Award Criteria": release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("description", "N/A"),
                "Tender Lot Value Amount": release.get("tender", {}).get("lots", [{}])[0].get("value", {}).get("amount", "N/A"),
                "Tender Lot Value Currency": release.get("tender", {}).get("lots", [{}])[0].get("value", {}).get("currency", "N/A"),
                "Tender Lot Contract Period Start Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("startDate", "N/A"),
                "Tender Lot Contract Period End Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("endDate", "N/A"),
                "Tender Lot Suitability SME": release.get("tender", {}).get("lots", [{}])[0].get("suitability", {}).get("sme", "N/A"),
                "Buyer Name": release.get("buyer", {}).get("name", "N/A"),
                "Buyer ID": release.get("buyer", {}).get("id", "N/A"),
                "Publisher Name": release.get("publisher", {}).get("name", "N/A"),
                "Publisher UID": release.get("publisher", {}).get("uid", "N/A"),
                "Publisher Scheme": release.get("publisher", {}).get("scheme", "N/A"),
                "Publisher URI": release.get("publisher", {}).get("uri", "N/A"),
                "License": release.get("license", "N/A"),
                "Publication Policy": release.get("publicationPolicy", "N/A"),
                "Release Date": release.get("date", "N/A"),
                "Release Tags": ", ".join(release.get("tag", [])),
                "Release Published Date": release.get("publishedDate", "N/A"),
                "Release URI": release.get("uri", "N/A"),
                "Extensions": ", ".join(release.get("extensions", [])),
                "Documents": ", ".join([doc.get("url", "N/A") for doc in release.get("documents", [])]),
            }
            results.append(tender_info)
        except (KeyError, IndexError) as e:
            print(f"Error extracting data for OCID: {ocid} - {e}")
    elif response.status_code == 404:
        print(f"OCID {ocid} not found. Skipping...")
    else:
        print(f"Error fetching OCID {ocid}, Status Code: {response.status_code}")

# Write data back to Google Sheets
results_sheet = sh.worksheet("Results")

# Convert results to a DataFrame
df = pd.DataFrame(results)

# Clear existing data and update the sheet
results_sheet.clear()
results_sheet.update([df.columns.values.tolist()] + df.values.tolist())

print("Data successfully written to Google Sheets!")
