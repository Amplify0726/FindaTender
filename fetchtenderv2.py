import gspread
import pandas as pd
import requests
import json
from urllib.parse import quote_plus

# Load Google Sheets credentials
SERVICE_ACCOUNT_FILE = "find-a-tender-script-75ef0d877e25.json"  # Replace with your JSON filename
SPREADSHEET_NAME = "Find a Tender Data"  # Replace with your actual sheet name

# Authenticate with Google Sheets
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
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
                "OCID": ocid,
                "Title": release.get("tender", {}).get("title", "N/A"),
                "Value": release.get("tender", {}).get("value", {}).get("amount", "N/A"),
                "Currency": release.get("tender", {}).get("value", {}).get("currency", "N/A"),
                "Published Date": release.get("date", "N/A"),
                "Description": release.get("tender", {}).get("description", "N/A"),
                "Contract Type": release.get("tender", {}).get("contractType", "N/A"),
                "Award Criteria": release.get("tender", {}).get("awardCriteria", "N/A"),
                "Parties": ", ".join([party.get("name", "N/A") for party in release.get("parties", [])]),
                # Ensure that 'contactPoint' is valid and exists
                "Contact Point": ", ".join([contact.get("email", "N/A") for party in release.get("parties", []) for contact in party.get("contactPoint", []) if isinstance(contact, dict)]),
                "Location": release.get("tender", {}).get("location", {}).get("address", {}).get("locality", "N/A"),
                "Tender ID": release.get("tender", {}).get("id", "N/A"),
                "Start Date": release.get("tender", {}).get("startDate", "N/A"),
                "End Date": release.get("tender", {}).get("endDate", "N/A"),
                "Deadline Date": release.get("tender", {}).get("submissionMethod", "N/A"),
                "Document Link": release.get("uri", "N/A"),
                "CPV": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A")
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
