import os
import json
from google.oauth2 import service_account
import gspread
import pandas as pd
import requests
from urllib.parse import quote_plus
from flask import Flask

app = Flask(__name__)

# Ensure the GOOGLE_SHEETS_CREDENTIALS environment variable is properly set
google_sheets_credentials = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

if google_sheets_credentials is None:
    raise ValueError("The environment variable 'GOOGLE_SHEETS_CREDENTIALS' is not set. Please set it correctly.")

# Load Google Sheets credentials from the environment variable
service_account_info = json.loads(google_sheets_credentials)

# Define the required scopes
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]

# Use the credentials and scopes
credentials = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=scopes
)

# Authorize the client
gc = gspread.authorize(credentials)

# Define your spreadsheet name
SPREADSHEET_NAME = "Find a Tender Data"

# Open the Google Sheets spreadsheet
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
                # Existing fields
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
                
                # Fixed publisher fields - now from root level
                "Publisher Name": publisher.get("name", "N/A"),
                "Publisher UID": publisher.get("uid", "N/A"),
                "Publisher Scheme": publisher.get("scheme", "N/A"),
                "Publisher URI": publisher.get("uri", "N/A"),
                "License": license_info,
                "Publication Policy": publication_policy,
                
                # Fixed release fields
                "Release Date": release.get("date", "N/A"),
                "Release Tags": ", ".join(release.get("tag", [])),
                "Release Published Date": published_date,  # Now from root level
                "Release URI": uri,  # Now from root level
                "Extensions": ", ".join(extensions),  # Now from root level
                "Documents": ", ".join(document_urls) if document_urls else "N/A",
                
                # New fields
                "Initiation Type": release.get("initiationType", "N/A"),
                
                # Buyer detailed information
                "Buyer Address Street": buyer_party.get("address", {}).get("streetAddress", "N/A"),
                "Buyer Address Locality": buyer_party.get("address", {}).get("locality", "N/A"),
                "Buyer Address Postal Code": buyer_party.get("address", {}).get("postalCode", "N/A"),
                "Buyer Address Country": buyer_party.get("address", {}).get("country", "N/A"),
                "Buyer Address Country Name": buyer_party.get("address", {}).get("countryName", "N/A"),
                "Buyer Address Region": buyer_party.get("address", {}).get("region", "N/A"),
                "Buyer Contact Name": buyer_contact_name,
                "Buyer Contact Email": buyer_contact_email,
                "Buyer Roles": ", ".join(buyer_party.get("roles", [])),
                
                # Buyer classifications
                "Buyer Classification Scheme": next((c.get("scheme", "N/A") for c in buyer_party.get("details", {}).get("classifications", [])), "N/A"),
                "Buyer Classification ID": next((c.get("id", "N/A") for c in buyer_party.get("details", {}).get("classifications", [])), "N/A"),
                "Buyer Classification Description": next((c.get("description", "N/A") for c in buyer_party.get("details", {}).get("classifications", [])), "N/A"),
                
                # Legal basis
                "Legal Basis ID": legal_basis_id,
                "Legal Basis Scheme": legal_basis_scheme,
                "Legal Basis URI": legal_basis_uri,
                
                # Additional tender information
                "Tender Above Threshold": release.get("tender", {}).get("aboveThreshold", "N/A"),
                
                # Other requirements
                "Reserved Participation Location Identifiers": ", ".join(
                    [identifier for identifier in release.get("tender", {})
                    .get("otherRequirements", {})
                    .get("reservedParticipationLocation", {})
                    .get("gazetteer", {})
                    .get("identifiers", [])]),
                
                # Document details (first 3 documents)
                "Document 1 Type": document_types[0] if len(document_types) > 0 else "N/A",
                "Document 1 Description": document_descriptions[0] if len(document_descriptions) > 0 else "N/A",
                "Document 1 URL": document_urls[0] if len(document_urls) > 0 else "N/A",
                "Document 1 Format": document_formats[0] if len(document_formats) > 0 else "N/A",
                
                "Document 2 Type": document_types[1] if len(document_types) > 1 else "N/A",
                "Document 2 Description": document_descriptions[1] if len(document_descriptions) > 1 else "N/A",
                "Document 2 URL": document_urls[1] if len(document_urls) > 1 else "N/A",
                "Document 2 Format": document_formats[1] if len(document_formats) > 1 else "N/A",
                
                "Document 3 Type": document_types[2] if len(document_types) > 2 else "N/A",
                "Document 3 Description": document_descriptions[2] if len(document_descriptions) > 2 else "N/A",
                "Document 3 URL": document_urls[2] if len(document_urls) > 2 else "N/A",
                "Document 3 Format": document_formats[2] if len(document_formats) > 2 else "N/A",
                
                # Item information
                "Item IDs": ", ".join(item_ids),
                "Item Classifications": ", ".join(item_classifications),
                
                # Gross value amounts (if available)
                "Tender Value Amount Gross": release.get("tender", {}).get("value", {}).get("amountGross", "N/A"),
                "Tender Lot Value Amount Gross": release.get("tender", {}).get("lots", [{}])[0].get("value", {}).get("amountGross", "N/A"),
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

# Clean data - replace None, empty lists, and other problematic values
def clean_value(val):
    if val is None:
        return ""
    if isinstance(val, (list, dict)):
        if not val:  # Empty list or dict
            return ""
        return str(val)
    return val

# Apply cleaning to all DataFrame cells
for col in df.columns:
    df[col] = df[col].apply(clean_value)

# For debugging (optional)
if len(df) >= 4 and len(df.columns) >= 22:
    print(f"Value at [4][22]: {df.iloc[3, 21]}")
    print(f"Type: {type(df.iloc[3, 21])}")

# Clear existing data and update the sheet
results_sheet.clear()
results_sheet.update([df.columns.values.tolist()] + df.values.tolist())

print("Data successfully written to Google Sheets!")