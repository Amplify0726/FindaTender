import os
import json
from google.oauth2 import service_account
import gspread
import pandas as pd
import requests
from urllib.parse import quote_plus
from flask import Flask, jsonify
import time
from threading import Thread

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

# Flag to track if a job is currently running
job_running = False
last_run_time = None


def fetch_and_process_data():
    global job_running, last_run_time
    
    # Set flag to indicate job is running
    job_running = True
    
    try:
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
                    # Extract root-level data first
                    publisher = data.get("publisher", {})
                    license_info = data.get("license", "N/A")
                    publication_policy = data.get("publicationPolicy", "N/A")
                    published_date = data.get("publishedDate", "N/A")
                    uri = data.get("uri", "N/A")
                    extensions = data.get("extensions", [])
                    
                    # Initialize a dictionary to store the latest information
                    tender_info = {}
                    
                    for release in data.get("releases", []):
                        # First determine the notice type from all possible locations
                        notice_type = "N/A"
                        notice_url = "N/A"
                        notice_date = "N/A"
                        
                        # Check planning documents (UK1-UK3)
                        planning_docs = release.get("planning", {}).get("documents", [])
                        if planning_docs:
                            last_doc = planning_docs[-1]
                            if last_doc.get("noticeType", "").startswith("UK"):
                                notice_type = last_doc.get("noticeType")
                                notice_url = last_doc.get("url", "N/A")
                                notice_date = last_doc.get("datePublished", "N/A")
                                
                        # Check tender documents (UK4, UK12, UK13)
                        if notice_type == "N/A":
                            tender_docs = release.get("tender", {}).get("documents", [])
                            if tender_docs:
                                last_doc = tender_docs[-1]
                                if last_doc.get("noticeType", "").startswith("UK"):
                                    notice_type = last_doc.get("noticeType")
                                    notice_url = last_doc.get("url", "N/A")
                                    notice_date = last_doc.get("datePublished", "N/A")
                        
                        # Check award documents (UK5-UK7)
                        if notice_type == "N/A":
                            awards = release.get("awards", [])
                            if awards:
                                award_docs = awards[-1].get("documents", [])
                                if award_docs:
                                    last_doc = award_docs[-1]
                                    if last_doc.get("noticeType", "").startswith("UK"):
                                        notice_type = last_doc.get("noticeType")
                                        notice_url = last_doc.get("url", "N/A")
                                        notice_date = last_doc.get("datePublished", "N/A")
                        
                        # Then check if this is an update
                        is_update = any("update" in tag.lower() for tag in release.get("tag", []))
                        
                        # Extract fields based on notice type
                        if notice_type.startswith(("UK1", "UK2", "UK3")):
                            # Planning phase fields
                            current_info = extract_planning_fields(release, notice_type, notice_url, notice_date, is_update)
                        elif notice_type.startswith(("UK4", "UK12", "UK13")):
                            # Tender phase fields
                            current_info = extract_tender_fields(release, notice_type, notice_url, notice_date, is_update)
                        elif notice_type.startswith(("UK5", "UK6", "UK7")):
                            # Award phase fields
                            current_info = extract_award_fields(release, notice_type, notice_url, notice_date, is_update)
                        else:
                            # Default/unknown notice type - extract common fields
                            current_info = extract_common_fields(release, notice_type, notice_url, notice_date, is_update)
                        
                        # Add common fields that should be included regardless of notice type
                        current_info.update({
                            "OCID": release.get("ocid", "N/A"),
                            "ID": release.get("id", "N/A"),
                            "Is Update": is_update,
                            "Notice Type": notice_type,
                            "Notice URL": notice_url,
                            "Notice Date": notice_date,
                            # Add other common fields...
                        })
                        
                        # Update the tender_info dictionary
                        if not tender_info:
                            tender_info = current_info
                        else:
                            # Merge current_info into tender_info, keeping track of what's changed
                            changed_fields = []
                            for key, value in current_info.items():
                                if key in tender_info and tender_info[key] != value and value != "N/A":
                                    tender_info[key] = value
                                    changed_fields.append(key)
                            
                            if is_update and changed_fields:
                                tender_info["Fields Changed"] = ", ".join(changed_fields)
                    
                    results.append(tender_info)
                    
                except (KeyError, IndexError) as e:
                    print(f"Error extracting data for OCID: {ocid} - {e}")
            elif response.status_code == 404:
                print(f"OCID {ocid} not found. Skipping...")
            else:
                print(f"Error fetching OCID {ocid}, Status Code: {response.status_code}")

        # Group results by notice type
        notice_type_results = {}
        for result in results:
            notice_type = result.get("Notice Type", "Unknown")
            if notice_type not in notice_type_results:
                notice_type_results[notice_type] = []
            notice_type_results[notice_type].append(result)

        # Process each notice type group
        for notice_type, type_results in notice_type_results.items():
            # Create worksheet name (e.g., "UK4_Results", "UK5_Results", etc.)
            worksheet_name = f"{notice_type}_Results"
            
            try:
                # Try to get existing worksheet
                worksheet = sh.worksheet(worksheet_name)
            except gspread.WorksheetNotFound:
                # Create new worksheet if it doesn't exist
                worksheet = sh.add_worksheet(title=worksheet_name, rows=1, cols=1)
            
            # Convert results to DataFrame
            df = pd.DataFrame(type_results)

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

            # Clear existing data and update the worksheet
            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())

        last_run_time = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Data successfully written to Google Sheets at {last_run_time}!")
        
        return True, f"Data successfully processed at {last_run_time}"
    except Exception as e:
        error_message = f"Error processing data: {str(e)}"
        print(error_message)
        return False, error_message
    finally:
        # Reset flag when job is done
        job_running = False

def extract_planning_fields(release, notice_type, notice_url, notice_date, is_update):
    # Extract fields relevant to planning phase (UK1-UK3)
    return {
        "Planning Description": release.get("planning", {}).get("description", "N/A"),
        # Add other planning-specific fields...
    }

def extract_tender_fields(release, notice_type, notice_url, notice_date, is_update):
    # Extract fields relevant to tender phase (UK4, UK12, UK13)
    tender = release.get("tender", {})
    lots = tender.get("lots", [{}])[0]  # Get first lot info
    buyer = release.get("buyer", {})
    buyer_party = next((party for party in release.get("parties", []) 
                       if party.get("id") == buyer.get("id")), {})
    
    return {
        # Basic tender information
        "Tender ID": tender.get("id", "N/A"),
        "Tender Title": tender.get("title", "N/A"),
        "Tender Description": tender.get("description", "N/A"),
        "Tender Status": tender.get("status", "N/A"),
        
        # Procurement details
        "Procurement Method": tender.get("procurementMethod", "N/A"),
        "Procurement Method Details": tender.get("procurementMethodDetails", "N/A"),
        "Main Procurement Category": tender.get("mainProcurementCategory", "N/A"),
        "Above Threshold": tender.get("aboveThreshold", "N/A"),
        
        # Value information
        "Tender Value Amount": tender.get("value", {}).get("amount", "N/A"),
        "Tender Value Currency": tender.get("value", {}).get("currency", "N/A"),
        "Tender Value Amount Gross": tender.get("value", {}).get("amountGross", "N/A"),
        
        # Dates and periods
        "Tender Period Start Date": tender.get("tenderPeriod", {}).get("startDate", "N/A"),
        "Tender Period End Date": tender.get("tenderPeriod", {}).get("endDate", "N/A"),
        "Enquiry Period End Date": tender.get("enquiryPeriod", {}).get("endDate", "N/A"),
        
        # Submission information
        "Tender Submission Method": tender.get("submissionMethodDetails", "N/A"),
        "Tender Submission Terms": tender.get("submissionTerms", {}).get("electronicSubmissionPolicy", "N/A"),
        
        # Lot information
        "Tender Award Criteria": lots.get("awardCriteria", {}).get("description", "N/A"),
        "Tender Lot Value Amount": lots.get("value", {}).get("amount", "N/A"),
        "Tender Lot Value Currency": lots.get("value", {}).get("currency", "N/A"),
        "Tender Lot Value Amount Gross": lots.get("value", {}).get("amountGross", "N/A"),
        "Tender Lot Contract Period Start Date": lots.get("contractPeriod", {}).get("startDate", "N/A"),
        "Tender Lot Contract Period End Date": lots.get("contractPeriod", {}).get("endDate", "N/A"),
        "Tender Lot Suitability SME": lots.get("suitability", {}).get("sme", "N/A"),
        
        # Legal information
        "Legal Basis ID": tender.get("legalBasis", {}).get("id", "N/A"),
        "Legal Basis Scheme": tender.get("legalBasis", {}).get("scheme", "N/A"),
        "Legal Basis URI": tender.get("legalBasis", {}).get("uri", "N/A"),
        
        # Buyer information
        "Buyer Name": buyer.get("name", "N/A"),
        "Buyer ID": buyer.get("id", "N/A"),
        "Buyer Contact Name": buyer_party.get("contactPoint", {}).get("name", "N/A"),
        "Buyer Contact Email": buyer_party.get("contactPoint", {}).get("email", "N/A"),
        "Buyer Address Street": buyer_party.get("address", {}).get("streetAddress", "N/A"),
        "Buyer Address Locality": buyer_party.get("address", {}).get("locality", "N/A"),
        "Buyer Address Postal Code": buyer_party.get("address", {}).get("postalCode", "N/A"),
        "Buyer Address Country": buyer_party.get("address", {}).get("country", "N/A"),
        "Buyer Address Country Name": buyer_party.get("address", {}).get("countryName", "N/A"),
        "Buyer Address Region": buyer_party.get("address", {}).get("region", "N/A"),
        
        # Amendment information
        "Amendments": ", ".join(amend.get("description", "N/A") 
                              for amend in tender.get("amendments", [])) or "N/A",
        
        # Document information
        "Documents": ", ".join(doc.get("url", "N/A") 
                             for doc in tender.get("documents", [])) or "N/A",
    }

def extract_award_fields(release, notice_type, notice_url, notice_date, is_update):
    # Extract fields relevant to award phase (UK5-UK7)
    awards = release.get("awards", [])
    if awards:
        award = awards[-1]  # Get the latest award
        return {
            "Award Title": award.get("title", "N/A"),
            "Award Description": award.get("description", "N/A"),
            # Add other award-specific fields...
        }
    return {}

def extract_common_fields(release, notice_type, notice_url, notice_date, is_update):
    # Extract fields common to all notice types
    return {
        "Release Date": release.get("date", "N/A"),
        "Release Tags": ", ".join(release.get("tag", [])),
        # Add other common fields...
    }