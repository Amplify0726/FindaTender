import os
import json
import logging
from typing import Tuple, Dict, Any
from google.oauth2 import service_account
import gspread
import pandas as pd
import requests
from urllib.parse import quote_plus
from flask import Flask, jsonify
import time
from threading import Thread
from notice_types import UK3Notice, UK4Notice

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

def validate_ocds_response(data: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate OCDS version and extensions"""
    expected_version = "1.1"
    required_extensions = [
        "https://standard.open-contracting.org/profiles/eu/latest/en/extension.json",
        "https://raw.githubusercontent.com/open-contracting-extensions/ocds_suitability_extension/master/extension.json",
        "https://raw.githubusercontent.com/cabinetoffice/ocds_uk_extension/main/extension.json"
    ]
    
    # Check OCDS version
    version = data.get('version', '')
    if not version.startswith(expected_version):
        return False, f"Unexpected OCDS version: {version}, expected {expected_version}"
    
    # Check required extensions
    extensions = set(data.get('extensions', []))
    missing_extensions = [ext for ext in required_extensions if ext not in extensions]
    if missing_extensions:
        return False, f"Missing required extensions: {', '.join(missing_extensions)}"
        
    return True, "Valid OCDS response"

def fetch_and_process_data():
    global job_running, last_run_time
    
    job_running = True
    
    try:
        # Open the Google Sheets spreadsheet
        sh = gc.open(SPREADSHEET_NAME)

        # Dictionary to store results by notice type
        results_by_type = {}

        # Load OCIDs from the "OCIDs" sheet
        ocid_sheet = sh.worksheet("OCIDs")
        ocid_list = ocid_sheet.col_values(1)  # Reads all OCIDs from column A
        ocid_list = [ocid for ocid in ocid_list if ocid.strip()]  # Remove empty values

        # Define API URL
        API_BASE_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages/"

        for ocid in ocid_list:
            # Ensure OCID is URL-encoded
            encoded_ocid = quote_plus(ocid.strip())
            response = requests.get(API_BASE_URL + encoded_ocid)
            
            if response.status_code == 200:
                data = response.json()
                
                # Validate OCDS response
                is_valid, message = validate_ocds_response(data)
                if not is_valid:
                    print(f"Warning for {ocid}: {message}")
                
                try:
                    for release in data.get('releases', []):
                        notice_type = None
                        for doc in release.get('planning', {}).get('documents', []):
                            notice_type = doc.get('noticeType')
                            if notice_type:
                                break
                        
                        if notice_type:
                            # Initialize list for this notice type if doesn't exist
                            if notice_type not in results_by_type:
                                results_by_type[notice_type] = []
                            
                            # Process based on notice type
                            if notice_type == 'UK3':
                                notice = UK3Notice.from_api_data(release)
                                validation_errors = notice.validate()
                                
                                if validation_errors:
                                    print(f"Warning - Validation issues for {notice.ocid}:")
                                    for error in validation_errors:
                                        print(f"  - {error}")
                                
                                tender_info = {
                                    # Summary section
                                    "Notice Type": "UK3",
                                    "Notice Identifier": notice.notice_identifier,
                                    "Procurement Identifier": notice.procurement_identifier,
                                    "Published Date": notice.published_date,
                                    "Last Edited Date": notice.last_edited_date,

                                    # Scope section
                                    "Commercial Tool": notice.commercial_tool,
                                    "Total Value (excl VAT)": notice.total_value_amount,
                                    "Total Value (incl VAT)": notice.total_value_amount_gross,
                                    "Currency": notice.total_value_currency,
                                    "Contract Start Date": notice.contract_dates['start'],
                                    "Contract End Date": notice.contract_dates['end'],
                                    "Contract Duration": notice.contract_dates['duration'],
                                    "Procurement Category": notice.procurement_category,
                                    "CPV Codes": ", ".join(f"{code['id']} - {code['description']}" for code in notice.cpv_codes),
                                    "Lot Constraints": notice.lot_constraints,

                                    # Lots are handled in a separate worksheet

                                    # Framework section
                                    "Framework End Date": notice.framework_end_date,
                                    "Maximum Participants": notice.framework_max_participants,
                                    "Framework Description": notice.framework_description,
                                    "Award Method": notice.framework_award_method,
                                    "Framework Buyers": "\n".join(notice.framework_buyers),

                                    # Rest of sections...
                                    "Validation Warnings": "\n".join(validation_errors) if validation_errors else "None",
                                    "Custom Fields": json.dumps(notice.custom_fields) if notice.custom_fields else '',
                                    "Unused OCDS Fields": ', '.join(notice.unused_fields) if notice.unused_fields else ''
                                }
                                
                                # Create separate worksheet for lots detail
                                lots_data = [{
                                    "Lot Number": idx + 1,
                                    "Lot Title": lot.title,
                                    "Lot Description": lot.description,
                                    "Value (excl VAT)": lot.value_amount,
                                    "Value (incl VAT)": lot.value_amount_gross,
                                    "Currency": lot.value_currency,
                                    "SME Suitable": lot.sme_suitable,
                                    "VCSE Suitable": lot.vcse_suitable,
                                    # Award criteria for each lot...
                                } for idx, lot in enumerate(notice.lots)]

                                # Add lots worksheet
                                if lots_data:
                                    lots_df = pd.DataFrame(lots_data)
                                    try:
                                        lots_worksheet = sh.worksheet(f"{notice_type}_Lots") 
                                    except gspread.exceptions.WorksheetNotFound:
                                        lots_worksheet = sh.add_worksheet(title=f"{notice_type}_Lots", rows="1000", cols="50")
                                    lots_worksheet.clear()
                                    lots_worksheet.update([lots_df.columns.values.tolist()] + lots_df.values.tolist())

                                results_by_type['UK3'].append(tender_info)
                            elif notice_type == 'UK4':
                                notice = UK4Notice.from_api_data(release)
                                validation_errors = notice.validate()
                                
                                if validation_errors:
                                    print(f"Warning - Validation issues for {notice.ocid}:")
                                    for error in validation_errors:
                                        print(f"  - {error}")
                                
                                tender_info = {
                                    "Notice Type": "UK4",
                                    "Notice Identifier": notice.notice_identifier,
                                    "Procurement Identifier": notice.procurement_identifier,
                                    "Published Date": notice.published_date,
                                    "Last Edited Date": notice.last_edited_date,
                                    "Tender Title": notice.tender_title,
                                    "Tender Description": notice.tender_description,
                                    "Tender Status": notice.tender_status,
                                    "Tender Value Amount": notice.tender_value_amount,
                                    "Tender Value Currency": notice.tender_value_currency,
                                    "Procurement Method": notice.procurement_method,
                                    "Procurement Category": notice.procurement_category,
                                    "CPV Codes": ", ".join(f"{code['id']} - {code['description']}" for code in notice.cpv_codes),
                                    "Tender Period End": notice.tender_period_end,
                                    "Enquiry Period End": notice.enquiry_period_end,
                                    "Submission Method": notice.submission_method,
                                    "Award Criteria": "\n".join(f"{c['name']}: {c['weight']}% ({c['type']})" for c in notice.award_criteria),
                                    "Buyer Name": notice.buyer_name,
                                    "Buyer ID": notice.buyer_id,
                                    "Validation Warnings": "\n".join(validation_errors) if validation_errors else "None",
                                    "Custom Fields": json.dumps(notice.custom_fields) if notice.custom_fields else '',
                                    "Unused OCDS Fields": ', '.join(notice.unused_fields) if notice.unused_fields else ''
                                }
                                
                                results_by_type['UK4'].append(tender_info)
                            # Add other notice types here as they are implemented
                            
                except (KeyError, IndexError) as e:
                    print(f"Error extracting data for OCID: {ocid} - {e}")

        # Write each notice type to its own worksheet
        for notice_type, results in results_by_type.items():
            # Create or get worksheet for this notice type
            try:
                worksheet = sh.worksheet(notice_type)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=notice_type, rows="1000", cols="100")

            if results:  # Only process if we have data
                df = pd.DataFrame(results)
                
                # Clean data
                def clean_value(val):
                    if val is None:
                        return ""
                    if isinstance(val, (list, dict)):
                        if not val:  # Empty list or dict
                            return ""
                        return str(val)
                    return val

                for col in df.columns:
                    df[col] = df[col].apply(clean_value)

                # Update sheet
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
        job_running = False

# Route for manual triggering of the data fetch
@app.route('/run')
def run_job():
    global job_running, last_run_time
    
    if job_running:
        return jsonify({
            "status": "in_progress",
            "message": "A job is already running, please try again later."
        })
    
    # Run in a separate thread to not block the response
    thread = Thread(target=fetch_and_process_data)
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Data fetch job has been started. Check logs for results.",
        "last_completed_run": last_run_time
    })

# Health check endpoint
@app.route('/')
def health_check():
    global last_run_time
    return jsonify({
        "status": "healthy",
        "service": "find-a-tender-data-fetcher",
        "job_running": job_running,
        "last_run": last_run_time
    })

if __name__ == '__main__':
    # Get port from environment variable or use default 5000
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)