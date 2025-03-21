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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        logger.info(f"Successfully opened spreadsheet: {SPREADSHEET_NAME}")

        # Dictionary to store results by notice type
        results_by_type = {
            'UK3': [],
            'UK4': []
        }

        # Load OCIDs from the "OCIDs" sheet
        try:
            ocid_sheet = sh.worksheet("OCIDs")
            ocid_list = ocid_sheet.col_values(1)  # Reads all OCIDs from column A
            ocid_list = [ocid for ocid in ocid_list if ocid.strip()]  # Remove empty values
            logger.info(f"Loaded {len(ocid_list)} OCIDs from sheet")
        except gspread.exceptions.WorksheetNotFound:
            logger.error("OCIDs worksheet not found")
            return False, "OCIDs worksheet not found"

        # Define API URL
        API_BASE_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages/"

        for ocid in ocid_list:
            # Ensure OCID is URL-encoded
            encoded_ocid = quote_plus(ocid.strip())
            logger.info(f"Fetching data for OCID: {ocid}")
            
            try:
                response = requests.get(API_BASE_URL + encoded_ocid)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Validate OCDS response
                    is_valid, message = validate_ocds_response(data)
                    if not is_valid:
                        logger.warning(f"Warning for {ocid}: {message}")
                    
                    for release in data.get('releases', []):
                        notice_type = None
                        for doc in release.get('planning', {}).get('documents', []):
                            notice_type = doc.get('noticeType')
                            if notice_type:
                                break
                        
                        if notice_type:
                            logger.info(f"Processing {notice_type} notice for {ocid}")
                            
                            # Process based on notice type
                            if notice_type == 'UK3':
                                try:
                                    notice = UK3Notice(
                                        notice_identifier=release.get('id', ''),
                                        procurement_identifier=release.get('ocid', ''),
                                        published_date=release.get('date', ''),
                                        commercial_tool=release.get('tender', {}).get('commercialTool', ''),
                                        total_value_amount=release.get('tender', {}).get('value', {}).get('amount', 0),
                                        total_value_amount_gross=release.get('tender', {}).get('value', {}).get('amountGross', 0),
                                        total_value_currency=release.get('tender', {}).get('value', {}).get('currency', 'GBP'),
                                        contract_dates=release.get('tender', {}).get('contractDates', {'start': '', 'end': '', 'duration': ''}),
                                        procurement_category=release.get('tender', {}).get('mainProcurementCategory', ''),
                                        cpv_codes=release.get('tender', {}).get('cpvCodes', []),
                                        lots=release.get('tender', {}).get('lots', []),
                                        framework_end_date=release.get('tender', {}).get('frameworkEndDate', ''),
                                        framework_max_participants=release.get('tender', {}).get('frameworkMaxParticipants', 0),
                                        framework_description=release.get('tender', {}).get('frameworkDescription', ''),
                                        framework_award_method=release.get('tender', {}).get('frameworkAwardMethod', ''),
                                        framework_buyers=release.get('tender', {}).get('frameworkBuyers', []),
                                        sme_suitable=release.get('tender', {}).get('smeSuitable', False),
                                        vcse_suitable=release.get('tender', {}).get('vcseSuitable', False),
                                        publication_date=release.get('tender', {}).get('publicationDate', ''),
                                        tender_deadline=release.get('tender', {}).get('tenderDeadline', ''),
                                        electronic_submission=release.get('tender', {}).get('electronicSubmission', False),
                                        submission_languages=release.get('tender', {}).get('submissionLanguages', []),
                                        award_date=release.get('tender', {}).get('awardDate', ''),
                                        award_criteria=release.get('tender', {}).get('awardCriteria', []),
                                        trade_agreements=release.get('tender', {}).get('tradeAgreements', []),
                                        procedure_type=release.get('tender', {}).get('procedureType', ''),
                                        procedure_description=release.get('tender', {}).get('procedureDescription', ''),
                                        buyer_name=release.get('buyer', {}).get('name', ''),
                                        buyer_id=release.get('buyer', {}).get('id', ''),
                                        buyer_address=release.get('buyer', {}).get('address', {}),
                                        buyer_contact=release.get('buyer', {}).get('contact', {}),
                                        buyer_type=release.get('buyer', {}).get('type', ''),
                                        last_edited_date=release.get('lastEditedDate', ''),
                                        lot_constraints=release.get('tender', {}).get('lotConstraints', '')
                                    )

                                    validation_errors = notice.validate()
                                    
                                    if validation_errors:
                                        logger.warning(f"Validation issues for {notice.procurement_identifier}: {', '.join(validation_errors)}")
                                    
                                    tender_info = {
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
                                        "Contract Start Date": notice.contract_dates.get('start', ''),
                                        "Contract End Date": notice.contract_dates.get('end', ''),
                                        "Contract Duration": notice.contract_dates.get('duration', ''),
                                        "Procurement Category": notice.procurement_category,
                                        "CPV Codes": ", ".join(f"{code.get('id', '')} - {code.get('description', '')}" for code in notice.cpv_codes),
                                        "Lot Constraints": notice.lot_constraints,

                                        # Framework section
                                        "Framework End Date": notice.framework_end_date,
                                        "Maximum Participants": notice.framework_max_participants,
                                        "Framework Description": notice.framework_description,
                                        "Award Method": notice.framework_award_method,
                                        "Framework Buyers": "\n".join(notice.framework_buyers),

                                        # Rest of sections...
                                        "Validation Warnings": "\n".join(validation_errors) if validation_errors else "None",
                                        "Custom Fields": json.dumps(getattr(notice, 'custom_fields', {})) if hasattr(notice, 'custom_fields') and notice.custom_fields else '',
                                        "Unused OCDS Fields": ', '.join(getattr(notice, 'unused_fields', [])) if hasattr(notice, 'unused_fields') and notice.unused_fields else ''
                                    }
                                    
                                    results_by_type['UK3'].append(tender_info)
                                    logger.info(f"Added UK3 notice {notice.notice_identifier}")
                                    
                                    # Create separate worksheet for lots detail if lots exist
                                    if notice.lots:
                                        lots_data = []
                                        for idx, lot in enumerate(notice.lots):
                                            lot_data = {
                                                "OCID": notice.procurement_identifier,
                                                "Notice ID": notice.notice_identifier,
                                                "Lot Number": idx + 1,
                                                "Lot Title": lot.get('title', ''),
                                                "Lot Description": lot.get('description', ''),
                                                "Value (excl VAT)": lot.get('value', {}).get('amount', 0),
                                                "Value (incl VAT)": lot.get('value', {}).get('amountGross', 0),
                                                "Currency": lot.get('value', {}).get('currency', ''),
                                                "SME Suitable": lot.get('sme_suitable', False),
                                                "VCSE Suitable": lot.get('vcse_suitable', False),
                                            }
                                            lots_data.append(lot_data)

                                        # Add lots worksheet
                                        if lots_data:
                                            try:
                                                lots_df = pd.DataFrame(lots_data)
                                                try:
                                                    lots_worksheet = sh.worksheet(f"UK3_Lots") 
                                                except gspread.exceptions.WorksheetNotFound:
                                                    lots_worksheet = sh.add_worksheet(title=f"UK3_Lots", rows="1000", cols="50")
                                                
                                                # Get existing data to append to
                                                try:
                                                    existing_data = lots_worksheet.get_all_records()
                                                    existing_df = pd.DataFrame(existing_data)
                                                    
                                                    # Append new data
                                                    if not existing_df.empty:
                                                        combined_df = pd.concat([existing_df, lots_df], ignore_index=True)
                                                    else:
                                                        combined_df = lots_df
                                                        
                                                    lots_worksheet.clear()
                                                    lots_worksheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
                                                    logger.info(f"Updated UK3_Lots worksheet with {len(lots_data)} lots")
                                                except Exception as e:
                                                    logger.error(f"Error updating lots worksheet: {str(e)}")
                                                    lots_worksheet.clear()
                                                    lots_worksheet.update([lots_df.columns.values.tolist()] + lots_df.values.tolist())
                                            except Exception as e:
                                                logger.error(f"Error processing lots data: {str(e)}")
                                except Exception as e:
                                    logger.error(f"Error processing UK3 notice: {str(e)}")
                                    
                            elif notice_type == 'UK4':
                                try:
                                    notice = UK4Notice(
                                        notice_identifier=release.get('id', ''),
                                        procurement_identifier=release.get('ocid', ''),
                                        tender_title=release.get('tender', {}).get('title', ''),
                                        tender_description=release.get('tender', {}).get('description', ''),
                                        tender_status=release.get('tender', {}).get('status', ''),
                                        tender_value_amount=release.get('tender', {}).get('value', {}).get('amountGross', 0),
                                        tender_value_currency=release.get('tender', {}).get('value', {}).get('currency', 'GBP'),
                                        procurement_method=release.get('tender', {}).get('procurementMethodDetails', ''),
                                        procurement_category=release.get('tender', {}).get('mainProcurementCategory', ''),
                                        cpv_codes=[item.get('additionalClassifications', [{}])[0] for item in release.get('tender', {}).get('items', [])],
                                        award_criteria=[],  # Will populate from lots below
                                        tender_period_end=release.get('tender', {}).get('tenderPeriod', {}).get('endDate', ''),
                                        enquiry_period_end=release.get('tender', {}).get('enquiryPeriod', {}).get('endDate', ''),
                                        submission_method=release.get('tender', {}).get('submissionMethodDetails', ''),
                                        buyer_name=release.get('buyer', {}).get('name', ''),
                                        buyer_id=release.get('buyer', {}).get('id', ''),
                                        language="en",
                                        published_date=release.get('date', '')
                                    )

                                    # Extract award criteria from first lot if exists
                                    if release.get('tender', {}).get('lots'):
                                        lot = release['tender']['lots'][0]
                                        if lot.get('awardCriteria', {}).get('criteria'):
                                            notice.award_criteria = [
                                                {
                                                    'name': c.get('name', ''),
                                                    'type': c.get('type', ''),
                                                    'weight': c.get('numbers', [{}])[0].get('number', 0)
                                                }
                                                for c in lot['awardCriteria']['criteria']
                                            ]

                                    validation_errors = notice.validate()
                                    
                                    if validation_errors:
                                        logger.warning(f"Validation issues for {notice.procurement_identifier}: {', '.join(validation_errors)}")
                                    
                                    tender_info = {
                                        "Notice Type": "UK4",
                                        "Notice Identifier": notice.notice_identifier,
                                        "Procurement Identifier": notice.procurement_identifier,
                                        "Published Date": notice.published_date,
                                        "Last Edited Date": notice.last_edited_date if hasattr(notice, 'last_edited_date') else '',
                                        "Tender Title": notice.tender_title,
                                        "Tender Description": notice.tender_description,
                                        "Tender Status": notice.tender_status,
                                        "Tender Value Amount": notice.tender_value_amount,
                                        "Tender Value Currency": notice.tender_value_currency,
                                        "Procurement Method": notice.procurement_method,
                                        "Procurement Category": notice.procurement_category,
                                        "CPV Codes": ", ".join(f"{code.get('id', '')} - {code.get('description', '')}" for code in notice.cpv_codes if 'id' in code and 'description' in code),
                                        "Tender Period End": notice.tender_period_end,
                                        "Enquiry Period End": notice.enquiry_period_end,
                                        "Submission Method": notice.submission_method,
                                        "Award Criteria": "\n".join(f"{c.get('name', '')}: {c.get('weight', '')}% ({c.get('type', '')})" for c in notice.award_criteria),
                                        "Buyer Name": notice.buyer_name,
                                        "Buyer ID": notice.buyer_id,
                                        "Validation Warnings": "\n".join(validation_errors) if validation_errors else "None",
                                        "Custom Fields": json.dumps(getattr(notice, 'custom_fields', {})) if hasattr(notice, 'custom_fields') and notice.custom_fields else '',
                                        "Unused OCDS Fields": ', '.join(getattr(notice, 'unused_fields', [])) if hasattr(notice, 'unused_fields') and notice.unused_fields else ''
                                    }
                                    
                                    results_by_type['UK4'].append(tender_info)
                                    logger.info(f"Added UK4 notice {notice.notice_identifier}")
                                except Exception as e:
                                    logger.error(f"Error processing UK4 notice: {str(e)}")
                            # Add other notice types here as they are implemented
                            
                else:
                    logger.error(f"Error fetching data for OCID {ocid}: HTTP {response.status_code}")
            except Exception as e:
                logger.error(f"Exception while processing OCID {ocid}: {str(e)}")

        # Write each notice type to its own worksheet
        for notice_type, results in results_by_type.items():
            try:
                if results:  # Only process if we have data
                    logger.info(f"Writing {len(results)} {notice_type} records to sheet")
                    
                    # Create or get worksheet for this notice type
                    try:
                        worksheet = sh.worksheet(notice_type)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sh.add_worksheet(title=notice_type, rows="1000", cols="100")
                        logger.info(f"Created new worksheet for {notice_type}")

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

                    # Update sheet with header row + data rows
                    worksheet.clear()
                    if not df.empty:
                        cell_list = [df.columns.values.tolist()] + df.values.tolist()
                        worksheet.update(cell_list)
                        logger.info(f"Updated {notice_type} worksheet with {len(results)} rows")
                    else:
                        logger.warning(f"No data to write for {notice_type}")
                else:
                    logger.info(f"No data for {notice_type}")
            except Exception as e:
                logger.error(f"Error writing {notice_type} data to sheet: {str(e)}")

        last_run_time = time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Data successfully written to Google Sheets at {last_run_time}!")
        
        return True, f"Data successfully processed at {last_run_time}"
    except Exception as e:
        error_message = f"Error processing data: {str(e)}"
        logger.error(error_message)
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