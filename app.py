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

        # Initialize results lists
        notice_results = []
        lot_results = []
        award_results = []

        # Define API URL
        API_BASE_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages/"


        for ocid in ocid_list:
            # Ensure OCID is URL-encoded
            encoded_ocid = quote_plus(ocid.strip())
            response = requests.get(API_BASE_URL + encoded_ocid)
            
            if response.status_code == 200:
                data = response.json()
                # Process single release
                release = data["releases"][0]

                contract_docs = release.get("contracts", [])[0].get("documents", []) if release.get("contracts") else []
                award_docs = release.get("awards", [])[0].get("documents", []) if release.get("awards") else []
                tender_docs = release.get("tender", {}).get("documents", [])
                planning_docs = release.get("planning", {}).get("documents", [])

                # Get documents in priority order
                if contract_docs:
                    documents = contract_docs
                elif award_docs:
                    documents = award_docs
                elif tender_docs:
                    documents = tender_docs
                elif planning_docs:
                    documents = planning_docs
                else:
                    continue

                notice_type = documents[-1].get("noticeType")
                lots = release.get("tender", {}).get("lots", [])
                is_update = any('update' in tag.lower() for tag in release.get('tag', []))

                if notice_type in ["UK1", "UK2", "UK3"]:
                    if "planning" in release:
                        # Extract notice fields
                        notice_fields = {
                        "OCID": release.get("ocid", "N/A"),
                        "Notice Type": notice_type,
                        "Is Update": is_update,
                        "Published Date": release.get("date", "N/A"),
                        "Notice ID": release.get("id", "N/A"),
                        "Reference": release.get("tender", {}).get("id", "N/A"),
                        "Notice Title": release.get("tender", {}).get("title", "N/A"),
                        "Notice Description": release.get("tender", {}).get("description", "N/A"),
                        "Value ex VAT": release.get("tender", {}).get("value", {}).get("amount", "N/A"),
                        "Value inc VAT": release.get("tender", {}).get("value", {}).get("amountGross", "N/A"),
                        "Currency": release.get("tender", {}).get("value", {}).get("currency", "N/A"),
                        "Threshold": "Above the relevant threshold" if release.get("tender", {}).get("aboveThreshold", False) else "Below the relevant threshold",
                        # Assume contract dates are same for all lots
                        "Contract Start Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("startDate", "N/A"),
                        "Contract End Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("endDate", "N/A"),
                        "Publication date of tender notice (estimated)": release.get("tender", {}).get("communication", {}).get("futureNoticeDate", "N/A"),
                        "Main Category": release.get("tender", {}).get("mainProcurementCategory", "N/A"),
                        "CPV Code": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A") if len(lots) == 1
                            else "See lots sheet for CPV codes",
                        "Submission Deadline": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                        "Enquiry Deadline": release.get("planning", {}).get("milestones", [{}])[0].get("dueDate", "N/A"),
                        "Estimated Award Date": release.get("tender", {}).get("awardPeriod", {}).get("endDate", "N/A"),
                        "Award Criteria": (
                                "Detailed in lots sheet" if len(lots) > 1
                                else (
                                    release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("description", "N/A")
                                    if not release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("criteria")
                                    else "Refer to notice for detailed weightings"
                                )
                            ),
                        "Framework Agreement": (
                                "Closed Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "closed"
                                else "Open Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "open"
                                else "N/A"
                            ), 
                        "Call off method": (
                                "With competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withReopeningCompetition"
                                else "Without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withoutReopeningCompetition"
                                else "Either with or without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withAndWithoutReopeningCompetition"
                                else "N/A"
                            ),
                        "Procedure Type": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                        "Procedure Description": release.get("tender", {}).get("procedure", {}).get("features", "N/A"),
                        "Contracting Authority": release.get("buyer", {}).get("name", "N/A"),
                        "PPON": release.get("buyer", {}).get("id", "N/A"),
                        "Contact Name": release.get("parties", [{}])[0].get("contactPoint", {}).get("name", "N/A"),
                        "Contact Email": release.get("parties", [{}])[0].get("contactPoint", {}).get("email", "N/A"),

                        }
                        notice_results.append(notice_fields)

                        if len(lots) > 1:  # Only create lot entries for multiple lots
                            for idx, lot in enumerate(lots, 1):
                                lot_fields = { 
                                    "OCID": release.get("ocid", "N/A"),
                                    "Notice Type": notice_type,
                                    "Is Update": is_update,
                                    "Lot Number": idx,
                                    "Lot Title": lot.get("title", "N/A"),
                                    "Lot Description": lot.get("description", "N/A"),
                                    "Lot Value ex VAT": lot.get("value", {}).get("amount", "N/A"),
                                    "Lot Value inc VAT": lot.get("value", {}).get("amountGross", "N/A"),
                                    "Lot Currency": lot.get("value", {}).get("currency", "N/A"),
                                    "Lot Start Date": lot.get("contractPeriod", {}).get("startDate", "N/A"),
                                    "Lot End Date": lot.get("contractPeriod", {}).get("endDate", "N/A"),
                                    "SME Suitable": lot.get("suitability", {}).get("sme", False),
                                    "VCSE Suitable": lot.get("suitability", {}).get("vcse", False),
                                    "Award Criteria": (
                                        lot.get("awardCriteria", {}).get("description", "N/A")
                                        if not lot.get("awardCriteria", {}).get("criteria")
                                        else "Refer to notice for detailed weightings"
                                        ),
                                    "CPV Code": (
                                        next(
                                            (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                            for item in release.get("tender", {}).get("items", [])
                                            if item.get("relatedLot") == lot.get("id")),
                                            "N/A"
                                        )
                                    ),
                                }
                                lot_results.append(lot_fields)


                elif notice_type in ["UK4"]:
                # Extract notice fields
                    notice_fields = {
                        "OCID": release.get("ocid", "N/A"),
                        "Notice Type": notice_type,
                        "Is Update": is_update,
                        "Published Date": release.get("date", "N/A"),
                        "Notice ID": release.get("id", "N/A"),
                        "Reference": release.get("tender", {}).get("id", "N/A"),
                        "Notice Title": release.get("tender", {}).get("title", "N/A"),
                        "Notice Description": release.get("tender", {}).get("description", "N/A"),
                        "Value ex VAT": release.get("tender", {}).get("value", {}).get("amount", "N/A"),
                        "Value inc VAT": release.get("tender", {}).get("value", {}).get("amountGross", "N/A"),
                        "Currency": release.get("tender", {}).get("value", {}).get("currency", "N/A"),
                        "Threshold": "Above the relevant threshold" if release.get("tender", {}).get("aboveThreshold", False) else "Below the relevant threshold",
                        "Contract Start Date": release.get("tender", {}).get("contractPeriod", {}).get("startDate", "N/A"),
                        "Contract End Date": release.get("tender", {}).get("contractPeriod", {}).get("endDate", "N/A"),
                        "Renewal": release.get("tender", {}).get("renewal", {}).get("description", "N/A"),
                        "Options": release.get("tender", {}).get("options", {}).get("description", "N/A"),
                        "Main Category": release.get("tender", {}).get("mainProcurementCategory", "N/A"),
                        "CPV Code": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A") if len(lots) == 1
                        else "See lots sheet for CPV codes",
                        "Particular Suitability": (
                            ", ".join(filter(None, [
                                "SME" if release.get("tender", {}).get("lots", [{}])[0].get("suitability", {}).get("sme") else None,
                                "VCSE" if release.get("tender", {}).get("lots", [{}])[0].get("suitability", {}).get("vcse") else None
                            ])) or "N/A"
                        ),
                        "Submission Deadline": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                        "Submission Method": release.get("tender", {}).get("submissionMethodDetails", "N/A"),
                        "Enquiry Deadline": release.get("tender", {}).get("enquiryPeriod", {}).get("endDate", "N/A"),
                        "Estimated Award Date": release.get("tender", {}).get("awardPeriod", {}).get("endDate", "N/A"),
                        "Award Criteria": (
                            "Detailed in lots sheet" if len(lots) > 1
                            else (
                                release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("description", "N/A")
                                if not release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("criteria")
                                else "Refer to notice for detailed weightings"
                            )
                        ),
                        "Framework Agreement": (
                            "Closed Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "closed"
                            else "Open Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "open"
                            else "N/A"
                        ), 
                        "Call off method": (
                            "With competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withReopeningCompetition"
                            else "Without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withoutReopeningCompetition"
                            else "Either with or without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withAndWithoutReopeningCompetition"
                            else "N/A"
                        ),
                        "Procedure Type": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                        "Contracting Authority": release.get("buyer", {}).get("name", "N/A"),
                        "PPON": release.get("buyer", {}).get("id", "N/A"),
                        "Contact Name": release.get("parties", [{}])[0].get("contactPoint", {}).get("name", "N/A"),
                        "Contact Email": release.get("parties", [{}])[0].get("contactPoint", {}).get("email", "N/A"),
                    }
                    
                    notice_results.append(notice_fields)
                    
                    
                    if len(lots) > 1:  # Only create lot entries for multiple lots
                        for idx, lot in enumerate(lots, 1):
                            lot_fields = { 
                                "OCID": release.get("ocid", "N/A"),
                                "Notice Type": notice_type,
                                "Is Update": is_update,
                                "Lot Number": idx,
                                "Lot Title": lot.get("title", "N/A"),
                                "Lot Description": lot.get("description", "N/A"),
                                "Lot Value ex VAT": lot.get("value", {}).get("amount", "N/A"),
                                "Lot Value inc VAT": lot.get("value", {}).get("amountGross", "N/A"),
                                "Lot Currency": lot.get("value", {}).get("currency", "N/A"),
                                "Lot Start Date": lot.get("contractPeriod", {}).get("startDate", "N/A"),
                                "Lot End Date": lot.get("contractPeriod", {}).get("endDate", "N/A"),
                                "SME Suitable": lot.get("suitability", {}).get("sme", False),
                                "VCSE Suitable": lot.get("suitability", {}).get("vcse", False),
                                "Award Criteria": (
                                    lot.get("awardCriteria", {}).get("description", "N/A")
                                    if not lot.get("awardCriteria", {}).get("criteria")
                                    else "Refer to notice for detailed weightings"
                                    ),
                                "CPV Code": (
                                        next(
                                        (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                        for item in release.get("tender", {}).get("items", [])
                                        if item.get("relatedLot") == lot.get("id")),
                                        "N/A"
                                    )
                                ),
                            }
                            lot_results.append(lot_fields)
            
                    
                
                
                elif notice_type in ["UK5", "UK6", "UK7"]:
                    # First try to get documents from contracts, if not found try awards
                    # Extract notice fields
                    notice_fields = {
                    "OCID": release.get("ocid", "N/A"),
                    "Notice Type": notice_type,
                    "Is Update": is_update,
                    "Published Date": release.get("date", "N/A"),
                    "Notice ID": release.get("id", "N/A"),
                    "Reference": release.get("tender", {}).get("id", "N/A"),
                    "Notice Title": release.get("tender", {}).get("title", "N/A"),
                    "Notice Description": release.get("tender", {}).get("description", "N/A"),
                    "Awarded Amount ex VAT": (
                        release.get("contracts", [{}])[0].get("value", {}).get("amount", "N/A") 
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("value", {}).get("amount", "N/A")
                    ),
                    "Awarded Amount inc VAT": (
                        release.get("contracts", [{}])[0].get("value", {}).get("amountGross", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("value", {}).get("amountGross", "N/A")
                    ),
                    "Currency": (
                        release.get("contracts", [{}])[0].get("value", {}).get("currency", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("value", {}).get("currency", "N/A")
                    ),
                    "Threshold": (
                        "Above the relevant threshold" 
                        if (notice_type == "UK7" and release.get("contracts", [{}])[0].get("aboveThreshold", False))
                        or (notice_type in ["UK5", "UK6"] and release.get("awards", [{}])[0].get("aboveThreshold", False))
                        else "Below the relevant threshold"
                    ),
                    "Earliest date the contract will be signed": (
                        release.get("awards", [{}])[0].get("milestones", [{}])[0].get("dueDate", "N/A") 
                        if release.get("awards", [{}])[0].get("milestones", [{}])[0].get("type") == "futureSignatureDate" 
                        else "N/A"
                    ),
                    "Contract Start Date": (
                        release.get("contracts", [{}])[0].get("period", {}).get("startDate", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("contractPeriod", {}).get("startDate", "N/A")
                    ),
                    "Contract End Date": (
                        release.get("contracts", [{}])[0].get("period", {}).get("endDate", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("contractPeriod", {}).get("endDate", "N/A")
                    ),
                    "Suppliers": (
                        ", ".join([supplier.get("name", "N/A") for supplier in release.get("awards", [{}])[0].get("suppliers", [])])
                    ),
                    "Supplier ID": (
                        ", ".join([supplier.get("id", "N/A") for supplier in release.get("awards", [{}])[0].get("suppliers", [])])
            ),
                    "Main Category": (
                        "See awards sheet" 
                        if notice_type in ["UK6", "UK7"]
                        else release.get("awards", [{}])[0].get("mainProcurementCategory", "N/A")
                    ),
                    "CPV Code": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A") if len(lots) == 1
                        else "See lots sheet for CPV codes",
                    "Submission Deadline": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                    "Procurement Method": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                    # To check if always the case. What if no bids for example
                    "Number of Tenders received": next(
                        (stat.get("value", "N/A") 
                        for stat in release.get("bids", {}).get("statistics", [])
                        if stat.get("measure") == "bids"),
                        "N/A"
                    ),
                    "Number of Tenders assessed": next(
                        (stat.get("value", "N/A") 
                        for stat in release.get("bids", {}).get("statistics", [])
                        if stat.get("measure") == "finalStageBids"),
                        "N/A"
                    ),
                    "Award decision date": release.get("awards", [{}])[0].get("date", "N/A"),
                    "Date assessment summaries sent": release.get("awards", [{}])[0].get("assessmentSummariesDateSent", "N/A"),
                    "Contracting Authority": release.get("buyer", {}).get("name", "N/A"),
                    "PPON": release.get("buyer", {}).get("id", "N/A"),
                    "Contact Name": release.get("parties", [{}])[0].get("contactPoint", {}).get("name", "N/A"),
                    "Contact Email": release.get("parties", [{}])[0].get("contactPoint", {}).get("email", "N/A"),
                    }
                    notice_results.append(notice_fields)

                    # Check lots info for UK6 notices and data pull through
                    if len(lots) > 1:  # Only create lot entries for multiple lots
                        for idx, lot in enumerate(lots, 1):
                            lot_fields = { 
                                "OCID": release.get("ocid", "N/A"),
                                "Notice Type": notice_type,
                                "Is Update": is_update,
                                "Lot Number": idx,
                                "Lot Title": lot.get("title", "N/A"),
                                "Lot Description": lot.get("description", "N/A"),
                                "Lot Value ex VAT": lot.get("value", {}).get("amount", "N/A"),
                                "Lot Value inc VAT": lot.get("value", {}).get("amountGross", "N/A"),
                                "Lot Currency": lot.get("value", {}).get("currency", "N/A"),
                                "Lot Start Date": lot.get("contractPeriod", {}).get("startDate", "N/A"),
                                "Lot End Date": lot.get("contractPeriod", {}).get("endDate", "N/A"),
                                "SME Suitable": lot.get("suitability", {}).get("sme", False),
                                "VCSE Suitable": lot.get("suitability", {}).get("vcse", False),
                                "Award Criteria": (
                                    lot.get("awardCriteria", {}).get("description", "N/A")
                                    if not lot.get("awardCriteria", {}).get("criteria")
                                    else "Refer to notice for detailed weightings"
                                    ),
                                "CPV Code": (
                                        next(
                                        (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                        for item in release.get("tender", {}).get("items", [])
                                        if item.get("relatedLot") == lot.get("id")),
                                        "N/A"
                                    )
                                ),
                            }
                            lot_results.append(lot_fields)

                #Separate UK 6 notices out - fields differ from other awards
                    if notice_type in ["UK6", "UK7"]:
                        awards = release.get("awards", [])
                        for award in awards:
                            award_fields = {
                                "OCID": release.get("ocid", "N/A"),
                                "Notice Type": notice_type,
                                "Notice ID": release.get("id", "N/A"),
                                "Is Update": is_update,
                                "Contract Title": award.get("title", "N/A"),
                                # For UK7, try to get value from contract first, then fall back to award
                                "Value ex VAT": (
                                    release.get("contracts", [{}])[0].get("value", {}).get("amount", "N/A") 
                                    if notice_type == "UK7" 
                                    else award.get("value", {}).get("amount", "N/A")
                                ),
                                "Value inc VAT": (
                                    release.get("contracts", [{}])[0].get("value", {}).get("amountGross", "N/A")
                                    if notice_type == "UK7"
                                    else award.get("value", {}).get("amountGross", "N/A")
                                ),
                                "Currency": award.get("value", {}).get("currency", "N/A"),
                                "Suppliers": ", ".join([supplier.get("name", "N/A") for supplier in award.get("suppliers", [])]),
                                "Contract Start Date": (
                                    release.get("contracts", [{}])[0].get("period", {}).get("startDate", "N/A")
                                    if notice_type == "UK7"
                                    else award.get("contractPeriod", {}).get("startDate", "N/A")
                                ),
                                "Contract End Date": (
                                    release.get("contracts", [{}])[0].get("period", {}).get("endDate", "N/A")
                                    if notice_type == "UK7"
                                    else award.get("contractPeriod", {}).get("endDate", "N/A")
                                ),
                                "Main Category": award.get("mainProcurementCategory", release.get("tender", {}).get("mainProcurementCategory", "N/A")),
                                "CPV Code": next(
                                    (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                    for item in award.get("items", [])
                                    if item.get("additionalClassifications")),
                                    "N/A"
                                )
                            }
                            award_results.append(award_fields)

                )

            elif response.status_code == 404:
                print(f"OCID {ocid} not found. Skipping...")
            else:
                print(f"Error fetching OCID {ocid}, Status Code: {response.status_code}")

        # Convert results to DataFrames
        notices_df = pd.DataFrame(notice_results)
        lots_df = pd.DataFrame(lot_results)
        awards_df = pd.DataFrame(award_results)
        
        # Update Google Sheets with results
        notices_sheet = sh.worksheet("Notices")
        lots_sheet = sh.worksheet("Lots")
        awards_sheet = sh.worksheet("Awards")

        # Clean data - replace None, empty lists, and other problematic values
        def clean_value(val):
            if val is None:
                return ""
            if isinstance(val, (list, dict)):
                if not val:  # Empty list or dict
                    return ""
                return str(val)
            return val

        # Clean DataFrames
        for df in [notices_df, lots_df, awards_df]:
            for col in df.columns:
                df[col] = df[col].apply(clean_value)

        # Update sheets without clearing first
        if not notices_df.empty:
            notices_sheet.update('A1', [notices_df.columns.values.tolist()] + notices_df.values.tolist(), value_input_option='RAW')
        if not lots_df.empty:
            lots_sheet.update('A1', [lots_df.columns.values.tolist()] + lots_df.values.tolist(), value_input_option='RAW')
        if not awards_df.empty:
            awards_sheet.update('A1', [awards_df.columns.values.tolist()] + awards_df.values.tolist(), value_input_option='RAW')

        last_run_time = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Data successfully written to Google Sheets at {last_run_time}!")
        return True, f"Data successfully processed at {last_run_time}"
        
        

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