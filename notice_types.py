from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime
import re
from decimal import Decimal

@dataclass
class Address:
    """Structured address information"""
    street: str
    locality: str
    postal_code: str
    country: str
    region: Optional[str] = None
    
    def validate(self) -> List[str]:
        errors = []
        if not self.postal_code.strip():
            errors.append("Postal code is required")
        if not self.street.strip():
            errors.append("Street address is required")
        return errors

@dataclass
class BuyerDetails:
    """Buyer/contracting authority information"""
    name: str
    id: str  # PPON number
    address: Address
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    website: Optional[str] = None
    org_type: Optional[str] = None
    
    def validate(self) -> List[str]:
        errors = []
        if self.contact_email and not re.match(r"[^@]+@[^@]+\.[^@]+", self.contact_email):
            errors.append("Invalid email format")
        if not self.id.startswith("GB-PPON-"):
            errors.append("PPON must start with GB-PPON-")
        errors.extend(self.address.validate())
        return errors

@dataclass
class OCDSValidationHelper:
    """Helper class for OCDS field validation based on standard"""
    
    OCDS_TENDER_FIELDS = {
        'id': True,  # True = required, False = optional
        'title': True,
        'description': True,
        'status': True,
        'items': True,
        'value': False,
        'procurementMethod': False,
        'procurementMethodDetails': False,
        'procurementMethodRationale': False,
        'mainProcurementCategory': False,
        'additionalProcurementCategories': False,
        'awardCriteria': False,
        'awardCriteriaDetails': False,
        'submissionMethod': False,
        'submissionMethodDetails': False,
        'tenderPeriod': False,
        'enquiryPeriod': False,
        'hasEnquiries': False,
        'eligibilityCriteria': False,
        'awardPeriod': False,
        'contractPeriod': False,
        'numberOfTenderers': False,
        'tenderers': False,
        'documents': False,
        'milestones': False,
        'amendments': False
    }
    
    @staticmethod
    def validate_tender_section(data: dict) -> Tuple[List[str], List[str]]:
        """Validate tender section against OCDS 1.1 schema"""
        errors = []
        warnings = []
        
        # Check required fields
        for field, required in OCDSValidationHelper.OCDS_TENDER_FIELDS.items():
            if required and not data.get(field):
                errors.append(f"Required tender field missing: {field}")
            elif not required and field not in data:
                warnings.append(f"Optional OCDS field not used: {field}")
                
        # Check for non-OCDS fields
        custom_fields = [
            field for field in data.keys() 
            if field not in OCDSValidationHelper.OCDS_TENDER_FIELDS
        ]
        if custom_fields:
            warnings.append(f"Non-OCDS fields present: {', '.join(custom_fields)}")
            
        return errors, warnings

# Modified UK3Notice that doesn't inherit from BaseNotice
@dataclass
class UK3Notice:
    """UK3 - Planned procurement notice"""
    # Required fields first (no defaults)
    notice_identifier: str  # e.g. "2025/S 000-010758"
    procurement_identifier: str  # OCID
    published_date: str
    commercial_tool: str  # "Establishes an open framework"
    total_value_amount: float
    total_value_amount_gross: float
    total_value_currency: str
    contract_dates: Dict[str, Any]  # Contains start, end, duration
    procurement_category: str  # e.g. "Services"
    cpv_codes: List[Dict[str, str]]
    lots: List[Dict[str, Any]]
    framework_end_date: str
    framework_max_participants: int
    framework_description: str 
    framework_award_method: str
    framework_buyers: List[str]
    sme_suitable: bool
    vcse_suitable: bool
    publication_date: str
    tender_deadline: str
    electronic_submission: bool
    submission_languages: List[str]
    award_date: str
    award_criteria: List[Dict[str, Any]]
    trade_agreements: List[str]
    procedure_type: str
    procedure_description: str
    buyer_name: str
    buyer_id: str  # PPON number
    buyer_address: Dict[str, str]
    buyer_contact: Dict[str, str]
    buyer_type: str

    # Optional fields with defaults last
    last_edited_date: Optional[str] = None
    lot_constraints: Optional[str] = None
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    unused_fields: List[str] = field(default_factory=list)

    def validate(self) -> List[str]:
        """Validate all fields according to business rules"""
        errors = []

        # Validate dates if they are valid datetime strings
        try:
            if self.tender_deadline and self.award_date:
                tender_deadline = datetime.fromisoformat(self.tender_deadline.replace('Z', '+00:00'))
                award_date = datetime.fromisoformat(self.award_date.replace('Z', '+00:00'))
                if tender_deadline > award_date:
                    errors.append("Tender deadline cannot be after award date")
        except (ValueError, TypeError, AttributeError):
            errors.append("Invalid date format in tender_deadline or award_date")

        # Validate contract dates if they exist and are valid
        try:
            if self.contract_dates and 'start' in self.contract_dates and 'end' in self.contract_dates:
                if self.contract_dates['start'] and self.contract_dates['end']:
                    contract_start = datetime.fromisoformat(self.contract_dates['start'].replace('Z', '+00:00'))
                    contract_end = datetime.fromisoformat(self.contract_dates['end'].replace('Z', '+00:00'))
                    if contract_start >= contract_end:
                        errors.append("Contract end date must be after start date")
        except (ValueError, TypeError, AttributeError):
            errors.append("Invalid date format in contract_dates")

        # Validate framework values
        if self.framework_max_participants and self.framework_max_participants <= 0:
            errors.append("Maximum participants must be positive")

        # Validate lots if they have the required structure for validation
        try:
            lot_total = sum(lot.get('value', {}).get('amount', 0) for lot in self.lots)
            if abs(lot_total - self.total_value_amount) > 0.01 and lot_total > 0 and self.total_value_amount > 0:
                errors.append(f"Sum of lot values ({lot_total}) does not match total value ({self.total_value_amount})")
        except (TypeError, ValueError, AttributeError):
            errors.append("Invalid lot value structure")

        # Basic validation of buyer details
        if not self.buyer_id:
            errors.append("Buyer ID is required")
        if not self.buyer_name:
            errors.append("Buyer name is required")

        return errors

# Modified UK4Notice that doesn't inherit from BaseNotice
@dataclass
class UK4Notice:
    """Class for handling UK4 tender notices"""
    # Required fields first (no defaults)
    notice_identifier: str
    procurement_identifier: str
    tender_title: str
    tender_description: str
    tender_status: str
    tender_value_amount: float
    tender_value_currency: str
    procurement_method: str
    procurement_category: str
    cpv_codes: List[Dict]
    award_criteria: List[Dict]
    tender_period_end: str
    enquiry_period_end: str
    submission_method: str
    buyer_name: str
    buyer_id: str
    
    # Optional fields with defaults last
    language: str = "en"
    published_date: Optional[str] = None
    last_edited_date: Optional[str] = None
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    unused_fields: List[str] = field(default_factory=list)

    def validate(self) -> List[str]:
        """Validate the notice data"""
        errors = []
        required_fields = [
            'notice_identifier', 'procurement_identifier', 'tender_title',
            'tender_description', 'tender_value_amount', 'procurement_method'
        ]
        
        for field in required_fields:
            value = getattr(self, field, None)
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"Missing required field: {field}")
                
        return errors