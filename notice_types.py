from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
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
class Value:
    """Monetary value with currency"""
    amount: Decimal
    currency: str
    amount_gross: Optional[Decimal] = None
    
    def validate(self) -> List[str]:
        errors = []
        if self.amount < 0:
            errors.append("Amount cannot be negative")
        if self.amount_gross and self.amount_gross < self.amount:
            errors.append("Gross amount cannot be less than net amount")
        return errors

@dataclass
class AwardCriterion:
    """Single award criterion with name, description, type and weighting"""
    name: str
    description: str
    type: str  # 'quality' or 'price'
    weighting: float

    def validate(self) -> List[str]:
        errors = []
        if self.type not in ['quality', 'price']:
            errors.append(f"Invalid criterion type: {self.type}")
        if not 0 <= self.weighting <= 100:
            errors.append(f"Weighting must be between 0 and 100: {self.weighting}")
        if not self.name.strip():
            errors.append("Name is required")
        return errors

@dataclass 
class Lot:
    """Framework lot information"""
    id: str
    title: str
    description: str
    value_amount: float
    value_currency: str
    value_amount_gross: float
    status: str
    sme_suitable: bool
    vcse_suitable: bool
    award_criteria: List[AwardCriterion]

    def validate(self) -> List[str]:
        errors = []
        # Validate basic fields
        if not self.title.strip():
            errors.append(f"Lot {self.id}: Title is required")
        if not self.description.strip():
            errors.append(f"Lot {self.id}: Description is required")
            
        # Validate value
        if self.value_amount < 0:
            errors.append(f"Lot {self.id}: Value amount cannot be negative")
        if self.value_amount_gross < self.value_amount:
            errors.append(f"Lot {self.id}: Gross value cannot be less than net value")
            
        # Validate award criteria
        total_weighting = sum(criterion.weighting for criterion in self.award_criteria)
        if total_weighting != 100:
            errors.append(f"Lot {self.id}: Award criteria weightings must sum to 100%")
            
        # Validate each award criterion
        for criterion in self.award_criteria:
            errors.extend(criterion.validate())
            
        return errors

@dataclass
class LotDetailedInfo(Lot):
    """Extended lot information with sublots and constraints"""
    parent_lot: Optional[str] = None
    sublots: List['LotDetailedInfo'] = None
    min_value: Optional[Value] = None
    max_value: Optional[Value] = None
    award_criteria_details: Optional[Dict[str, Any]] = None
    
    def validate(self) -> List[str]:
        errors = []
        if self.min_value and self.max_value:
            if self.min_value.amount > self.max_value.amount:
                errors.append("Minimum value cannot exceed maximum value")
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
    def validate_tender_section(data: dict) -> List[str]:
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

@dataclass
class BaseNotice:
    """Base class for all notice types following OCDS 1.1 schema"""
    ocid: str  # Open Contracting ID
    id: str    # Notice identifier
    date: str  # ISO date
    tender: Dict[str, Any]  # Tender section following OCDS schema
    parties: List[Dict[str, Any]]  # Organizations involved
    buyer: Dict[str, str]  # Buyer information
    language: str = "en"
    custom_fields: Dict[str, Any] = None  # Store non-OCDS fields
    unused_fields: List[str] = None  # Track unused optional OCDS fields
    
    def validate_ocds(self) -> Tuple[List[str], List[str]]:
        """Validate notice follows OCDS 1.1 structure"""
        errors, warnings = OCDSValidationHelper.validate_tender_section(self.tender)
        
        # Store results
        self.unused_fields = [
            w.replace("Optional OCDS field not used: ", "")
            for w in warnings if "Optional OCDS field not used" in w
        ]
        
        self.custom_fields = {
            field: self.tender[field]
            for field in self.tender.keys()
            if field not in OCDSValidationHelper.OCDS_TENDER_FIELDS
        }
        
        return errors, warnings

@dataclass
class UK3Notice(BaseNotice):
    """UK3 - Planned procurement notice 
    
    Fields are structured to exactly match the Find a Tender notice webpage sections:
    - Summary section at top
    - Scope section
    - Lots section  
    - Framework section
    - Participation section
    - Submission section
    - Award criteria section
    - Other information section
    - Procedure section
    - Contracting authority section
    """
    # Summary section fields
    notice_identifier: str  # e.g. "2025/S 000-010758"
    procurement_identifier: str  # OCID
    published_date: datetime
    last_edited_date: Optional[datetime]

    # Scope section fields 
    commercial_tool: str  # "Establishes an open framework"
    total_value_amount: float
    total_value_amount_gross: float
    total_value_currency: str
    contract_dates: Dict[str, Any]  # Contains start, end, duration
    procurement_category: str  # e.g. "Services"
    cpv_codes: List[Dict[str, str]]
    lot_constraints: Optional[str]

    # Lots section
    lots: List[Lot]

    # Framework section  
    framework_end_date: datetime
    framework_max_participants: int
    framework_description: str 
    framework_award_method: str
    framework_buyers: List[str]

    # Participation section
    sme_suitable: bool
    vcse_suitable: bool

    # Submission section
    publication_date: datetime
    tender_deadline: datetime
    electronic_submission: bool
    submission_languages: List[str]
    award_date: datetime

    # Award criteria section
    award_criteria: List[AwardCriterion]

    # Other information
    trade_agreements: List[str]

    # Procedure section
    procedure_type: str
    procedure_description: str

    # Contracting authority section
    buyer_name: str
    buyer_id: str  # PPON number
    buyer_address: Dict[str, str]
    buyer_contact: Dict[str, str]
    buyer_type: str

    def validate(self) -> List[str]:
        """Validate all fields according to business rules"""
        errors = []

        # Validate dates
        if self.tender_deadline and self.award_date:
            if self.tender_deadline > self.award_date:
                errors.append("Tender deadline cannot be after award date")

        # Validate contract dates
        contract_start = datetime.fromisoformat(self.contract_dates['start'].replace('Z', '+00:00'))
        contract_end = datetime.fromisoformat(self.contract_dates['end'].replace('Z', '+00:00'))
        if contract_start >= contract_end:
            errors.append("Contract end date must be after start date")

        # Validate framework values
        if self.framework_max_participants and self.framework_max_participants <= 0:
            errors.append("Maximum participants must be positive")

        # Validate lots
        lot_total = sum(lot.value_amount for lot in self.lots)
        if abs(lot_total - self.total_value_amount) > 0.01:  # Allow small rounding differences
            errors.append(f"Sum of lot values ({lot_total}) does not match total value ({self.total_value_amount})")

        # Validate each lot
        for lot in self.lots:
            errors.extend(lot.validate())

        # Validate buyer details
        buyer = BuyerDetails(
            name=self.buyer_name,
            id=self.buyer_id,
            address=Address(**self.buyer_address),
            contact_email=self.buyer_contact.get('email')
        )
        errors.extend(buyer.validate())

        return errors

    @classmethod
    def from_api_data(cls, data: Dict[str, Any]) -> 'UK3Notice':
        """Create UK3Notice from API response, mapping fields to match webpage display"""
        tender = data.get('tender', {})
        
        # Get framework agreement details
        framework = tender.get('techniques', {}).get('frameworkAgreement', {})
        has_framework = tender.get('techniques', {}).get('hasFrameworkAgreement', False)
        
        # Map commercial tool based on framework agreement type
        if has_framework and framework.get('isOpenFrameworkScheme'):
            commercial_tool = "Establishes an open framework"
        elif has_framework:
            commercial_tool = "Establishes a framework"
        else:
            commercial_tool = "Not a framework"
            
        # Rest of existing from_api_data implementation
        return cls(
            # ...existing code...
            commercial_tool=commercial_tool,
            # ...existing code...
        )

@dataclass
class UK4Notice(BaseNotice):
    """Class for handling UK4 tender notices"""
    # Required fields first
    notice_identifier: str
    procurement_identifier: str  # OCID
    tender_title: str
    tender_description: str
    tender_status: str
    tender_value_amount: float
    tender_value_currency: str
    procurement_method: str
    procurement_category: str
    buyer_name: str
    buyer_id: str
    cpv_codes: List[Dict]
    award_criteria: List[Dict]
    tender_period_end: str
    enquiry_period_end: str
    submission_method: str
    
    # Optional fields with defaults last
    published_date: str = None
    last_edited_date: Optional[str] = None
    custom_fields: Dict[str, Any] = None
    unused_fields: List[str] = None

    @classmethod
    def from_api_data(cls, data: dict):
        """Create UK4Notice instance from API response data"""
        tender = data.get('tender', {})
        
        # Get latest amendment description if any
        amendments = tender.get('amendments', [])
        latest_amendment = amendments[0].get('description') if amendments else None
        
        # Extract award criteria from first lot
        award_criteria = []
        if tender.get('lots'):
            criteria = tender['lots'][0].get('awardCriteria', {}).get('criteria', [])
            for c in criteria:
                award_criteria.append({
                    'name': c.get('name'),
                    'type': c.get('type'),
                    'weight': c.get('numbers', [{}])[0].get('number')
                })
        
        return cls(
            notice_identifier=data.get('id'),
            procurement_identifier=data.get('ocid'),
            tender_title=tender.get('title'),
            tender_description=tender.get('description'),
            tender_status=tender.get('status'),
            tender_value_amount=tender.get('value', {}).get('amountGross'),
            tender_value_currency=tender.get('value', {}).get('currency'),
            procurement_method=tender.get('procurementMethodDetails'),
            procurement_category=tender.get('mainProcurementCategory'),
            cpv_codes=[item.get('additionalClassifications', [{}])[0] for item in tender.get('items', [])],
            tender_period_end=tender.get('tenderPeriod', {}).get('endDate'),
            enquiry_period_end=tender.get('enquiryPeriod', {}).get('endDate'),
            submission_method=tender.get('submissionMethodDetails'),
            award_criteria=award_criteria,
            buyer_name=data.get('buyer', {}).get('name'),
            buyer_id=data.get('buyer', {}).get('id'),
            published_date=data.get('date'),
            last_edited_date=None  # Set from amendments if needed
        )

    def validate(self) -> List[str]:
        """Validate the notice data"""
        errors = []
        required_fields = [
            'notice_identifier', 'procurement_identifier', 'tender_title',
            'tender_description', 'tender_value_amount', 'procurement_method'
        ]
        
        for field in required_fields:
            if not getattr(self, field):
                errors.append(f"Missing required field: {field}")
                
        return errors
