## UK1: Pipeline notice
    releases>planning>documents>last>noticeType
## UK2: Preliminary market engagement notice
    releases>planning>documents>last>noticeType
## UK3: Planned procurement notice
    releases>planning>documents>last>noticeType
## UK4: Tender notice
    releases>tender>documents>last>noticeType
### Scope
1. Reference - "Tender ID": release.get("tender", {}).get("id", "N/A")
2. Description - "Description": release.get("tender", {}).get("description", "N/A")
3. Total value ex VAT - "Tender Value": release.get("tender", {}).get("value", {}).get("amount", "N/A")
4. Total value inc VAT - "Tender Value": release.get("tender", {}).get("value", {}).get("amountGross", "N/A")
5. Contract Dates
6. Contract Duration
7. Main Procurement Category
8. CPV Classifications
9. Contract Locations
### Suitability
1. Particular Suitability
### Submission
1. Tender submission deadline
2. Submission address and special instructions
3. Electronic submission
### Award Criteria
1. Award criteria
### Procedure
1. Procedure type
### Documents
1. Associated documents
### Contracting Authority
1. Name
2. PPON
3. Address
4. Contact Name
5. Email
6. Region
7. Org type


## UK5: Transparency notice
    releases>awards>documents>last>noticeType
## UK6: Contract award notice
    releases>awards>documents>last>noticeType
## UK7: Contract details notice
    releases>awards>documents>last>noticeType
## UK10: Contract change notice
## UK11: Contract termination notice
## UK12: Procurement termination notice
    releases>tender>documents>last>noticeType
## UK13: Dynamic market intention notice
    releases>tender>documents>last>noticeType
## UK14: Dynamic market establishment notice
## UK15: Dynamic market modification notice
## UK16: Dynamic market cessation notice
