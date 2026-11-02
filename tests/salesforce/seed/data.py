"""Records seeded into the Salesforce test org, shared by the seed script and the tests.

`DltTypes__c` has a field of every Salesforce type. Its rows carry values that commonly break
CSV parsing and type mapping: null-like and number-like text, quotes, line breaks, unicode,
boundary numbers, dates and times.
"""

from datetime import date, datetime, time, timezone  # noqa: I251
from typing import Any, Dict, List

from dlt.common import Decimal

API_VERSION = "59.0"
DLT_TYPES = "DltTypes__c"
SEED_ID_FIELD = "Dlt_Seed_Id__c"
PERMISSION_SET = "Dlt_Seed"
# standard objects get an external id so records are upserted in place on each seed
SEED_ID_OBJECTS = [
    "Account",
    "Contact",
    "Lead",
    "Opportunity",
    "Campaign",
    "Product2",
    "Pricebook2",
    "PricebookEntry",
    "Case",
    "OpportunityLineItem",
]
# Pricebook2 reference resolved by the seed script
STANDARD_PRICEBOOK = "standard"

PICKLIST_VALUES = ["Alpha", "Beta", "Gamma delta", "Ω omega"]
# max long text length of 3-byte UTF-8 characters, 6 such fields make a CSV row over 2 MiB. a row
# longer than two parse blocks of 1 MiB fails pyarrow
HUGE_TEXT = ("漢" * 60 + '"x"\n') * 2047 + "漢" * 64
HUGE_TEXT_FIELDS = ["LongText__c"] + [f"LongText{n}__c" for n in range(2, 7)]

# custom fields of DltTypes__c as Metadata API CustomField elements
DLT_TYPES_FIELDS: List[Dict[str, Any]] = [
    {
        "fullName": "Seed_Id__c",
        "label": "Seed Id",
        "type": "Text",
        "length": 64,
        "externalId": True,
        "unique": True,
    },
    {"fullName": "Category__c", "label": "Category", "type": "Text", "length": 40},
    {"fullName": "Text__c", "label": "Text", "type": "Text", "length": 255},
    {"fullName": "TextArea__c", "label": "Text Area", "type": "TextArea"},
    *(
        {
            "fullName": name,
            "label": name[:-3].replace("LongText", "Long Text ").strip(),
            "type": "LongTextArea",
            "length": 131072,
            "visibleLines": 3,
        }
        for name in HUGE_TEXT_FIELDS
    ),
    {
        "fullName": "RichText__c",
        "label": "Rich Text",
        "type": "Html",
        "length": 32768,
        "visibleLines": 10,
    },
    {
        "fullName": "Checkbox__c",
        "label": "Checkbox",
        "type": "Checkbox",
        "defaultValue": False,
    },
    {
        "fullName": "Number18__c",
        "label": "Number 18",
        "type": "Number",
        "precision": 18,
        "scale": 0,
    },
    {
        "fullName": "Number16_2__c",
        "label": "Number 16 2",
        "type": "Number",
        "precision": 16,
        "scale": 2,
    },
    {
        "fullName": "Currency__c",
        "label": "Currency",
        "type": "Currency",
        "precision": 16,
        "scale": 2,
    },
    {
        "fullName": "Percent__c",
        "label": "Percent",
        "type": "Percent",
        "precision": 5,
        "scale": 2,
    },
    {"fullName": "Date__c", "label": "Date", "type": "Date"},
    {"fullName": "DateTime__c", "label": "Date Time", "type": "DateTime"},
    {"fullName": "Time__c", "label": "Time", "type": "Time"},
    {
        "fullName": "Picklist__c",
        "label": "Picklist",
        "type": "Picklist",
        "valueSet": {
            "restricted": True,
            "valueSetDefinition": {
                "sorted": False,
                "value": [
                    {"fullName": v, "default": False, "label": v}
                    for v in PICKLIST_VALUES
                ],
            },
        },
    },
    {
        "fullName": "MultiPicklist__c",
        "label": "Multi Picklist",
        "type": "MultiselectPicklist",
        "visibleLines": 4,
        "valueSet": {
            "restricted": True,
            "valueSetDefinition": {
                "sorted": False,
                "value": [
                    {"fullName": v, "default": False, "label": v}
                    for v in PICKLIST_VALUES
                ],
            },
        },
    },
    {"fullName": "Email__c", "label": "Email", "type": "Email"},
    {"fullName": "Phone__c", "label": "Phone", "type": "Phone"},
    {"fullName": "Url__c", "label": "Url", "type": "Url"},
    {
        "fullName": "Location__c",
        "label": "Location",
        "type": "Location",
        "displayLocationInDecimal": True,
        "scale": 6,
    },
    {
        "fullName": "Account__c",
        "label": "Account",
        "type": "Lookup",
        "referenceTo": "Account",
        "relationshipName": "Dlt_Types",
        "relationshipLabel": "dlt Types",
        "deleteConstraint": "SetNull",
    },
    {
        "fullName": "AutoNumber__c",
        "label": "Auto Number",
        "type": "AutoNumber",
        "displayFormat": "DT-{0000}",
    },
    {
        "fullName": "FormulaNumber__c",
        "label": "Formula Number",
        "type": "Number",
        "precision": 18,
        "scale": 2,
        "formula": "Currency__c * 2",
        "formulaTreatBlanksAs": "BlankAsBlank",
    },
    {
        "fullName": "FormulaText__c",
        "label": "Formula Text",
        "type": "Text",
        "formula": 'Text__c & "!"',
        "formulaTreatBlanksAs": "BlankAsBlank",
    },
    {
        "fullName": "FormulaDate__c",
        "label": "Formula Date",
        "type": "Date",
        "formula": "IF(Date__c < DATE(4000, 12, 31), Date__c + 1, Date__c)",
        "formulaTreatBlanksAs": "BlankAsBlank",
    },
    {
        "fullName": "FormulaCheckbox__c",
        "label": "Formula Checkbox",
        "type": "Checkbox",
        "formula": "Currency__c > 0",
    },
    # divisions may return more decimals than the field scale
    {
        "fullName": "FormulaCurrency__c",
        "label": "Formula Currency",
        "type": "Currency",
        "precision": 18,
        "scale": 2,
        "formula": "Currency__c / 3",
        "formulaTreatBlanksAs": "BlankAsBlank",
    },
    {
        "fullName": "FormulaRatio__c",
        "label": "Formula Ratio",
        "type": "Number",
        "precision": 18,
        "scale": 2,
        "formula": "Number16_2__c / 7",
        "formulaTreatBlanksAs": "BlankAsBlank",
    },
]

# values are python types, the seed script serializes them for the REST API
DLT_TYPES_RECORDS: List[Dict[str, Any]] = [
    {
        "Seed_Id__c": "null-like",
        "Name": "null-like text",
        "Category__c": "edge",
        "Text__c": "NA",
        "TextArea__c": "N/A",
        "LongText__c": "NULL",
        "RichText__c": "null",
        "Phone__c": "#N/A",
        "Picklist__c": "Alpha",
    },
    {
        "Seed_Id__c": "type-like",
        "Name": "type-like text",
        "Category__c": "edge",
        "Text__c": "2024-01-01",
        "TextArea__c": "1.0",
        "LongText__c": "true",
        "Phone__c": "0012345",
        "Picklist__c": "Beta",
    },
    {
        "Seed_Id__c": "quoting",
        "Name": "quotes and line breaks",
        "Category__c": "edge",
        "Text__c": "comma, \"double\" and 'single' quotes",
        "TextArea__c": "line 1\nline 2\r\nline 3",
        "LongText__c": ('"quoted", line\n' * 1500) + "end",
        "RichText__c": '<p>bold <b>x</b> &amp; "q"</p>',
        "Picklist__c": "Gamma delta",
    },
    {
        "Seed_Id__c": "unicode",
        "Name": "unicode and whitespace",
        "Category__c": "edge",
        "Text__c": "Zażółć gęślą jaźń 漢字 🙂 مرحبا",
        "TextArea__c": "  leading and trailing  ",
        "LongText__c": "back\\slash \\n not a line break",
        "Url__c": "https://example.com/path?a=1&b=two,three",
        "Email__c": "dlt.seed+tag@example.com",
        "Picklist__c": "Ω omega",
    },
    {
        "Seed_Id__c": "max",
        "Name": "max values",
        "Category__c": "boundary",
        "Checkbox__c": True,
        "Number18__c": 123456789012345678,
        "Number16_2__c": Decimal("12345678901234.56"),
        "Currency__c": Decimal("99999999999999.99"),
        "Percent__c": Decimal("999.99"),
        "Date__c": date(4000, 12, 31),
        "DateTime__c": datetime(4000, 12, 31, tzinfo=timezone.utc),
        "Time__c": time(23, 59, 59, 999000),
        "MultiPicklist__c": "Alpha;Beta;Gamma delta",
        "Location__Latitude__s": Decimal("89.999999"),
        "Location__Longitude__s": Decimal("179.999999"),
    },
    {
        "Seed_Id__c": "min",
        "Name": "min values",
        "Category__c": "boundary",
        "Checkbox__c": False,
        "Number18__c": -123456789012345678,
        "Number16_2__c": Decimal("-0.01"),
        "Currency__c": Decimal("-0.01"),
        "Percent__c": Decimal("-5.5"),
        "Date__c": date(1700, 1, 1),
        "DateTime__c": datetime(1970, 1, 1, tzinfo=timezone.utc),
        "Time__c": time(0, 0),
        "MultiPicklist__c": "Ω omega",
        "Location__Latitude__s": Decimal("-90"),
        "Location__Longitude__s": Decimal("-180"),
        "Account__r": {SEED_ID_FIELD: "acc-1"},
    },
    {
        "Seed_Id__c": "typical",
        "Name": "typical values",
        "Category__c": "plain",
        "Text__c": "typical",
        "Checkbox__c": True,
        "Number18__c": 42,
        "Number16_2__c": Decimal("3.14"),
        "Currency__c": Decimal("1234.5"),
        "Percent__c": Decimal("12.34"),
        "Date__c": date(2024, 2, 29),
        "DateTime__c": datetime(2024, 2, 29, 23, 59, 59, 999000, tzinfo=timezone.utc),
        "Time__c": time(12, 34, 56, 789000),
        "Account__r": {SEED_ID_FIELD: "acc-2"},
    },
    {
        "Seed_Id__c": "blank",
        "Name": "all optional fields blank",
        "Category__c": "plain",
    },
    {
        "Seed_Id__c": "huge",
        "Name": "long text over 2 MiB",
        "Category__c": "boundary",
        **{name: HUGE_TEXT for name in HUGE_TEXT_FIELDS},
    },
]
# rows deleted after each seed so the recycle bin has records for `queryAll`
DELETED_RECORDS = 2
# values Salesforce stores differently than seeded, observed in Bulk API 2.0 results
STORED_AS: Dict[str, Dict[str, Any]] = {
    # numbers are stored as doubles: 17+ significant digits are lost in Salesforce
    "max": {
        "Number18__c": 123456789012345680,
        "Currency__c": Decimal("99999999999999.98"),
    },
    "min": {"Number18__c": -123456789012345680},
    # DateTime fields have second precision, Time fields keep milliseconds
    "typical": {"DateTime__c": datetime(2024, 2, 29, 23, 59, 59, tzinfo=timezone.utc)},
    # leading and trailing whitespace is trimmed
    "unicode": {"TextArea__c": "leading and trailing"},
    # rich text is sanitized
    "quoting": {"RichText__c": "<p>bold <b>x</b> &amp; &quot;q&quot;</p>"},
}

ACCOUNTS: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: "acc-1",
        "Name": "dlt Acme",
        "Industry": "Technology",
        "Type": "Customer - Direct",
        "AnnualRevenue": 1500000,
        "NumberOfEmployees": 120,
        "BillingStreet": "1 Main Street\nSuite 2",
        "BillingCity": "Berlin",
        "BillingCountry": "Germany",
        "Description": 'multi-line\n"quoted" description',
        "Phone": "+49 30 1234567",
        "Website": "https://acme.example.com",
    },
    {
        SEED_ID_FIELD: "acc-2",
        "Name": "dlt Globex, Inc.",
        "Industry": "Manufacturing",
        "AnnualRevenue": 99999999,
        "NumberOfEmployees": 5000,
        "BillingCity": "São Paulo",
    },
    {SEED_ID_FIELD: "acc-3", "Name": "dlt Initech", "Industry": "Finance"},
    {SEED_ID_FIELD: "acc-4", "Name": "dlt Umbrella"},
    {SEED_ID_FIELD: "acc-5", "Name": "dlt Zażółć"},
]
CONTACTS: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: f"con-{n}",
        "FirstName": first,
        "LastName": last,
        "Email": f"contact{n}@example.com",
        "Account": {SEED_ID_FIELD: account},
    }
    for n, (first, last, account) in enumerate(
        [
            ("Ada", "Lovelace", "acc-1"),
            ("Grace", "Hopper", "acc-1"),
            ("Alan", "Turing", "acc-2"),
            ("Edsger", "Dijkstra", "acc-2"),
            ("Barbara", "Liskov", "acc-3"),
            ("Łukasz", "Żółw", "acc-5"),
        ],
        start=1,
    )
]
LEADS: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: f"lead-{n}",
        "FirstName": first,
        "LastName": last,
        "Company": company,
        "Status": "Open - Not Contacted",
    }
    for n, (first, last, company) in enumerate(
        [
            ("Ken", "Thompson", "Bell Labs"),
            ("Dennis", "Ritchie", "Bell Labs"),
            ("Linus", "Torvalds", "Transmeta"),
            ("Margaret", "Hamilton", "MIT"),
        ],
        start=1,
    )
]
CAMPAIGNS: List[Dict[str, Any]] = [
    {SEED_ID_FIELD: "camp-1", "Name": "dlt Launch", "Status": "Planned"},
    {SEED_ID_FIELD: "camp-2", "Name": "dlt Webinar", "Status": "Completed"},
]
PRODUCTS: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: f"prod-{n}",
        "Name": f"dlt Product {n}",
        "ProductCode": f"DLT-{n}",
        "IsActive": True,
    }
    for n in range(1, 4)
]
PRICEBOOKS: List[Dict[str, Any]] = [
    {SEED_ID_FIELD: "pb-1", "Name": "dlt Pricebook", "IsActive": True}
]
# entries in the standard pricebook must exist before entries in a custom one
PRICEBOOK_ENTRIES: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: f"pbe-{book}-{n}",
        "Pricebook2Id"
        if book == STANDARD_PRICEBOOK
        else "Pricebook2": (
            STANDARD_PRICEBOOK if book == STANDARD_PRICEBOOK else {SEED_ID_FIELD: book}
        ),
        "Product2": {SEED_ID_FIELD: f"prod-{n}"},
        "UnitPrice": 100 * n if book == STANDARD_PRICEBOOK else 90 * n,
        "IsActive": True,
    }
    for book in (STANDARD_PRICEBOOK, "pb-1")
    for n in range(1, 4)
]
OPPORTUNITIES: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: f"opp-{n}",
        "Name": f"dlt Deal {n}",
        "StageName": stage,
        "CloseDate": close_date,
        "Amount": amount,
        "Account": {SEED_ID_FIELD: account},
    }
    for n, (stage, close_date, amount, account) in enumerate(
        [
            ("Prospecting", date(2026, 3, 31), 10000, "acc-1"),
            ("Qualification", date(2026, 6, 30), 25000.5, "acc-1"),
            ("Closed Won", date(2025, 12, 31), 99999.99, "acc-2"),
            ("Closed Lost", date(2025, 11, 15), 500, "acc-3"),
            ("Negotiation/Review", date(2026, 1, 15), 0, "acc-4"),
        ],
        start=1,
    )
]
OPPORTUNITY_LINE_ITEMS: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: f"oli-{n}",
        "Opportunity": {SEED_ID_FIELD: opportunity},
        "PricebookEntry": {SEED_ID_FIELD: f"pbe-{STANDARD_PRICEBOOK}-{product}"},
        "Quantity": quantity,
        "UnitPrice": 100 * product,
    }
    for n, (opportunity, product, quantity) in enumerate(
        [("opp-1", 1, 2), ("opp-1", 2, 1), ("opp-2", 3, 5)], start=1
    )
]
# line items deleted on each seed run, on their own and with their opportunity
DELETED_LINE_ITEM_PREFIX = "oli-deleted-"
CASES: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: "case-1",
        "Subject": "dlt broken widget",
        "Status": "New",
        "Origin": "Web",
        "Account": {SEED_ID_FIELD: "acc-1"},
    },
    {
        SEED_ID_FIELD: "case-2",
        "Subject": "dlt question",
        "Status": "Working",
        "Origin": "Email",
        "Account": {SEED_ID_FIELD: "acc-2"},
    },
]
# upserted on the seed id, in order so parents exist before children
UPSERTED: Dict[str, List[Dict[str, Any]]] = {
    "Account": ACCOUNTS,
    "Contact": CONTACTS,
    "Lead": LEADS,
    "Campaign": CAMPAIGNS,
    "Product2": PRODUCTS,
    "Pricebook2": PRICEBOOKS,
    "Opportunity": OPPORTUNITIES,
    "Case": CASES,
}
# created when missing, matched on the key field
CREATED: Dict[str, Dict[str, Any]] = {
    "PricebookEntry": {"key": SEED_ID_FIELD, "records": PRICEBOOK_ENTRIES},
    "OpportunityLineItem": {"key": SEED_ID_FIELD, "records": OPPORTUNITY_LINE_ITEMS},
    "UserRole": {
        "key": "DeveloperName",
        "records": [
            {"DeveloperName": "Dlt_Sales", "Name": "dlt Sales"},
            {"DeveloperName": "Dlt_Support", "Name": "dlt Support"},
        ],
    },
    "Task": {
        "key": "Subject",
        "records": [
            {"Subject": f"dlt task {n}", "Status": "Not Started", "Priority": "Normal"}
            for n in range(1, 4)
        ],
    },
    "Event": {
        "key": "Subject",
        "records": [
            {
                "Subject": f"dlt event {n}",
                "StartDateTime": datetime(2026, 1, n, 10, tzinfo=timezone.utc),
                "EndDateTime": datetime(2026, 1, n, 11, tzinfo=timezone.utc),
            }
            for n in range(1, 3)
        ],
    },
}

# many slim records read in several result pages. data storage of a Developer Edition org is 5 MB
PAGES = "DltPages__c"
PAGES_FIELDS: List[Dict[str, Any]] = [
    {
        "fullName": "Seed_Id__c",
        "label": "Seed Id",
        "type": "Text",
        "length": 64,
        "externalId": True,
        "unique": True,
    },
    {
        "fullName": "Value__c",
        "label": "Value",
        "type": "Number",
        "precision": 9,
        "scale": 0,
    },
]
PAGES_RECORDS: List[Dict[str, Any]] = [
    {"Seed_Id__c": f"page-{n:05d}", "Name": f"row {n}", "Value__c": n}
    for n in range(1, 1001)
]
CUSTOM_OBJECTS: Dict[str, Dict[str, Any]] = {
    DLT_TYPES: {"label": "dlt Types", "fields": DLT_TYPES_FIELDS},
    PAGES: {"label": "dlt Pages", "fields": PAGES_FIELDS},
}
