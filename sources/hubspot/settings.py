"""Hubspot source settings and constants"""
from typing import Dict
from dlt.common import pendulum
from dlt.common.data_types import TDataType

HUBSPOT_CREATION_DATE = pendulum.datetime(year=2006, month=6, day=1)
STARTDATE = pendulum.datetime(year=2024, month=2, day=10)

CRM_CONTACTS_ENDPOINT = "/crm/v3/objects/contacts"
CRM_COMPANIES_ENDPOINT = "/crm/v3/objects/companies"
CRM_DEALS_ENDPOINT = "/crm/v3/objects/deals"
CRM_PRODUCTS_ENDPOINT = "/crm/v3/objects/products"
CRM_TICKETS_ENDPOINT = "/crm/v3/objects/tickets"
CRM_QUOTES_ENDPOINT = "/crm/v3/objects/quotes"
CRM_OWNERS_ENDPOINT = "/crm/v3/owners/"
CRM_PROPERTIES_ENDPOINT = "/crm/v3/properties/{objectType}/{property_name}"
CRM_PIPELINES_ENDPOINT = "/crm/v3/pipelines/{objectType}"

CRM_SEARCH_ENDPOINT = "{crm_endpoint}/search"
CRM_ASSOCIATIONS_ENDPOINT = "{crm_endpoint}/{object_id}/associations/{association}"

CRM_OBJECT_ENDPOINTS = {
    "contact": CRM_CONTACTS_ENDPOINT,
    "company": CRM_COMPANIES_ENDPOINT,
    "deal": CRM_DEALS_ENDPOINT,
    "product": CRM_PRODUCTS_ENDPOINT,
    "ticket": CRM_TICKETS_ENDPOINT,
    "quote": CRM_QUOTES_ENDPOINT,
    "owner": CRM_OWNERS_ENDPOINT,
}

CRM_OBJECT_ASSOCIATIONS = {
    "contact": ["deals", "products", "tickets", "quotes"],
    "company": ["contacts", "deals", "products", "tickets", "quotes"],
    "deal": [],
    "product": [],
    "ticket": [],
    "quote": [],
    "owner": [],
}

WEB_ANALYTICS_EVENTS_ENDPOINT = "/events/v3/events?objectType={objectType}&objectId={objectId}&occurredAfter={occurredAfter}&occurredBefore={occurredBefore}&sort=-occurredAt"

OBJECT_TYPE_SINGULAR = {
    "companies": "company",
    "contacts": "contact",
    "deals": "deal",
    "tickets": "ticket",
    "products": "product",
    "quotes": "quote",
}

OBJECT_TYPE_PLURAL = {v: k for k, v in OBJECT_TYPE_SINGULAR.items()}
ALL_OBJECTS = OBJECT_TYPE_PLURAL.keys()


DEFAULT_COMPANY_PROPS = [
    "createdate",
    "domain",
    "hs_lastmodifieddate",
    "hs_object_id",
    "name",
]

DEFAULT_CONTACT_PROPS = [
    "createdate",
    "email",
    "firstname",
    "hs_object_id",
    "lastmodifieddate",
    "lastname",
]

DEFAULT_DEAL_PROPS = [
    "amount",
    "closedate",
    "createdate",
    "dealname",
    "dealstage",
    "hs_lastmodifieddate",
    "hs_object_id",
    "pipeline",
]

DEFAULT_TICKET_PROPS = [
    "createdate",
    "content",
    "hs_lastmodifieddate",
    "hs_object_id",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hs_ticket_category",
    "hs_ticket_priority",
    "subject",
]

DEFAULT_PRODUCT_PROPS = [
    "createdate",
    "description",
    "hs_lastmodifieddate",
    "hs_object_id",
    "name",
    "price",
]

DEFAULT_QUOTE_PROPS = [
    "hs_createdate",
    "hs_expiration_date",
    "hs_lastmodifieddate",
    "hs_object_id",
    "hs_public_url_key",
    "hs_status",
    "hs_title",
]

ENTITY_PROPERTIES = {
    "company": DEFAULT_COMPANY_PROPS,
    "contact": DEFAULT_CONTACT_PROPS,
    "deal": DEFAULT_DEAL_PROPS,
    "ticket": DEFAULT_TICKET_PROPS,
    "product": DEFAULT_PRODUCT_PROPS,
    "quote": DEFAULT_QUOTE_PROPS,
}

LAST_MODIFIED_PROPERTY = {
    "company": "hs_lastmodifieddate",
    "contact": "lastmodifieddate",
    "deal": "hs_lastmodifieddate",
    "ticket": "hs_lastmodifieddate",
    "product": "hs_lastmodifieddate",
    "quote": "hs_lastmodifieddate",
}

PIPELINES_OBJECTS = ["deals", "tickets"]
SOFT_DELETE_KEY = "is_deleted"
ARCHIVED_PARAM = {"archived": True}
PREPROCESSING = {"split": ["hs_merged_object_ids"]}
STAGE_PROPERTY_PREFIX = "hs_v2_date_entered_"
MAX_PROPS_LENGTH = 2000
PROPERTIES_WITH_CUSTOM_LABELS = ()

HS_TO_DLT_TYPE: Dict[str, TDataType] = {
    "bool": "bool",
    "enumeration": "text",
    "number": "double",
    "datetime": "timestamp",
    "string": "text",
}
