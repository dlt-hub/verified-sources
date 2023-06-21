"""Hubspot source settings and constants"""

from dlt.common import pendulum

STARTDATE = pendulum.datetime(year=2000, month=1, day=1)

CRM_CONTACTS_ENDPOINT = (
    "/crm/v3/objects/contacts?associations=deals,products,tickets,quotes&limit=100"
)
CRM_COMPANIES_ENDPOINT = "/crm/v3/objects/companies?associations=contacts,deals,products,tickets,quotes&limit=100"
CRM_DEALS_ENDPOINT = "/crm/v3/objects/deals?properties={properties}&limit=100"
CRM_PRODUCTS_ENDPOINT = "/crm/v3/objects/products?limit=100"
CRM_TICKETS_ENDPOINT = "/crm/v3/objects/tickets?limit=100"
CRM_QUOTES_ENDPOINT = "/crm/v3/objects/quotes?limit=100"
WEB_ANALYTICS_EVENTS_ENDPOINT = "/events/v3/events?objectType={objectType}&objectId={objectId}&occurredAfter={occurredAfter}&occurredBefore={occurredBefore}&sort=-occurredAt"
