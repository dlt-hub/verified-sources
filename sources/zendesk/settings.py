"""Zendesk source settings and constants"""

from dlt.common import pendulum

DEFAULT_START_DATE = pendulum.datetime(year=2000, month=1, day=1)


CUSTOM_FIELDS_STATE_KEY = "ticket_custom_fields_v2"

# Tuples of (Resource name, endpoint URL, data_key)
# data_key is the key which data list is nested under in responses
# if the data key is None it is assumed to be the same as the resource name
SUPPORT_EXTRA_ENDPOINTS = [
    ("activities", "/api/v2/activities.json", None),
    ("automations", "/api/v2/automations.json", None),
    ("custom_agent_roles", "/api/v2/custom_roles.json", "custom_roles"),
    ("dynamic_content", "/api/v2/dynamic_content/items.json", "items"),
    ("group_memberships", "/api/v2/group_memberships.json", None),
    ("job_status", "/api/v2/job_statuses.json", "job_statuses"),
    ("macros", "/api/v2/macros.json", None),
    ("organization_fields", "/api/v2/organization_fields.json", None),
    ("organization_memberships", "/api/v2/organization_memberships.json", None),
    ("recipient_addresses", "/api/v2/recipient_addresses.json", None),
    ("requests", "/api/v2/requests.json", None),
    ("satisfaction_ratings", "/api/v2/satisfaction_ratings.json", None),
    ("sharing_agreements", "/api/v2/sharing_agreements.json", None),
    ("skips", "/api/v2/skips.json", None),
    ("suspended_tickets", "/api/v2/suspended_tickets.json", None),
    ("targets", "/api/v2/targets.json", None),
    ("ticket_forms", "/api/v2/ticket_forms.json", None),
    ("ticket_metrics", "/api/v2/ticket_metrics.json", None),
    ("triggers", "/api/v2/triggers.json", None),
    ("user_fields", "/api/v2/user_fields.json", None),
    ("views", "/api/v2/views.json", None),
    ("tags", "/api/v2/tags.json", None),
]


TALK_ENDPOINTS = {
    "calls": "/api/v2/channels/voice/calls",
    "addresses": "/api/v2/channels/voice/addresses",
    "greeting_categories": "/api/v2/channels/voice/greeting_categories",
    "greetings": "/api/v2/channels/voice/greetings",
    "ivrs": "/api/v2/channels/voice/ivr",
    "phone_numbers": "/api/v2/channels/voice/phone_numbers",
    "settings": "/api/v2/channels/voice/settings",
    "lines": "/api/v2/channels/voice/lines",
    "agents_activity": "/api/v2/channels/voice/stats/agents_activity",
    "current_queue_activity": "/api/v2/channels/voice/stats/current_queue_activity",
}
INCREMENTAL_ENDPOINTS = {
    "calls": "/api/v2/channels/voice/stats/incremental/calls.json",
    "legs": "/api/v2/channels/voice/stats/incremental/legs.json",
}

possible_endpoints = {
    "availabilities": "/api/v2/channels/voice/availabilities",
    "recordings": "/api/v2/channels/voice/calls/{call_id}/recordings",
    "digital_lines": "/api/v2/channels/voice/digital_lines",
    "agents_overview": "/api/v2/channels/voice/stats/agents_overview",
    "account_overview": "/api/v2/channels/voice/stats/account_overview",
}
