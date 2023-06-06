"""Zendesk source settings and constants"""

from dlt.common import pendulum

DEFAULT_START_DATE = pendulum.datetime(year=2000, month=1, day=1)

EXTRA_RESOURCES_SUPPORT = [
    "activities",
    "automations",
    "custom_agent_roles",
    "dynamic_content",
    "group_memberships",
    "job_status",
    "macros",
    "organization_fields",
    "organization_memberships",
    "recipient_addresses",
    "requests",
    "satisfaction_ratings",
    "sharing_agreements",
    "skips",
    "suspended_tickets",
    "targets",
    "ticket_forms",
    "ticket_metrics",
    "triggers",
    "user_fields",
    "views",
    "tags",
]

CUSTOM_FIELDS_STATE_KEY = "ticket_custom_fields_v2"

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
