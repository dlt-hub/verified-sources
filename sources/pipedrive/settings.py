"""Pipedrive source settings and constants"""

ENTITY_MAPPINGS = [
    ("activity", "activityFields", {"user_id": 0}),
    ("organization", "organizationFields", None),
    ("person", "personFields", None),
    ("product", "productFields", None),
    ("deal", "dealFields", None),
    ("pipeline", None, None),
    ("stage", None, None),
    ("user", None, None),
]

RECENTS_ENTITIES = {
    "activity": "activities",
    "activityType": "activity_types",
    "deal": "deals",
    "file": "files",
    "filter": "filters",
    "note": "notes",
    "person": "persons",
    "organization": "organizations",
    "pipeline": "pipelines",
    "product": "products",
    "project": "projects",
    "stage": "stages",
    "task": "tasks",
    "user": "users",
}


"""
Available Pipedrive API v2 endpoints for configuration.

Note: Some endpoints (e.g., followers, deal_products) require nested configuration.
See NESTED_ENTITIES_V2 for examples.

# For more details, see: https://developers.pipedrive.com/docs/api/v2
"""
ENTITIES_V2 = {
    "activities": {},
    "deals": {
        "params": {
            "include_fields": (
                "next_activity_id,last_activity_id,first_won_time,products_count,"
                "files_count,notes_count,followers_count,email_messages_count,"
                "activities_count,done_activities_count,undone_activities_count,"
                "participants_count,last_incoming_mail_time,last_outgoing_mail_time,"
                "smart_bcc_email"
            )
        }
    },
    "persons": {
        "params": {
            "include_fields": (
                "next_activity_id,last_activity_id,open_deals_count,"
                "related_open_deals_count,closed_deals_count,related_closed_deals_count,"
                "participant_open_deals_count,participant_closed_deals_count,"
                "email_messages_count,activities_count,done_activities_count,"
                "undone_activities_count,files_count,notes_count,followers_count,"
                "won_deals_count,related_won_deals_count,lost_deals_count,"
                "related_lost_deals_count,last_incoming_mail_time,last_outgoing_mail_time"
            )
        }
    },
    "organizations": {
        "params": {
            "include_fields": (
                "next_activity_id,last_activity_id,open_deals_count,"
                "related_open_deals_count,closed_deals_count,related_closed_deals_count,"
                "email_messages_count,activities_count,done_activities_count,"
                "undone_activities_count,files_count,notes_count,followers_count,"
                "won_deals_count,related_won_deals_count,lost_deals_count,"
                "related_lost_deals_count"
            )
        }
    },
    "products": {},
    "pipelines": {},
    "stages": {},
}

# Nested V2 API Endpoints Configuration
# Automatically loaded when their parent resource is included in use_v2_endpoints.
NESTED_ENTITIES_V2 = {
    "deal_products": {
        "parent": "deals",
        "endpoint_path": "deals/{id}/products",
        "params": {
            "limit": 500,
        },
    },
    "deal_followers": {
        "parent": "deals",
        "endpoint_path": "deals/{id}/followers",
        "primary_key": ["user_id", "_deals_id"],  # Followers don't have 'id', use composite key
        "include_from_parent": ["id"],  # Include deal id from parent
        "params": {
            "limit": 500,
        },
    },
}

# Default v2 resources to load when none are specified
# This curated set includes the most commonly used endpoints.
# Users can customize this list to match their needs.
# See ENTITIES_V2 above for all available v2 endpoints.
DEFAULT_V2_RESOURCES = ["deals", "persons", "organizations" ,"products", "pipelines", "stages"]
