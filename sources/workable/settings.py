# define which endpoints to load
DEFAULT_ENDPOINTS = (
    "members",
    "recruiters",
    "stages",
    "requisitions",
    "jobs",
    "custom_attributes",
    "events",
)

# define which sub endpoints to load for each main endpoint if details
# are requested
DEFAULT_DETAILS = {
    "candidates": (
        "activities",
        "offer",
    ),
    "jobs": (
        "activities",
        "application_form",
        "questions",
        "stages",
        "custom_attributes",
        "members",
        "recruiters",
    ),
}
