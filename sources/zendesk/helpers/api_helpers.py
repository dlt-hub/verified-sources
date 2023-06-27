from typing import Optional, TypedDict, Dict, Union
from dlt.common import pendulum
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem
from dlt.common.time import parse_iso_like_datetime


from .credentials import (
    ZendeskCredentialsToken,
    ZendeskCredentialsEmailPass,
    ZendeskCredentialsOAuth,
    TZendeskCredentials,
)


class TCustomFieldInfo(TypedDict):
    title: str
    options: DictStrStr


def _parse_date_or_none(value: Optional[str]) -> Optional[pendulum.DateTime]:
    if not value:
        return None
    return parse_iso_like_datetime(value)


def parse_iso_or_timestamp(value: Union[str, int, float]) -> pendulum.DateTime:
    if isinstance(value, (int, float)):
        return pendulum.from_timestamp(value)
    return parse_iso_like_datetime(value)


def process_ticket(
    ticket: DictStrAny,
    custom_fields: Dict[str, TCustomFieldInfo],
    pivot_custom_fields: bool = True,
) -> DictStrAny:
    """
    Helper function that processes a ticket object and returns a dictionary of ticket data.

    Args:
        ticket: The ticket dict object returned by a Zendesk API call.
        custom_fields: A dictionary containing all the custom fields available for tickets.
        pivot_custom_fields: A boolean indicating whether to pivot all custom fields or not.
            Defaults to True.

    Returns:
        DictStrAny: A dictionary containing cleaned data about a ticket.
    """
    # pivot custom field if indicated as such
    # get custom fields
    for custom_field in ticket["custom_fields"]:
        if pivot_custom_fields:
            cus_field_id = str(custom_field["id"])
            field = custom_fields[cus_field_id]
            field_name = field["title"]
            current_value = custom_field["value"]
            options = field["options"]
            # Map dropdown values to labels
            if not current_value or not options:
                ticket[field_name] = current_value
            elif isinstance(
                current_value, list
            ):  # Multiple choice field has a list of values
                ticket[field_name] = [options.get(key, key) for key in current_value]
            else:
                ticket[field_name] = options.get(current_value)
        else:
            custom_field["ticket_id"] = ticket["id"]
    # delete fields that are not needed for pivoting
    if pivot_custom_fields:
        del ticket["custom_fields"]
    del ticket["fields"]
    # modify dates to return datetime objects instead
    ticket["updated_at"] = _parse_date_or_none(ticket["updated_at"])
    ticket["created_at"] = _parse_date_or_none(ticket["created_at"])
    ticket["due_at"] = _parse_date_or_none(ticket["due_at"])
    return ticket


def process_ticket_field(
    field: DictStrAny, custom_fields_state: Dict[str, TCustomFieldInfo]
) -> TDataItem:
    """Update custom field mapping in dlt state for the given field."""
    # grab id and update state dict
    # if the id is new, add a new key to indicate that this is the initial value for title
    # New dropdown options are added to existing field but existing options are not changed
    return_dict = field.copy()
    field_id = str(field["id"])

    options = field.get("custom_field_options", [])
    new_options = {o["value"]: o["name"] for o in options}
    existing_field = custom_fields_state.get(field_id)
    if existing_field:
        existing_options = existing_field["options"]
        if return_options := return_dict.get("custom_field_options"):
            for item in return_options:
                item["name"] = existing_options.get(item["value"], item["name"])
        for key, value in new_options.items():
            if key not in existing_options:
                existing_options[key] = value
    else:
        custom_fields_state[field_id] = dict(title=field["title"], options=new_options)
        return_dict["initial_title"] = field["title"]
    return return_dict
