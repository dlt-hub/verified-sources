from enum import IntEnum
from pydantic import BaseModel, ConfigDict, Field
from typing import ClassVar, List, Annotated
from datetime import datetime

from dlt.common.libs.pydantic import DltConfig


class NoteType(IntEnum):
    PLAIN_TEXT = 0
    HTML = 2
    AI_SUMMARY = 3
    """Can only be created by the Notetaker AI tool from Affinity."""
    EMAIL = 1
    """Deprecated"""


class InteractionType(IntEnum):
    MEETING = 0
    """Type specifying a meeting interaction."""
    CALL = 1
    """Type specifying a call interaction."""
    CHAT_MESSAGE = 2
    """Type specifying a chat message interaction."""
    EMAIL = 3
    """Type specifying an email interaction."""


class Note(
    BaseModel,
):
    dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}
    model_config = ConfigDict(
        json_encoders={InteractionType: lambda x: x.name, NoteType: lambda x: x.name}
    )

    """Represents a note object with metadata and associations."""

    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """The unique identifier of the note object."""

    creator_id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """The unique identifier of the person object who created the note."""

    person_ids: List[int]
    """An array containing the unique identifiers for all the persons relevant to the note.
    This is the union of associated_person_ids and interaction_person_ids."""

    associated_person_ids: List[int]
    """An array containing the unique identifiers for the persons directly associated with the note."""

    interaction_person_ids: List[int]
    """An array containing the unique identifiers for the persons on the interaction the note is attached to, if any."""

    interaction_id: Annotated[
        int | None, Field(examples=[1], ge=1, le=9007199254740991)
    ]
    """The unique identifier of the interaction the note is attached to, if any."""

    interaction_type: InteractionType | None
    """The type of the interaction the note is attached to, if any."""

    is_meeting: bool
    """True if the note is attached to a meeting or a call."""

    mentioned_person_ids: List[
        Annotated[int | None, Field(examples=[1], ge=1, le=9007199254740991)]
    ]
    """An array containing the unique identifiers for the persons who are @ mentioned in the note."""

    organization_ids: List[
        Annotated[int | None, Field(examples=[1], ge=1, le=9007199254740991)]
    ]
    """An array of unique identifiers of organization objects that are associated with the note."""

    opportunity_ids: List[
        Annotated[int | None, Field(examples=[1], ge=1, le=9007199254740991)]
    ]
    """An array of unique identifiers of opportunity objects that are associated with the note."""

    parent_id: Annotated[int | None, Field(examples=[1], ge=1, le=9007199254740991)]
    """The unique identifier of the note that this note is a reply to.
    If this field is null, the note is not a reply."""

    content: str
    """The string containing the content of the note."""

    type: NoteType
    """The type of the note. Supported types for new note creation via API are 0 and 2, representing plain text and HTML notes, respectively."""

    created_at: datetime
    """The string representing the time when the note was created."""

    updated_at: datetime | None
    """The string representing the last time the note was updated."""
