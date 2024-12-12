# generated by datamodel-codegen:
#   filename:  v2_spec.json

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Literal

import MyBaseModel
from pydantic import AnyUrl, Field, RootModel, constr
from typing_extensions import Annotated


class BadRequestError(MyBaseModel):
    code: Literal["bad-request"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class ConflictError(MyBaseModel):
    code: Literal["conflict"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class MethodNotAllowedError(MyBaseModel):
    code: Literal["method-not-allowed"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class NotAcceptableError(MyBaseModel):
    code: Literal["not-acceptable"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class NotImplementedError(MyBaseModel):
    code: Literal["not-implemented"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class RateLimitError(MyBaseModel):
    code: Literal["rate-limit"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class ServerError(MyBaseModel):
    code: Literal["server"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class UnprocessableEntityError(MyBaseModel):
    code: Literal["unprocessable-entity"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class UnsupportedMediaTypeError(MyBaseModel):
    code: Literal["unsupported-media-type"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class Tenant(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The tenant's unique identifier
    """
    name: Annotated[str, Field(examples=["Contoso Ltd."])]
    """
    The name of the tenant
    """
    subdomain: Annotated[
        constr(
            pattern=r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z0-9])$"
        ),
        Field(examples=["contoso"]),
    ]
    """
    The tenant's subdomain under affinity.co
    """


class User(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The user's unique identifier
    """
    firstName: Annotated[str, Field(examples=["John"])]
    """
    The user's first name
    """
    lastName: Annotated[str | None, Field(examples=["Smith"])] = None
    """
    The user's last name
    """
    emailAddress: Annotated[str, Field(examples=["john.smith@contoso.com"])]
    """
    The user's email address
    """


class Grant(MyBaseModel):
    type: Annotated[Literal["api-key"], Field(examples=["api-key"])]
    """
    The type of grant used to authenticate
    """
    scopes: Annotated[List[str], Field(examples=[["api"]])]
    """
    The scopes available to the current grant
    """
    createdAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    When the grant was created
    """


class WhoAmI(MyBaseModel):
    tenant: Tenant
    user: User
    grant: Grant


class AuthenticationError(MyBaseModel):
    code: Literal["authentication"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class AuthenticationErrors(MyBaseModel):
    errors: List[AuthenticationError]
    """
    AuthenticationError errors
    """


class NotFoundError(MyBaseModel):
    code: Literal["not-found"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class NotFoundErrors(MyBaseModel):
    errors: List[NotFoundError]
    """
    NotFoundError errors
    """


class CompanyData(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The company's unique identifier
    """
    name: Annotated[str, Field(examples=["Acme"])]
    """
    The company's name
    """
    domain: Annotated[str | None, Field(examples=["acme.co"])] = None
    """
    The company's primary domain
    """


class CompaniesValue(MyBaseModel):
    type: Literal["company-multi"]
    """
    The type of value
    """
    data: List[CompanyData]
    """
    The values for many companies
    """


class CompanyValue(MyBaseModel):
    type: Literal["company"]
    """
    The type of value
    """
    data: CompanyData | None = None


class DateValue(MyBaseModel):
    type: Literal["datetime"]
    """
    The type of value
    """
    data: datetime | None = None
    """
    The value for a date
    """


class Dropdown(MyBaseModel):
    dropdownOptionId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    Dropdown item's unique identifier
    """
    text: Annotated[str, Field(examples=["first"])]
    """
    Dropdown item text
    """


class DropdownValue(MyBaseModel):
    type: Literal["dropdown"]
    """
    The type of value
    """
    data: Dropdown | None = None


class DropdownsValue(MyBaseModel):
    type: Literal["dropdown-multi"]
    """
    The type of value
    """
    data: List[Dropdown] | None
    """
    The value for many dropdown items
    """


class FloatValue(MyBaseModel):
    type: Literal["number"]
    """
    The type of value
    """
    data: float | None = None
    """
    The value for a number
    """


class FloatsValue(MyBaseModel):
    type: Literal["number-multi"]
    """
    The type of value
    """
    data: List[float]
    """
    The value for many numbers
    """


class FormulaNumber(MyBaseModel):
    calculatedValue: float | None = None
    """
    Calculated value
    """


class FormulaValue(MyBaseModel):
    type: Literal["formula-number"]
    """
    The type of value
    """
    data: FormulaNumber | None = None


class Type(Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"
    COLLABORATOR = "collaborator"


class PersonData(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The persons's unique identifier
    """
    firstName: Annotated[str | None, Field(examples=["Jane"])] = None
    """
    The person's first name
    """
    lastName: Annotated[str | None, Field(examples=["Doe"])] = None
    """
    The person's last name
    """
    primaryEmailAddress: Annotated[
        str | None, Field(examples=["jane.doe@acme.co"])
    ] = None
    """
    The person's primary email address
    """
    type: Annotated[Type, Field(examples=["internal"])]
    """
    The person's type
    """


class Direction(Enum):
    RECEIVED = "received"
    SENT = "sent"


class ChatMessage(MyBaseModel):
    type: Annotated[Literal["chat-message"], Field(examples=["chat-message"])]
    """
    The type of interaction
    """
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The chat message's unique identifier
    """
    direction: Annotated[Direction, Field(examples=["outbound"])]
    """
    The direction of the chat message
    """
    sentAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The time the chat message was sent
    """
    manualCreator: PersonData
    participants: List[PersonData]
    """
    The participants of the chat
    """


class Attendee(MyBaseModel):
    emailAddress: Annotated[
        str | None, Field(examples=["john.smith@contoso.com"])
    ] = None
    """
    The email addresses of the attendee
    """
    person: PersonData | None = None


class Email(MyBaseModel):
    type: Annotated[Literal["email"], Field(examples=["email"])]
    """
    The type of interaction
    """
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The email's unique identifier
    """
    subject: Annotated[str | None, Field(examples=["Acme Upsell $10k"])] = None
    """
    The subject of the email
    """
    sentAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The time the email was sent
    """
    from_: Annotated[Attendee, Field(alias="from")]
    to: List[Attendee]
    """
    The recipients of the email
    """
    cc: List[Attendee]
    """
    The cc recipients of the email
    """


class Meeting(MyBaseModel):
    type: Annotated[Literal["meeting"], Field(examples=["meeting"])]
    """
    The type of interaction
    """
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The meeting's unique identifier
    """
    title: Annotated[str | None, Field(examples=["Acme Upsell $10k"])] = None
    """
    The meeting's title
    """
    allDay: Annotated[bool, Field(examples=[False])]
    """
    Whether the meeting is an all-day event
    """
    startTime: Annotated[datetime, Field(examples=["2023-02-03T04:00:00Z"])]
    """
    The meeting start time
    """
    endTime: Annotated[datetime | None, Field(examples=["2023-02-03T05:00:00Z"])] = None
    """
    The meeting end time
    """
    attendees: List[Attendee]
    """
    People attending the meeting
    """


class PhoneCall(MyBaseModel):
    type: Annotated[Literal["call"], Field(examples=["call"])]
    """
    The type of interaction
    """
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The phon_call's unique identifier
    """
    startTime: Annotated[datetime, Field(examples=["2023-02-03T04:00:00Z"])]
    """
    The call start time
    """
    attendees: List[Attendee]
    """
    People attending the call
    """


class Interaction(RootModel[ChatMessage | Email | Meeting | PhoneCall]):
    root: Annotated[
        ChatMessage | Email | Meeting | PhoneCall, Field(discriminator="type")
    ]


class InteractionValue(MyBaseModel):
    type: Literal["interaction"]
    """
    The type of value
    """
    data: Interaction | None = None


class Location(MyBaseModel):
    streetAddress: Annotated[str | None, Field(examples=["170 Columbus Ave"])] = None
    """
    Street address
    """
    city: Annotated[str | None, Field(examples=["San Francisco"])] = None
    """
    City
    """
    state: Annotated[str | None, Field(examples=["California"])] = None
    """
    State
    """
    country: Annotated[str | None, Field(examples=["United States"])] = None
    """
    Country
    """
    continent: Annotated[str | None, Field(examples=["North America"])] = None
    """
    Continent
    """


class LocationValue(MyBaseModel):
    type: Literal["location"]
    """
    The type of value
    """
    data: Location | None = None


class LocationsValue(MyBaseModel):
    type: Literal["location-multi"]
    """
    The type of value
    """
    data: List[Location]
    """
    The values for many locations
    """


class PersonValue(MyBaseModel):
    type: Literal["person"]
    """
    The type of value
    """
    data: PersonData | None = None


class PersonsValue(MyBaseModel):
    type: Literal["person-multi"]
    """
    The type of value
    """
    data: List[PersonData]
    """
    The values for many persons
    """


class RankedDropdown(MyBaseModel):
    dropdownOptionId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    Dropdown item's unique identifier
    """
    text: Annotated[str, Field(examples=["first"])]
    """
    Dropdown item text
    """
    rank: Annotated[int, Field(examples=[0], ge=0, le=9007199254740991)]
    """
    Dropdown item rank
    """
    color: Annotated[str | None, Field(examples=["white"])] = None
    """
    Dropdown item color
    """


class RankedDropdownValue(MyBaseModel):
    type: Literal["ranked-dropdown"]
    """
    The type of value
    """
    data: RankedDropdown | None = None


class Type1(Enum):
    FILTERABLE_TEXT = "filterable-text"
    TEXT = "text"


class TextValue(MyBaseModel):
    type: Annotated[
        Literal["filterable-text", "text"], Field(examples=["filterable-text"])
    ]
    """
    The type of value
    """
    data: str | None = None
    """
    The value for a string
    """


class LinkedInEntry(MyBaseModel):
    link: str | None = None
    text: str | None = None


class TextsValue(MyBaseModel):
    type: Literal["filterable-text-multi"]
    """
    The type of value
    """
    data: List[LinkedInEntry] | List[str] | None = None
    """
    The value for many strings
    """


class FieldValue(
    RootModel[
        CompaniesValue
        | CompanyValue
        | DateValue
        | DropdownValue
        | DropdownsValue
        | FloatValue
        | FloatsValue
        | FormulaValue
        | InteractionValue
        | LocationValue
        | LocationsValue
        | PersonValue
        | PersonsValue
        | RankedDropdownValue
        | TextValue
        | TextsValue
    ]
):
    root: Annotated[
        CompaniesValue
        | CompanyValue
        | DateValue
        | DropdownValue
        | DropdownsValue
        | FloatValue
        | FloatsValue
        | FormulaValue
        | InteractionValue
        | LocationValue
        | LocationsValue
        | PersonValue
        | PersonsValue
        | RankedDropdownValue
        | TextValue
        | TextsValue,
        Field(
            discriminator="type",
            examples=[
                {
                    "data": {
                        "continent": "North America",
                        "country": "United States",
                        "streetAddress": "170 Columbus Ave",
                        "city": "San Francisco",
                        "state": "California",
                    },
                    "type": "location",
                }
            ],
        ),
    ]


class Type2(Enum):
    ENRICHED = "enriched"
    GLOBAL_ = "global"
    LIST = "list"
    RELATIONSHIP_INTELLIGENCE = "relationship-intelligence"


class EnrichmentSource(Enum):
    AFFINITY_DATA = "affinity-data"
    DEALROOM = "dealroom"
    NONE_TYPE_NONE = None


class FieldModel(MyBaseModel):
    id: Annotated[str, Field(examples=["affinity-data-location"])]
    """
    The field's unique identifier
    """
    name: Annotated[str, Field(examples=["Location"])]
    """
    The field's name
    """
    type: Annotated[Type2, Field(examples=["enriched"])]
    """
    The field's type
    """
    enrichmentSource: Annotated[EnrichmentSource, Field(examples=["affinity-data"])]
    """
    The source of the data in this Field (if it is enriched)
    """
    value: FieldValue


class Company(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The company's unique identifier
    """
    name: Annotated[str, Field(examples=["Acme"])]
    """
    The company's name
    """
    domain: Annotated[str | None, Field(examples=["acme.co"])] = None
    """
    The company's primary domain
    """
    domains: Annotated[List[str], Field(examples=[["acme.co"]])]
    """
    All of the company's domains
    """
    isGlobal: Annotated[bool, Field(examples=[True])]
    """
    Whether or not the company is org specific
    """
    fields: List[FieldModel] | None = None
    """
    The fields associated with the company
    """


class Pagination(MyBaseModel):
    prevUrl: Annotated[
        AnyUrl | None,
        Field(
            examples=["https://api.affinity.co/v2/foo?cursor=ICAgICAgYmVmb3JlOjo6Nw"]
        ),
    ] = None
    """
    URL for the previous page
    """
    nextUrl: Annotated[
        AnyUrl | None,
        Field(
            examples=["https://api.affinity.co/v2/foo?cursor=ICAgICAgIGFmdGVyOjo6NA"]
        ),
    ] = None
    """
    URL for the next page
    """


class CompanyPaged(MyBaseModel):
    data: List[Company]
    """
    A page of Company results
    """
    pagination: Pagination


class ValidationError(MyBaseModel):
    code: Literal["validation"]
    """
    Error code
    """
    message: str
    """
    Error message
    """
    param: str
    """
    Param the error refers to
    """


class ValidationErrors(MyBaseModel):
    errors: List[ValidationError]
    """
    ValidationError errors
    """


class AuthorizationError(MyBaseModel):
    code: Literal["authorization"]
    """
    Error code
    """
    message: str
    """
    Error message
    """


class AuthorizationErrors(MyBaseModel):
    errors: List[AuthorizationError]
    """
    AuthorizationError errors
    """


class ValueType(Enum):
    PERSON = "person"
    PERSON_MULTI = "person-multi"
    COMPANY = "company"
    COMPANY_MULTI = "company-multi"
    FILTERABLE_TEXT = "filterable-text"
    FILTERABLE_TEXT_MULTI = "filterable-text-multi"
    NUMBER = "number"
    NUMBER_MULTI = "number-multi"
    DATETIME = "datetime"
    LOCATION = "location"
    LOCATION_MULTI = "location-multi"
    TEXT = "text"
    RANKED_DROPDOWN = "ranked-dropdown"
    DROPDOWN = "dropdown"
    DROPDOWN_MULTI = "dropdown-multi"
    FORMULA_NUMBER = "formula-number"
    INTERACTION = "interaction"


class FieldMetadata(MyBaseModel):
    id: Annotated[str, Field(examples=["affinity-data-location"])]
    """
    The field's unique identifier
    """
    name: Annotated[str, Field(examples=["Location"])]
    """
    The field's name
    """
    type: Annotated[Type2, Field(examples=["enriched"])]
    """
    The field's type
    """
    enrichmentSource: Annotated[EnrichmentSource, Field(examples=["affinity-data"])]
    """
    The source of the data in this Field (if it is enriched)
    """
    valueType: Annotated[ValueType, Field(examples=["location"])]
    """
    The type of the data in this Field
    """


class FieldMetadataPaged(MyBaseModel):
    data: List[FieldMetadata]
    """
    A page of FieldMetadata results
    """
    pagination: Pagination


class ListModel(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The unique identifier for the list
    """
    name: Annotated[str, Field(examples=["All companies"])]
    """
    The name of the list
    """
    creatorId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the user that created this list
    """
    ownerId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the user that owns this list
    """
    isPublic: Annotated[bool, Field(examples=[False])]
    """
    Whether or not the list is public
    """


class ListPaged(MyBaseModel):
    data: List[ListModel]
    """
    A page of List results
    """
    pagination: Pagination


class ListEntry(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The list entry's unique identifier
    """
    listId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the list that this list entry belongs to
    """
    createdAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The date that the list entry was created
    """
    creatorId: Annotated[
        int | None, Field(examples=[1], ge=1, le=9007199254740991)
    ] = None
    """
    The ID of the user that created this list entry
    """
    fields: List[FieldModel]
    """
    The fields associated with the list entry
    """


class ListEntryPaged(MyBaseModel):
    data: List[ListEntry]
    """
    A page of ListEntry results
    """
    pagination: Pagination


class Type4(Enum):
    COMPANY = "company"
    OPPORTUNITY = "opportunity"
    PERSON = "person"


class ListWithType(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The unique identifier for the list
    """
    name: Annotated[str, Field(examples=["All companies"])]
    """
    The name of the list
    """
    creatorId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the user that created this list
    """
    ownerId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the user that owns this list
    """
    isPublic: Annotated[bool, Field(examples=[False])]
    """
    Whether or not the list is public
    """
    type: Annotated[Type4, Field(examples=["company"])]
    """
    The entity type for this list
    """


class ListWithTypePaged(MyBaseModel):
    data: List[ListWithType]
    """
    A page of ListWithType results
    """
    pagination: Pagination


class CompanyListEntry(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The list entry's unique identifier
    """
    type: Annotated[Literal["company"], Field(examples=["company"])]
    """
    The entity type for this list entry
    """
    createdAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The date that the list entry was created
    """
    creatorId: Annotated[
        int | None, Field(examples=[1], ge=1, le=9007199254740991)
    ] = None
    """
    The ID of the user that created this list entry
    """
    entity: Company


class OpportunityWithFields(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The unique identifier for the opportunity
    """
    name: Annotated[str, Field(examples=["Acme Upsell $10k"])]
    """
    The name of the opportunity
    """
    listId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the list that the opportunity belongs to
    """
    fields: List[FieldModel] | None = None
    """
    The fields associated with the opportunity
    """


class OpportunityListEntry(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The list entry's unique identifier
    """
    type: Annotated[Literal["opportunity"], Field(examples=["opportunity"])]
    """
    The entity type for this list entry
    """
    createdAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The date that the list entry was created
    """
    creatorId: Annotated[
        int | None, Field(examples=[1], ge=1, le=9007199254740991)
    ] = None
    """
    The ID of the user that created this list entry
    """
    entity: OpportunityWithFields


class Type5(Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"


class Person(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The persons's unique identifier
    """
    firstName: Annotated[str, Field(examples=["Jane"])]
    """
    The person's first name
    """
    lastName: Annotated[str | None, Field(examples=["Doe"])] = None
    """
    The person's last name
    """
    primaryEmailAddress: Annotated[
        str | None, Field(examples=["jane.doe@acme.co"])
    ] = None
    """
    The person's primary email address
    """
    emailAddresses: Annotated[
        List[str], Field(examples=[["jane.doe@acme.co", "janedoe@gmail.com"]])
    ]
    """
    All of the person's email addresses
    """
    type: Annotated[Type5, Field(examples=["internal"])]
    """
    The person's type
    """
    fields: List[FieldModel] | None = None
    """
    The fields associated with the person
    """


class PersonListEntry(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The list entry's unique identifier
    """
    type: Annotated[Literal["person"], Field(examples=["person"])]
    """
    The entity type for this list entry
    """
    createdAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The date that the list entry was created
    """
    creatorId: Annotated[
        int | None, Field(examples=[1], ge=1, le=9007199254740991)
    ] = None
    """
    The ID of the user that created this list entry
    """
    entity: Person


class ListEntryWithEntity(
    RootModel[CompanyListEntry | OpportunityListEntry | PersonListEntry]
):
    root: Annotated[
        CompanyListEntry | OpportunityListEntry | PersonListEntry,
        Field(
            discriminator="type",
            examples=[
                {
                    "createdAt": "2023-01-01 00:00:00.000000000 Z",
                    "creatorId": 1,
                    "id": 1,
                    "type": "company",
                    "entity": {
                        "domain": "acme.co",
                        "name": "Acme",
                        "isGlobal": True,
                        "domains": ["acme.co"],
                        "id": 1,
                        "fields": [
                            {
                                "enrichmentSource": "affinity-data",
                                "name": "Location",
                                "id": "affinity-data-location",
                                "type": "enriched",
                                "value": {
                                    "data": {
                                        "continent": "North America",
                                        "country": "United States",
                                        "streetAddress": "170 Columbus Ave",
                                        "city": "San Francisco",
                                        "state": "California",
                                    },
                                    "type": "location",
                                },
                            },
                            {
                                "enrichmentSource": "affinity-data",
                                "name": "Location",
                                "id": "affinity-data-location",
                                "type": "enriched",
                                "value": {
                                    "data": {
                                        "continent": "North America",
                                        "country": "United States",
                                        "streetAddress": "170 Columbus Ave",
                                        "city": "San Francisco",
                                        "state": "California",
                                    },
                                    "type": "location",
                                },
                            },
                        ],
                    },
                }
            ],
        ),
    ]


class ListEntryWithEntityPaged(MyBaseModel):
    data: List[ListEntryWithEntity]
    """
    A page of ListEntryWithEntity results
    """
    pagination: Pagination


class Type6(Enum):
    SHEET = "sheet"
    BOARD = "board"
    DASHBOARD = "dashboard"


class SavedView(MyBaseModel):
    id: Annotated[int, Field(examples=[28], ge=1, le=9007199254740991)]
    """
    The saved view's unique identifier
    """
    name: Annotated[str, Field(examples=["my interesting companies"])]
    """
    The saved view's name
    """
    type: Annotated[Type6, Field(examples=["sheet"])]
    """
    The type for this saved view
    """
    createdAt: Annotated[datetime, Field(examples=["2023-01-01T00:00:00Z"])]
    """
    The date that the saved view was created
    """


class SavedViewPaged(MyBaseModel):
    data: List[SavedView]
    """
    A page of SavedView results
    """
    pagination: Pagination


class Opportunity(MyBaseModel):
    id: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The unique identifier for the opportunity
    """
    name: Annotated[str, Field(examples=["Acme Upsell $10k"])]
    """
    The name of the opportunity
    """
    listId: Annotated[int, Field(examples=[1], ge=1, le=9007199254740991)]
    """
    The ID of the list that the opportunity belongs to
    """


class OpportunityPaged(MyBaseModel):
    data: List[Opportunity]
    """
    A page of Opportunity results
    """
    pagination: Pagination


class PersonPaged(MyBaseModel):
    data: List[Person]
    """
    A page of Person results
    """
    pagination: Pagination
