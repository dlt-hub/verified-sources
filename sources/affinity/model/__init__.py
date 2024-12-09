# generated by datamodel-codegen:
#   filename:  2024-12-03.json
#   timestamp: 2024-12-03T16:11:26+00:00

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, EmailStr, Field, RootModel, constr, model_serializer
from typing_extensions import Annotated


class BadRequestError(BaseModel):
    code: Annotated[Literal['bad-request'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class ConflictError(BaseModel):
    code: Annotated[Literal['conflict'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class MethodNotAllowedError(BaseModel):
    code: Annotated[Literal['method-not-allowed'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class NotAcceptableError(BaseModel):
    code: Annotated[Literal['not-acceptable'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class NotImplementedError(BaseModel):
    code: Annotated[Literal['not-implemented'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class RateLimitError(BaseModel):
    code: Annotated[Literal['rate-limit'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class ServerError(BaseModel):
    code: Annotated[Literal['server'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class UnprocessableEntityError(BaseModel):
    code: Annotated[Literal['unprocessable-entity'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class UnsupportedMediaTypeError(BaseModel):
    code: Annotated[Literal['unsupported-media-type'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class Tenant(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The tenant's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[
        str, Field(description='The name of the tenant', examples=['Contoso Ltd.'])
    ]
    subdomain: Annotated[
        constr(
            pattern=r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z0-9])$'
        ),
        Field(
            description="The tenant's subdomain under affinity.co", examples=['contoso']
        ),
    ]


class User(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The user's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    firstName: Annotated[
        str, Field(description="The user's first name", examples=['John'])
    ]
    lastName: Annotated[
        Optional[str], Field(description="The user's last name", examples=['Smith'])
    ] = None
    emailAddress: Annotated[
        EmailStr,
        Field(
            description="The user's email address", examples=['john.smith@contoso.com']
        ),
    ]


class Grant(BaseModel):
    type: Annotated[
        Literal['api-key'],
        Field(
            description='The type of grant used to authenticate', examples=['api-key']
        ),
    ]
    scopes: Annotated[
        List[str],
        Field(
            description='The scopes available to the current grant', examples=[['api']]
        ),
    ]
    createdAt: Annotated[
        datetime,
        Field(
            description='When the grant was created', examples=['2023-01-01T00:00:00Z']
        ),
    ]


class WhoAmI(BaseModel):
    tenant: Tenant
    user: User
    grant: Grant


class AuthenticationError(BaseModel):
    code: Annotated[Literal['authentication'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class AuthenticationErrors(BaseModel):
    errors: Annotated[
        List[AuthenticationError], Field(description='AuthenticationError errors')
    ]


class NotFoundError(BaseModel):
    code: Annotated[Literal['not-found'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class NotFoundErrors(BaseModel):
    errors: Annotated[List[NotFoundError], Field(description='NotFoundError errors')]


class CompanyData(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The company's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[str, Field(description="The company's name", examples=['Acme'])]
    domain: Annotated[
        Optional[
            constr(
                pattern=r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z0-9])$'
            )
        ],
        Field(description="The company's primary domain", examples=['acme.co']),
    ] = None


class CompaniesValue(BaseModel):
    type: Annotated[Literal['company-multi'], Field(description='The type of value')]
    data: Annotated[
        List[CompanyData], Field(description='The values for many companies')
    ]


class CompanyValue(BaseModel):
    type: Annotated[Literal['company'], Field(description='The type of value')]
    data: Optional[CompanyData] = None


class DateValue(BaseModel):
    type: Annotated[Literal['datetime'], Field(description='The type of value')]
    data: Annotated[
        Optional[datetime], Field(description='The value for a date')
    ] = None


class Dropdown(BaseModel):
    dropdownOptionId: Annotated[
        int,
        Field(
            description="Dropdown item's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    text: Annotated[str, Field(description='Dropdown item text', examples=['first'])]


class DropdownValue(BaseModel):
    type: Annotated[Literal['dropdown'], Field(description='The type of value')]
    data: Optional[Dropdown] = None


class DropdownsValue(BaseModel):
    type: Annotated[Literal['dropdown-multi'], Field(description='The type of value')]
    data: Annotated[
        Optional[List[Dropdown]], Field(description='The value for many dropdown items')
    ]


class FloatValue(BaseModel):
    type: Annotated[Literal['number'], Field(description='The type of value')]
    data: Annotated[Optional[float], Field(description='The value for a number')] = None


class FloatsValue(BaseModel):
    type: Annotated[Literal['number-multi'], Field(description='The type of value')]
    data: Annotated[List[float], Field(description='The value for many numbers')]


class FormulaNumber(BaseModel):
    calculatedValue: Annotated[
        Optional[float], Field(description='Calculated value')
    ] = None


class FormulaValue(BaseModel):
    type: Annotated[Literal['formula-number'], Field(description='The type of value')]
    data: Optional[FormulaNumber] = None


class Type(Enum):
    internal = 'internal'
    external = 'external'
    collaborator = 'collaborator'


class PersonData(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The persons's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    firstName: Annotated[
        Optional[str], Field(description="The person's first name", examples=['Jane'])
    ] = None
    lastName: Annotated[
        Optional[str], Field(description="The person's last name", examples=['Doe'])
    ] = None
    primaryEmailAddress: Annotated[
        Optional[EmailStr],
        Field(
            description="The person's primary email address",
            examples=['jane.doe@acme.co'],
        ),
    ] = None
    type: Annotated[Type, Field(description="The person's type", examples=['internal'])]


class Direction(Enum):
    received = 'received'
    sent = 'sent'


class ChatMessage(BaseModel):
    type: Annotated[
        Literal['chat-message'],
        Field(description='The type of interaction', examples=['chat-message']),
    ]
    id: Annotated[
        int,
        Field(
            description="The chat message's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    direction: Annotated[
        Direction,
        Field(description='The direction of the chat message', examples=['outbound']),
    ]
    sentAt: Annotated[
        datetime,
        Field(
            description='The time the chat message was sent',
            examples=['2023-01-01T00:00:00Z'],
        ),
    ]
    manualCreator: PersonData
    participants: Annotated[
        List[PersonData], Field(description='The participants of the chat')
    ]


class Attendee(BaseModel):
    emailAddress: Annotated[
        Optional[EmailStr],
        Field(
            description='The email addresses of the attendee',
            examples=['john.smith@contoso.com'],
        ),
    ] = None
    person: Optional[PersonData] = None

    @model_serializer
    def ser_model(self):
        return {'emailAddress': self.emailAddress, 'person_id': getattr(self.person, 'id', None)}


class Email(BaseModel):
    type: Annotated[
        Literal['email'],
        Field(description='The type of interaction', examples=['email']),
    ]
    id: Annotated[
        int,
        Field(
            description="The email's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    subject: Annotated[
        Optional[str],
        Field(description='The subject of the email', examples=['Acme Upsell $10k']),
    ] = None
    sentAt: Annotated[
        datetime,
        Field(
            description='The time the email was sent', examples=['2023-01-01T00:00:00Z']
        ),
    ]
    from_: Annotated[Attendee, Field(alias='from')]
    to: Annotated[List[Attendee], Field(description='The recipients of the email')]
    cc: Annotated[List[Attendee], Field(description='The cc recipients of the email')]


class Meeting(BaseModel):
    type: Annotated[
        Literal['meeting'],
        Field(description='The type of interaction', examples=['meeting']),
    ]
    id: Annotated[
        int,
        Field(
            description="The meeting's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    title: Annotated[
        Optional[str],
        Field(description="The meeting's title", examples=['Acme Upsell $10k']),
    ] = None
    allDay: Annotated[
        bool,
        Field(description='Whether the meeting is an all-day event', examples=[False]),
    ]
    startTime: Annotated[
        datetime,
        Field(description='The meeting start time', examples=['2023-02-03T04:00:00Z']),
    ]
    endTime: Annotated[
        Optional[datetime],
        Field(description='The meeting end time', examples=['2023-02-03T05:00:00Z']),
    ] = None
    attendees: Annotated[
        List[Attendee], Field(description='People attending the meeting')
    ]


class PhoneCall(BaseModel):
    type: Annotated[
        Literal['call'], Field(description='The type of interaction', examples=['call'])
    ]
    id: Annotated[
        int,
        Field(
            description="The phon_call's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    startTime: Annotated[
        datetime,
        Field(description='The call start time', examples=['2023-02-03T04:00:00Z']),
    ]
    attendees: Annotated[List[Attendee], Field(description='People attending the call')]


class Interaction(RootModel[Union[ChatMessage, Email, Meeting, PhoneCall]]):
    root: Annotated[
        Union[ChatMessage, Email, Meeting, PhoneCall], Field(discriminator='type')
    ]


class InteractionValue(BaseModel):
    type: Annotated[Literal['interaction'], Field(description='The type of value')]
    data: Optional[Interaction] = None


class Location(BaseModel):
    streetAddress: Annotated[
        Optional[str],
        Field(description='Street address', examples=['170 Columbus Ave']),
    ] = None
    city: Annotated[
        Optional[str], Field(description='City', examples=['San Francisco'])
    ] = None
    state: Annotated[
        Optional[str], Field(description='State', examples=['California'])
    ] = None
    country: Annotated[
        Optional[str], Field(description='Country', examples=['United States'])
    ] = None
    continent: Annotated[
        Optional[str], Field(description='Continent', examples=['North America'])
    ] = None


class LocationValue(BaseModel):
    type: Annotated[Literal['location'], Field(description='The type of value')]
    data: Optional[Location] = None


class LocationsValue(BaseModel):
    type: Annotated[Literal['location-multi'], Field(description='The type of value')]
    data: Annotated[List[Location], Field(description='The values for many locations')]


class PersonValue(BaseModel):
    type: Annotated[Literal['person'], Field(description='The type of value')]
    data: Optional[PersonData] = None


class PersonsValue(BaseModel):
    type: Annotated[Literal['person-multi'], Field(description='The type of value')]
    data: Annotated[List[PersonData], Field(description='The values for many persons')]


class RankedDropdown(BaseModel):
    dropdownOptionId: Annotated[
        int,
        Field(
            description="Dropdown item's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    text: Annotated[str, Field(description='Dropdown item text', examples=['first'])]
    rank: Annotated[
        int,
        Field(
            description='Dropdown item rank', examples=[0], ge=0, le=9007199254740991
        ),
    ]
    color: Annotated[
        Optional[str], Field(description='Dropdown item color', examples=['white'])
    ] = None


class RankedDropdownValue(BaseModel):
    type: Annotated[Literal['ranked-dropdown'], Field(description='The type of value')]
    data: Optional[RankedDropdown] = None


class Type1(Enum):
    filterable_text = 'filterable-text'
    text = 'text'

class LinkedInEntry(BaseModel):
    link: Annotated[str, Field()]
    text: Annotated[str, Field()]

class TextValue(BaseModel):
    type: Annotated[
        Literal['filterable-text', 'text'],
        Field(description='The type of value', examples=['filterable-text']),
    ]
    data: Annotated[Optional[str], Field(description='The value for a string')] = None


class TextsValue(BaseModel):
    type: Annotated[
        Literal['filterable-text-multi'], Field(description='The type of value')
    ]
    data: Annotated[Optional[List[Union[str, LinkedInEntry]]], Field(description='The value for many strings')]


class FieldValue(
    RootModel[
        Union[
            CompaniesValue,
            CompanyValue,
            DateValue,
            DropdownValue,
            DropdownsValue,
            FloatValue,
            FloatsValue,
            FormulaValue,
            InteractionValue,
            LocationValue,
            LocationsValue,
            PersonValue,
            PersonsValue,
            RankedDropdownValue,
            TextValue,
            TextsValue,
        ]
    ]
):
    root: Annotated[
        Union[
            CompaniesValue,
            CompanyValue,
            DateValue,
            DropdownValue,
            DropdownsValue,
            FloatValue,
            FloatsValue,
            FormulaValue,
            InteractionValue,
            LocationValue,
            LocationsValue,
            PersonValue,
            PersonsValue,
            RankedDropdownValue,
            TextValue,
            TextsValue,
        ],
        Field(
            discriminator='type',
            examples=[
                {
                    'data': {
                        'continent': 'North America',
                        'country': 'United States',
                        'streetAddress': '170 Columbus Ave',
                        'city': 'San Francisco',
                        'state': 'California',
                    },
                    'type': 'location',
                }
            ],
        ),
    ]


class Type2(Enum):
    enriched = 'enriched'
    global_ = 'global'
    list = 'list'
    relationship_intelligence = 'relationship-intelligence'


class EnrichmentSource(Enum):
    affinity_data = 'affinity-data'
    dealroom = 'dealroom'
    NoneType_None = None


class FieldModel(BaseModel):
    id: Annotated[
        str,
        Field(
            description="The field's unique identifier",
            examples=['affinity-data-location'],
        ),
    ]
    name: Annotated[str, Field(description="The field's name", examples=['Location'])]
    type: Annotated[Type2, Field(description="The field's type", examples=['enriched'])]
    enrichmentSource: Annotated[
        EnrichmentSource,
        Field(
            description='The source of the data in this Field (if it is enriched)',
            examples=['affinity-data'],
        ),
    ]
    value: FieldValue


class Company(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The company's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[str, Field(description="The company's name", examples=['Acme'])]
    domain: Annotated[
        Optional[
            constr(
                pattern=r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z0-9])$'
            )
        ],
        Field(description="The company's primary domain", examples=['acme.co']),
    ] = None
    domains: Annotated[
        List[
            constr(
                pattern=r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]{0,61}[A-Za-z0-9])$'
            )
        ],
        Field(description="All of the company's domains", examples=[['acme.co']]),
    ]
    isGlobal: Annotated[
        bool,
        Field(
            description='Whether or not the company is org specific', examples=[True]
        ),
    ]
    fields: Annotated[
        Optional[List[FieldModel]],
        Field(description='The fields associated with the company'),
    ] = None


class Pagination(BaseModel):
    prevUrl: Annotated[
        Optional[str],
        Field(
            description='URL for the previous page',
            examples=['https://api.affinity.co/v2/foo?cursor=ICAgICAgYmVmb3JlOjo6Nw'],
        ),
    ] = None
    nextUrl: Annotated[
        Optional[str],
        Field(
            description='URL for the next page',
            examples=['https://api.affinity.co/v2/foo?cursor=ICAgICAgIGFmdGVyOjo6NA'],
        ),
    ] = None


class CompanyPaged(BaseModel):
    data: Annotated[List[Company], Field(description='A page of Company results')]
    pagination: Pagination


class ValidationError(BaseModel):
    code: Annotated[Literal['validation'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]
    param: Annotated[str, Field(description='Param the error refers to')]


class ValidationErrors(BaseModel):
    errors: Annotated[
        List[ValidationError], Field(description='ValidationError errors')
    ]


class AuthorizationError(BaseModel):
    code: Annotated[Literal['authorization'], Field(description='Error code')]
    message: Annotated[str, Field(description='Error message')]


class AuthorizationErrors(BaseModel):
    errors: Annotated[
        List[AuthorizationError], Field(description='AuthorizationError errors')
    ]


class ValueType(Enum):
    person = 'person'
    person_multi = 'person-multi'
    company = 'company'
    company_multi = 'company-multi'
    filterable_text = 'filterable-text'
    filterable_text_multi = 'filterable-text-multi'
    number = 'number'
    number_multi = 'number-multi'
    datetime = 'datetime'
    location = 'location'
    location_multi = 'location-multi'
    text = 'text'
    ranked_dropdown = 'ranked-dropdown'
    dropdown = 'dropdown'
    dropdown_multi = 'dropdown-multi'
    formula_number = 'formula-number'
    interaction = 'interaction'


class FieldMetadata(BaseModel):
    id: Annotated[
        str,
        Field(
            description="The field's unique identifier",
            examples=['affinity-data-location'],
        ),
    ]
    name: Annotated[str, Field(description="The field's name", examples=['Location'])]
    type: Annotated[Type2, Field(description="The field's type", examples=['enriched'])]
    enrichmentSource: Annotated[
        EnrichmentSource,
        Field(
            description='The source of the data in this Field (if it is enriched)',
            examples=['affinity-data'],
        ),
    ]
    valueType: Annotated[
        ValueType,
        Field(description='The type of the data in this Field', examples=['location']),
    ]


class FieldMetadataPaged(BaseModel):
    data: Annotated[
        List[FieldMetadata], Field(description='A page of FieldMetadata results')
    ]
    pagination: Pagination


class ListModel(BaseModel):
    id: Annotated[
        int,
        Field(
            description='The unique identifier for the list',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[
        str, Field(description='The name of the list', examples=['All companies'])
    ]
    creatorId: Annotated[
        int,
        Field(
            description='The ID of the user that created this list',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    ownerId: Annotated[
        int,
        Field(
            description='The ID of the user that owns this list',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    isPublic: Annotated[
        bool, Field(description='Whether or not the list is public', examples=[False])
    ]


class ListPaged(BaseModel):
    data: Annotated[List[ListModel], Field(description='A page of List results')]
    pagination: Pagination


class ListEntry(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The list entry's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    listId: Annotated[
        int,
        Field(
            description='The ID of the list that this list entry belongs to',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    createdAt: Annotated[
        datetime,
        Field(
            description='The date that the list entry was created',
            examples=['2023-01-01T00:00:00Z'],
        ),
    ]
    creatorId: Annotated[
        Optional[int],
        Field(
            description='The ID of the user that created this list entry',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ] = None
    fields: Annotated[
        List[FieldModel], Field(description='The fields associated with the list entry')
    ]


class ListEntryPaged(BaseModel):
    data: Annotated[List[ListEntry], Field(description='A page of ListEntry results')]
    pagination: Pagination


class Type4(Enum):
    company = 'company'
    opportunity = 'opportunity'
    person = 'person'


class ListWithType(BaseModel):
    id: Annotated[
        int,
        Field(
            description='The unique identifier for the list',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[
        str, Field(description='The name of the list', examples=['All companies'])
    ]
    creatorId: Annotated[
        int,
        Field(
            description='The ID of the user that created this list',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    ownerId: Annotated[
        int,
        Field(
            description='The ID of the user that owns this list',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    isPublic: Annotated[
        bool, Field(description='Whether or not the list is public', examples=[False])
    ]
    type: Annotated[
        Type4, Field(description='The entity type for this list', examples=['company'])
    ]


class ListWithTypePaged(BaseModel):
    data: Annotated[
        List[ListWithType], Field(description='A page of ListWithType results')
    ]
    pagination: Pagination


class CompanyListEntry(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The list entry's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    type: Annotated[
        Literal['company'],
        Field(description='The entity type for this list entry', examples=['company']),
    ]
    createdAt: Annotated[
        datetime,
        Field(
            description='The date that the list entry was created',
            examples=['2023-01-01T00:00:00Z'],
        ),
    ]
    creatorId: Annotated[
        Optional[int],
        Field(
            description='The ID of the user that created this list entry',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ] = None
    entity: Company


class OpportunityWithFields(BaseModel):
    id: Annotated[
        int,
        Field(
            description='The unique identifier for the opportunity',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[
        str,
        Field(description='The name of the opportunity', examples=['Acme Upsell $10k']),
    ]
    listId: Annotated[
        int,
        Field(
            description='The ID of the list that the opportunity belongs to',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    fields: Annotated[
        Optional[List[FieldModel]],
        Field(description='The fields associated with the opportunity'),
    ] = None


class OpportunityListEntry(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The list entry's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    type: Annotated[
        Literal['opportunity'],
        Field(
            description='The entity type for this list entry', examples=['opportunity']
        ),
    ]
    createdAt: Annotated[
        datetime,
        Field(
            description='The date that the list entry was created',
            examples=['2023-01-01T00:00:00Z'],
        ),
    ]
    creatorId: Annotated[
        Optional[int],
        Field(
            description='The ID of the user that created this list entry',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ] = None
    entity: OpportunityWithFields


class Type5(Enum):
    internal = 'internal'
    external = 'external'


class Person(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The persons's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    firstName: Annotated[
        str, Field(description="The person's first name", examples=['Jane'])
    ]
    lastName: Annotated[
        Optional[str], Field(description="The person's last name", examples=['Doe'])
    ] = None
    primaryEmailAddress: Annotated[
        Optional[EmailStr],
        Field(
            description="The person's primary email address",
            examples=['jane.doe@acme.co'],
        ),
    ] = None
    emailAddresses: Annotated[
        List[EmailStr],
        Field(
            description="All of the person's email addresses",
            examples=[['jane.doe@acme.co', 'janedoe@gmail.com']],
        ),
    ]
    type: Annotated[
        Type5, Field(description="The person's type", examples=['internal'])
    ]
    fields: Annotated[
        Optional[List[FieldModel]],
        Field(description='The fields associated with the person'),
    ] = None


class PersonListEntry(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The list entry's unique identifier",
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    type: Annotated[
        Literal['person'],
        Field(description='The entity type for this list entry', examples=['person']),
    ]
    createdAt: Annotated[
        datetime,
        Field(
            description='The date that the list entry was created',
            examples=['2023-01-01T00:00:00Z'],
        ),
    ]
    creatorId: Annotated[
        Optional[int],
        Field(
            description='The ID of the user that created this list entry',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ] = None
    entity: Person


class ListEntryWithEntity(
    RootModel[Union[CompanyListEntry, OpportunityListEntry, PersonListEntry]]
):
    root: Annotated[
        Union[CompanyListEntry, OpportunityListEntry, PersonListEntry],
        Field(
            discriminator='type',
            examples=[
                {
                    'createdAt': '2023-01-01 00:00:00.000000000 Z',
                    'creatorId': 1,
                    'id': 1,
                    'type': 'company',
                    'entity': {
                        'domain': 'acme.co',
                        'name': 'Acme',
                        'isGlobal': True,
                        'domains': ['acme.co'],
                        'id': 1,
                        'fields': [
                            {
                                'enrichmentSource': 'affinity-data',
                                'name': 'Location',
                                'id': 'affinity-data-location',
                                'type': 'enriched',
                                'value': {
                                    'data': {
                                        'continent': 'North America',
                                        'country': 'United States',
                                        'streetAddress': '170 Columbus Ave',
                                        'city': 'San Francisco',
                                        'state': 'California',
                                    },
                                    'type': 'location',
                                },
                            },
                            {
                                'enrichmentSource': 'affinity-data',
                                'name': 'Location',
                                'id': 'affinity-data-location',
                                'type': 'enriched',
                                'value': {
                                    'data': {
                                        'continent': 'North America',
                                        'country': 'United States',
                                        'streetAddress': '170 Columbus Ave',
                                        'city': 'San Francisco',
                                        'state': 'California',
                                    },
                                    'type': 'location',
                                },
                            },
                        ],
                    },
                }
            ],
        ),
    ]


class ListEntryWithEntityPaged(BaseModel):
    data: Annotated[
        List[ListEntryWithEntity],
        Field(description='A page of ListEntryWithEntity results'),
    ]
    pagination: Pagination


class Type6(Enum):
    sheet = 'sheet'
    board = 'board'
    dashboard = 'dashboard'


class SavedView(BaseModel):
    id: Annotated[
        int,
        Field(
            description="The saved view's unique identifier",
            examples=[28],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[
        str,
        Field(
            description="The saved view's name", examples=['my interesting companies']
        ),
    ]
    type: Annotated[
        Type6, Field(description='The type for this saved view', examples=['sheet'])
    ]
    createdAt: Annotated[
        datetime,
        Field(
            description='The date that the saved view was created',
            examples=['2023-01-01T00:00:00Z'],
        ),
    ]


class SavedViewPaged(BaseModel):
    data: Annotated[List[SavedView], Field(description='A page of SavedView results')]
    pagination: Pagination


class Opportunity(BaseModel):
    id: Annotated[
        int,
        Field(
            description='The unique identifier for the opportunity',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]
    name: Annotated[
        str,
        Field(description='The name of the opportunity', examples=['Acme Upsell $10k']),
    ]
    listId: Annotated[
        int,
        Field(
            description='The ID of the list that the opportunity belongs to',
            examples=[1],
            ge=1,
            le=9007199254740991,
        ),
    ]


class OpportunityPaged(BaseModel):
    data: Annotated[
        List[Opportunity], Field(description='A page of Opportunity results')
    ]
    pagination: Pagination


class PersonPaged(BaseModel):
    data: Annotated[List[Person], Field(description='A page of Person results')]
    pagination: Pagination
