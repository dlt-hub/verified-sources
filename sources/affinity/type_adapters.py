from .errors import Errors
from .model.v1 import Note
from .model.v2 import (
    AuthenticationErrors,
    AuthorizationErrors,
    ListEntryWithEntity,
    NotFoundErrors,
    ValidationErrors,
)


from pydantic import TypeAdapter


error_adapter = TypeAdapter(
    AuthenticationErrors
    | NotFoundErrors
    | AuthorizationErrors
    | ValidationErrors
    | Errors
)
list_adapter = TypeAdapter(list[ListEntryWithEntity])
note_adapter = TypeAdapter(list[Note])
