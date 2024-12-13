from .model.v2 import *
from pydantic import BaseModel


from typing import List


class Error(
    RootModel[
        BadRequestError
        | ConflictError
        | MethodNotAllowedError
        | NotAcceptableError
        | NotImplementedError
        | RateLimitError
        | ServerError
        | UnprocessableEntityError
        | UnsupportedMediaTypeError
    ]
):
    root: Annotated[
        BadRequestError
        | ConflictError
        | MethodNotAllowedError
        | NotAcceptableError
        | NotImplementedError
        | RateLimitError
        | ServerError
        | UnprocessableEntityError
        | UnsupportedMediaTypeError,
        Field(discriminator="code"),
    ]


class Errors(BaseModel):
    errors: List[Error]
