from typing import Any, Iterator, List, Optional, Set, Tuple
from uuid import UUID

from dlt.common import json

TSchemaId = Tuple[str, Any]
"""("id", int) for ids in the payload, ("guid", UUID) for ids in headers"""

VALUE_SCHEMA_ID_HEADER = "__value_schema_id"
KEY_SCHEMA_ID_HEADER = "__key_schema_id"
CONFLUENT_PREFIX_SIZE = 5


def registry_schema(registry: Any, schema_id: TSchemaId) -> Optional[Any]:
    """The `Schema` registered with `schema_id`, `None` when the Schema Registry has none"""
    kind, value = schema_id
    try:
        if kind == "guid":
            return registry.get_schema_by_guid(str(value))
        return registry.get_schema(value)
    except Exception as ex:
        if _is_not_found(ex):
            return None
        raise


def latest_registry_schema(registry: Any, subject: str) -> Optional[Any]:
    """The latest `RegisteredSchema` of `subject`, `None` when the subject does not exist"""
    try:
        return registry.get_latest_version(subject)
    except Exception as ex:
        if _is_not_found(ex):
            return None
        raise


def _is_not_found(ex: Exception) -> bool:
    return bool(getattr(ex, "http_status_code", None) == 404)


def referenced_schemas(
    registry: Any, schema: Any, seen: Optional[Set[Tuple[str, int]]] = None
) -> List[Any]:
    """Schemas the references of `schema` point to, parsed from JSON, each once and before the
    schemas that reference it"""
    seen = set() if seen is None else seen
    schemas: List[Any] = []
    for reference in schema.references or []:
        key = (reference.subject, reference.version)
        if key in seen:
            continue
        seen.add(key)
        registered = registry.get_version(reference.subject, reference.version)
        schemas.extend(referenced_schemas(registry, registered.schema, seen))
        schemas.append(json.loads(registered.schema.schema_str))
    return schemas


def framed_schema_id(data: bytes, headers: Any, header: str) -> Optional[TSchemaId]:
    """Schema id of a key or value: a GUID in the `header`, else the Confluent prefix"""
    for value in iter_header(headers, header):
        guid = parse_guid(value)
        if guid is not None:
            return ("guid", guid)
    if len(data) >= CONFLUENT_PREFIX_SIZE and data[0] == 0:
        return ("id", int.from_bytes(data[1:CONFLUENT_PREFIX_SIZE], "big"))
    return None


def body_start(schema_id: TSchemaId) -> int:
    """Where the body of a framed key or value starts, ids in headers have no prefix"""
    return CONFLUENT_PREFIX_SIZE if schema_id[0] == "id" else 0


def parse_guid(value: bytes) -> Optional[UUID]:
    """GUID of a `__value_schema_id` header: version byte 1 and 16 bytes"""
    if len(value) == 17 and value[0] == 1:
        return UUID(bytes=bytes(value[1:]))
    return None


def iter_header(headers: Any, name: str) -> Iterator[bytes]:
    """Values of the `name` headers of a consumed message"""
    for key, value in headers or ():
        if key == name and isinstance(value, bytes):
            yield value
