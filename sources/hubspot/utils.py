from typing import Any, Dict, Iterator, List

from .settings import PREPROCESSING


def split_data(doc: Dict[str, Any]) -> Dict[str, Any]:
    for prop in PREPROCESSING["split"]:
        if prop in doc and doc[prop] is not None:
            if isinstance(doc[prop], str):
                doc[prop] = doc[prop].split(";")
    return doc


def chunk_properties(properties: List[str], max_length: int) -> Iterator[List[str]]:
    """Function which yields chunk of properties list, making sure that
    for each chunk, len(",".join(chunk)) =< max_length.
    """
    chunk: List[str] = []
    length = 0
    for prop in properties:
        prop_len = len(prop) + (1 if chunk else 0)  # include comma length if not first
        if length + prop_len > max_length:
            yield chunk
            chunk, length = [prop], len(prop)
        else:
            chunk.append(prop)
            length += prop_len
    if chunk:
        yield chunk
