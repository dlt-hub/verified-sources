from typing import Dict, Any
from .settings import PREPROCESSING

def split_data(doc: Dict[str, Any]) -> Dict[str, Any]:
    for prop in PREPROCESSING["split"]:
        if prop in doc and doc[prop] is not None:
            if isinstance(doc[prop], str):
                doc[prop] = doc[prop].split(";")
    return doc
