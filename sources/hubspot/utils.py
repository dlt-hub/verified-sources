from .settings import PREPROCESSING

def split_data(doc):
    for prop in PREPROCESSING["split"]:
        if (prop in doc) and (doc[prop] is not None):
            doc[prop] = doc[prop].split(";")
        return doc