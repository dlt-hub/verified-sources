"""
"""

import datetime
from typing import Any, Iterator, List, Sequence

import dlt
from dlt.common.typing import TDataItem, StrAny
from dlt.extract.source import DltResource, DltSource
from dlt.sources.helpers import requests

EDAMAM_NUTRITION_DATA_API_URL = 'https://api.edamam.com/api/nutrition-data'
current_time = datetime.datetime.now()

@dlt.source(name='edamam')
def edamam_source(
    ingredients : List
) -> Sequence[DltResource]:
    return (nutrition_data(ingredients))

@dlt.resource(write_disposition='append')
def nutrition_data(ingredients: List[str]) -> Iterator[TDataItem]:
    params = {
        'app_id': dlt.secrets.value,
        'app_key': dlt.secrets.value,
        'nutrition-type': 'cooking',
    }
    for ingredient in ingredients:
        params['ingr'] = ingredient
        nutrition_detail = requests.get(EDAMAM_NUTRITION_DATA_API_URL, params=params)
        calorie_data = nutrition_detail.json()['calories']

        yield {
            "ingredient": ingredient,
            "calories": calorie_data,
            "created_at": current_time
        }
