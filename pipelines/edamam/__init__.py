"""
A pipeline for getting calories data via edamam API
"""

import datetime
from typing import Any, Iterator, List, Sequence

import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

EDAMAM_NUTRITION_DATA_API_URL = 'https://api.edamam.com/api/nutrition-data'
current_time = datetime.datetime.now()

@dlt.source(name='edamam')
def edamam_source(api_secrets_key: str = dlt.secrets.value) -> Sequence[DltResource]:
    """edamam dlt source"""
    return edamam_source(api_secrets_key)

@dlt.resource(write_disposition='append')
def nutrition_data(
    ingredients: List[str],
    app_id: str = dlt.secrets.value,
    api_secrets_key: str = dlt.secrets.value
) -> Iterator[TDataItem]:
    params = {
        'app_id': app_id,
        'app_key': api_secrets_key,
        'nutrition-type': 'cooking',
    }
    # fetch calorie data for each ingredient
    for ingredient in ingredients:
        params['ingr'] = ingredient
        nutrition_detail = requests.get(EDAMAM_NUTRITION_DATA_API_URL, params=params)
        calorie_data = nutrition_detail.json()['calories']
        # yield data and add created_at colummn since we are in append method
        yield {
            "ingredient": ingredient,
            "calories": calorie_data,
            "created_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
        }
