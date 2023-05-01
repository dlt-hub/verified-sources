from typing import List

import dlt
from edamam import nutrition_data

def load_nutrition_data(ingredients: List[str]) -> None:
    """construct nutrition_data pipeline for duckdb"""
    pipeline = dlt.pipeline(
        pipeline_name='edamam',
        destination='duckdb',
        dataset_name='edamam_data'
    )

    data = nutrition_data(ingredients)
    info = pipeline.run(data)
    print(info)

if __name__ == "__main__":
    # you can modify the list of ingredients below
    # the format of each ingredient is <weight> <measurement> <ingredient>
    ingredients = ['100 gr chicken', '250 gr rice']
    load_nutrition_data(ingredients)
