from typing import List

import dlt

from pipelines.sql_database import sql_database

if __name__ == '__main__':
    pipeline = dlt.pipeline(
        dataset_name='sql_database_data',
        destination='bigquery',
        full_refresh=False
    )
    data = sql_database()
    info = pipeline.run(data)
    print(info)


