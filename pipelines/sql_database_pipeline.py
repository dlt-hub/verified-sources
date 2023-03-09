from typing import List

import dlt

from sql_database import sql_database

if __name__ == '__main__':
    pipeline = dlt.pipeline(
        pipeline_name="sql_tables",
        destination="bigquery",
        dataset_name='sql_database_data',
        full_refresh=False,
    )
    data = sql_database()
    info = pipeline.run(data)
    print(info)


