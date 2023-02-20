from typing import List

import dlt

from pipelines.sql_database import sql_database

if __name__ == '__main__':
    pipeline = dlt.pipeline(
        dataset_name='sql_database_data',
        full_refresh=False,
        restore_from_destination=True,
    )
    data = sql_database()
    info = pipeline.run(data)
    print(info)


