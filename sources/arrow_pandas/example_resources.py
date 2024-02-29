import dlt

from typing import Generator

from dlt.common import pendulum
import pandas as pd
import pyarrow as pa


@dlt.resource(name="orders", write_disposition="append")
def orders() -> Generator[pd.DataFrame, None, None]:
    # this is example data, you will get this from somewhere on your resource function
    EXAMPLE_ORDERS_DATA_FRAME = pd.DataFrame(
        data={
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 3],
            "ordered_at": [
                pendulum.DateTime(2021, 1, 1, 4, 5, 6),
                pendulum.DateTime(2021, 1, 3, 4, 5, 6),
                pendulum.DateTime(2021, 1, 6, 4, 5, 6),
            ],
            "order_amount": [100.0, 200.0, 300.0],
        }
    )

    # we can yield dataframes here, you will usually read them from a file or
    # receive them from another library
    yield EXAMPLE_ORDERS_DATA_FRAME


@dlt.resource(
    name="customers",
    write_disposition="merge",
    primary_key="customer_id",
    merge_key="customer_id",
)
def customers() -> Generator[pd.DataFrame, None, None]:
    # we can yield arrow tables here, you will usually read them from a file or
    # receive them from another library
    EXAMPLE_CUSTOMERS_DATA_FRAME = pd.DataFrame(
        data={
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )

    # here we convert our dataframe to an arrow table, usually you would just yield the
    # dataframe if you have it, this is for demonstration purposes
    yield pa.Table.from_pandas(EXAMPLE_CUSTOMERS_DATA_FRAME)
