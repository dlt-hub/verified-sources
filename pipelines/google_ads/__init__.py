from typing import Dict, List
import dlt
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem
from dlt.common.pendulum import pendulum


@dlt.source(max_table_nesting=2)
def google_ads_query(credentials=dlt.secrets.value, query_list: List[DictStrAny] = dlt.config.value):
    pass


@dlt.source(max_table_nesting=2)
def google_ads():
    pass


@dlt.resource(name="dimension_tables", write_disposition="replace")
def get_dimensions():
    pass


@dlt.resource(name="reports_table", write_disposition="append")
def get_reports():
    pass
