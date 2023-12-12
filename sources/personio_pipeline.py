"""Pipeline to load personio data into Duckdb."""

from typing import List

import dlt
from personio import personio_source


def load_all_resources(resources: List[str]) -> None:
    """Execute a pipeline that will load the given Personio resources incrementally.
    Subsequent runs will load only items updated since the previous run, if supported by the resource.
    """

    pipeline = dlt.pipeline(
        pipeline_name="personio", destination="duckdb", dataset_name="personio_data"
    )
    load_info = pipeline.run(
        personio_source().with_resources(*resources),
    )
    print(load_info)


if __name__ == "__main__":
    # Add your desired resources to the list.

    resources = [
        "employees",
        "absence_types",
        "absences",
        "attendances",
        "projects",
        "document_categories",
        "employees_absences_balance",
        "custom_reports_list",
        "custom_reports",
    ]
    load_all_resources(resources)
