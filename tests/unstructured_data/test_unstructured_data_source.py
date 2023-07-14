from typing import Sequence

import dlt
import pytest
from dlt.extract.source import DltResource

from sources.unstructured_data import unstructured_to_structured_resource
from sources.unstructured_data.filesystem import google_drive, local_folder

from tests.utils import ALL_DESTINATIONS, assert_load_info


def run_pipeline(
    destination_name: str, queries: dict, resource: DltResource, run_async: bool
):
    # Mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # This allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_name",
        destination=destination_name,
        dataset_name="dataset_name",
        full_refresh=True,
    )
    data_extractor = resource | unstructured_to_structured_resource(
        queries,
        table_name=f"unstructured_from_{resource.name}",
        run_async=run_async,
    )
    # run the pipeline with your parameters
    load_info = pipeline.run(data_extractor)
    return pipeline, load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("run_async", (False, True))
class TestUnstructuredFromLocalFolder:
    @pytest.fixture
    def data_resource(self, data_dir: str) -> DltResource:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        resource = local_folder(data_dir=data_dir, extensions=(".txt", ".pdf"))
        return resource

    def test_load_info(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
        run_async: bool,
    ) -> None:
        _, load_info = run_pipeline(destination_name, queries, data_resource, run_async)
        # make sure all jobs were loaded
        assert_load_info(load_info)

    def test_tables(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
        run_async: bool,
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource, run_async)
        # now let's inspect the generated schema. it should contain just
        # two tables with filepaths and structured data
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 1
        # tables are typed dicts
        structured_data_table = tables[0]
        assert structured_data_table["name"] == "unstructured_from_local_folder"

    def test_structured_data_content(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
        run_async: bool,
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource, run_async)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT file_path FROM unstructured_from_local_folder"
            ) as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 2  # 2 files were processed, .jpg was skipped


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("run_async", (False, True))
class TestUnstructuredFromGoogleDrive:
    @pytest.fixture(scope="session")
    def data_resource(self, tmpdir_factory, gd_folders: Sequence[str]) -> DltResource:
        tmp_path = tmpdir_factory.mktemp("temp_data")
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        resource = google_drive(
            download=True,
            extensions=(".txt", ".pdf"),
            storage_folder_path=tmp_path,
            folder_ids=gd_folders,
        )
        return resource

    def test_load_info(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
        run_async: bool,
    ) -> None:
        _, load_info = run_pipeline(destination_name, queries, data_resource, run_async)
        # make sure all data were loaded
        assert_load_info(load_info)

    def test_tables(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
        run_async: bool,
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource, run_async)
        # now let's inspect the generated schema. it should contain just
        # one table with filepaths
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 1
        # tables are typed dicts
        structured_data_table = tables[0]
        assert structured_data_table["name"] == "unstructured_from_google_drive"

    def test_google_drive_content(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
        run_async: bool,
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource, run_async)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT file_path FROM unstructured_from_google_drive"
            ) as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 2  # 2 files were processed, .jpg was skipped
