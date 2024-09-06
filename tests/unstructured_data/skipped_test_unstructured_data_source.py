from typing import Sequence

import dlt
import pytest

from dlt.sources import DltResource

from sources.unstructured_data import unstructured_to_structured_resource
from sources.unstructured_data.google_drive import google_drive_source
from sources.unstructured_data.inbox import inbox_source
from sources.unstructured_data.local_folder import local_folder_resource

from tests.utils import ALL_DESTINATIONS, assert_load_info, skipifwindows


def run_pipeline(
    destination_name: str, queries: dict, resource: DltResource, run_async: bool = False
):
    # Mind the dev_mode flag - it makes sure that data is loaded to unique dataset.
    # This allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_name",
        destination=destination_name,
        dataset_name="dataset_name",
        dev_mode=True,
    )
    data_extractor = resource | unstructured_to_structured_resource(
        queries,
        table_name=f"unstructured_from_{resource.name}",
        run_async=run_async,
    )
    # run the pipeline with your parameters
    load_info = pipeline.run(data_extractor)
    return pipeline, load_info


@skipifwindows
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestUnstructuredFromLocalFolder:
    @pytest.fixture
    def data_resource(self, data_dir: str) -> DltResource:
        resource = local_folder_resource(data_dir=data_dir)
        filtered_data_resource = resource.add_filter(
            lambda item: item["content_type"] == "application/pdf"
        )
        return filtered_data_resource

    @pytest.mark.parametrize("run_async", (False, True))
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
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        # now let's inspect the generated schema. it should contain just
        # two tables with filepaths and structured data
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 1
        # tables are typed dicts
        structured_data_table = tables[0]
        assert structured_data_table["name"] == "unstructured_from_local_folder"

    def test_content(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
    ) -> None:
        filtered_data_resource = data_resource.add_filter(
            lambda item: item["content_type"] == "application/pdf"
        )
        pipeline, _ = run_pipeline(destination_name, queries, filtered_data_resource)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT file_path FROM unstructured_from_local_folder"
            ) as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 1  # 1 file was processed, .jpg and .txt was skipped


@skipifwindows
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestUnstructuredFromGoogleDrive:
    @pytest.fixture(scope="session")
    def data_resource(
        self,
        tmpdir_factory,
        gd_folders: Sequence[str],
        filter_by_mime_type: Sequence[str] = (),
    ) -> DltResource:
        tmp_path = tmpdir_factory.mktemp("temp_data")
        source = google_drive_source(
            download=True,
            storage_folder_path=tmp_path,
            folder_ids=gd_folders,
            filter_by_mime_type=filter_by_mime_type,
        )
        resource = source.resources["attachments"]
        filtered_data_resource = resource.add_filter(
            lambda item: item["content_type"] == "application/pdf"
        )
        return filtered_data_resource

    @pytest.mark.parametrize("run_async", (False, True))
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
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        # now let's inspect the generated schema. it should contain just
        # one table with filepaths
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 2
        # tables are typed dicts
        structured_data_table = tables[0]
        assert structured_data_table["name"] == "unstructured_from_attachments"

    def test_content(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT file_path FROM unstructured_from_attachments"
            ) as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 1  # 1 file was processed, .jpg and .txt was skipped


@skipifwindows
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestUnstructuredFromInbox:
    @pytest.fixture(scope="session")
    def data_resource(self, tmpdir_factory) -> DltResource:
        tmp_path = tmpdir_factory.mktemp("temp_data")
        source = inbox_source(
            attachments=True,
            storage_folder_path=tmp_path,
        )
        resource = source.resources["attachments"]
        filtered_data_resource = resource.add_filter(
            lambda item: item["content_type"] == "application/pdf"
        )
        return filtered_data_resource

    @pytest.mark.parametrize("run_async", (False, True))
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
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        # now let's inspect the generated schema. it should contain just
        # one table with filepaths
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 1
        # tables are typed dicts
        structured_data_table = tables[0]
        assert structured_data_table["name"] == "unstructured_from_attachments"

    def test_content(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
    ) -> None:
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT file_path FROM unstructured_from_attachments"
            ) as cur:
                rows = list(cur.fetchall())
                assert (
                    len(rows) == 3
                )  # 3 pdfs were processed, txt were skipped, and one duplicate was skipped

    def test_incremental_loading(
        self,
        destination_name: str,
        queries: dict,
        data_resource: DltResource,
    ) -> None:
        pipeline, load_info = run_pipeline(destination_name, queries, data_resource)
        # make sure all data were loaded
        assert_load_info(load_info)
        print(load_info)

        data_extractor = data_resource | unstructured_to_structured_resource(
            queries,
            table_name=f"unstructured_from_{data_resource.name}",
        )
        # run the pipeline with your parameters
        load_info = pipeline.run(data_extractor)
        # make sure all data were loaded
        print(load_info)
        assert_load_info(load_info, expected_load_packages=0)
