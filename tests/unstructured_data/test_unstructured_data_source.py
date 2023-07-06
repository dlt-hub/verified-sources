import tempfile
from pathlib import Path
from typing import List

import dlt
import pytest

from sources.filesystem import google_drive, local_folder
from sources.unstructured_data import unstructured_to_structured_source

from tests.utils import ALL_DESTINATIONS, assert_load_info


def run_pipeline(destination_name: str, queries: dict, resource):
    # Mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # This allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_name",
        destination=destination_name,
        dataset_name="dataset_name",
        full_refresh=True,
    )

    data_extractor = unstructured_to_structured_source(resource, queries)
    # run the pipeline with your parameters
    load_info = pipeline.run(data_extractor)
    return pipeline, load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestUnstructuredFromLocalFolder:
    @pytest.fixture
    def data_dir(self) -> str:
        current_dir = Path(__file__).parent.resolve()
        return (current_dir / "test_data").as_posix()

    def test_load_info(
        self, destination_name: str, queries: dict, data_dir: str
    ) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        _, load_info = run_pipeline(destination_name, queries, data_resource)
        # make sure all jobs were loaded
        assert_load_info(load_info)

    def test_tables(self, destination_name: str, queries: dict, data_dir: str) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        # now let's inspect the generated schema. it should contain just
        # two tables with filepaths and structured data
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 2
        # tables are typed dicts
        filepaths_table = tables[1]
        structured_data_table = tables[0]

        assert filepaths_table["name"] == "local_folder"
        assert structured_data_table["name"] == "structured_data_from_local_folder"

    def test_local_folder_content(
        self, destination_name: str, queries: dict, data_dir: str
    ) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query("SELECT file_path FROM local_folder") as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 3  # 3 files in local folder

    def test_structured_data_content(
        self, destination_name: str, queries: dict, data_dir: str
    ) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        pipeline, _ = run_pipeline(destination_name, queries, data_resource)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query(
                "SELECT file_path FROM structured_data_from_local_folder"
            ) as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 2  # 2 files were processed, .jpg was skipped


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestUnstructuredFromGoogleDrive:
    FOLDER_IDS: List[str] = ["1-yiloGjyl9g40VguIE1QnY5tcRPaF0Nm"]

    def test_load_info(
        self,
        destination_name: str,
        queries: dict,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
            data_resource = google_drive(
                download=True,
                extensions=(".txt", ".pdf", ".jpg"),
                storage_folder_path=tmp_dir_path,
                folder_ids=self.FOLDER_IDS,
            )
            _, load_info = run_pipeline(destination_name, queries, data_resource)
            # make sure all data were loaded
            assert_load_info(load_info)

    def test_tables(
        self,
        destination_name: str,
        queries: dict,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
            data_resource = google_drive(
                download=True,
                storage_folder_path=tmp_dir_path,
                folder_ids=self.FOLDER_IDS,
            )
            pipeline, _ = run_pipeline(destination_name, queries, data_resource)
            # now let's inspect the generated schema. it should contain just
            # one table with filepaths
            schema = pipeline.default_schema
            tables = schema.data_tables()
            assert len(tables) == 2
            # tables are typed dicts
            filepaths_table = tables[1]
            structured_data_table = tables[0]

            assert filepaths_table["name"] == "google_drive"
            assert structured_data_table["name"] == "structured_data_from_google_drive"

    def test_google_drive_content(
        self,
        destination_name: str,
        queries: dict,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
            data_resource = google_drive(
                download=True,
                storage_folder_path=tmp_dir_path,
                folder_ids=self.FOLDER_IDS,
            )
            pipeline, _ = run_pipeline(destination_name, queries, data_resource)
            with pipeline.sql_client() as c:
                # you can use parametrized queries as well, see python dbapi
                # you can use unqualified table names
                with c.execute_query(
                    "SELECT file_path FROM structured_data_from_google_drive"
                ) as cur:
                    rows = list(cur.fetchall())
                    assert len(rows) == 2  # 2 files were processed, .jpg was skipped
