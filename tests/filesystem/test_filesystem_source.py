import tempfile
from pathlib import Path

import dlt
import pytest

from sources.filesystem import google_drive, local_folder

from tests.utils import ALL_DESTINATIONS, assert_load_info


def run_pipeline(destination_name: str, resource):
    # Mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # This allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_name",
        destination=destination_name,
        dataset_name="dataset_name",
        full_refresh=True,
    )
    # run the pipeline with your parameters
    load_info = pipeline.run(resource)
    return pipeline, load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestLoadFromLocalFolder:
    @pytest.fixture
    def data_dir(self) -> str:
        current_dir = Path(__file__).parent.resolve()
        return (current_dir / "test_data").as_posix()

    def test_load_info(self, destination_name: str, data_dir: str) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        _, load_info = run_pipeline(destination_name, data_resource)
        # make sure all data were loaded
        assert_load_info(load_info)

    def test_tables(self, destination_name: str, data_dir: str) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        pipeline, _ = run_pipeline(destination_name, data_resource)
        # now let's inspect the generated schema. it should contain just
        # one table with filepaths
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 1
        # tables are typed dicts
        filepaths_table = tables[0]
        assert filepaths_table["name"] == "local_folder"

    def test_local_folder_content(self, destination_name: str, data_dir: str) -> None:
        # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
        data_resource = local_folder(data_dir=data_dir)
        pipeline, _ = run_pipeline(destination_name, data_resource)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query("SELECT file_path FROM local_folder") as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 3  # 3 files in local folder


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestLoadFromGoogleDrive:
    @pytest.fixture
    def folder_ids(self):
        return ["1-yiloGjyl9g40VguIE1QnY5tcRPaF0Nm"]

    @pytest.mark.parametrize("download", [False, True])
    def test_load_info(
        self, destination_name: str, download: bool, folder_ids: list
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
            data_resource = google_drive(
                download=download,
                extensions=(".txt", ".pdf", ".jpg"),
                storage_folder_path=tmp_dir_path,
            )
            _, load_info = run_pipeline(destination_name, data_resource)
            # make sure all data were loaded
            assert_load_info(load_info)

    def test_tables(self, destination_name: str) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
            data_resource = google_drive(
                download=True, storage_folder_path=tmp_dir_path
            )
            pipeline, _ = run_pipeline(destination_name, data_resource)
            # now let's inspect the generated schema. it should contain just
            # one table with filepaths
            schema = pipeline.default_schema
            tables = schema.data_tables()
            assert len(tables) == 1
            # tables are typed dicts
            filepaths_table = tables[0]
            assert filepaths_table["name"] == "google_drive"

    def test_google_drive_content(self, destination_name: str) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
            data_resource = google_drive(
                download=True, storage_folder_path=tmp_dir_path
            )
            pipeline, _ = run_pipeline(destination_name, data_resource)
            with pipeline.sql_client() as c:
                # you can use parametrized queries as well, see python dbapi
                # you can use unqualified table names
                with c.execute_query("SELECT file_path FROM google_drive") as cur:
                    rows = list(cur.fetchall())
                    assert len(rows) == 2  # 2 files were downloaded, .jpg was skipped
