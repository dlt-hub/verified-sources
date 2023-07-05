import dlt
import pytest

from sources.filesystem import local_folder

from tests.utils import ALL_DESTINATIONS, assert_load_info


def run_pipeline(destination_name: str, data_dir: str, resource):
    # Mind the full_refresh flag - it makes sure that data is loaded to unique dataset.
    # This allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_name",
        destination=destination_name,
        dataset_name="dataset_name",
        full_refresh=True,
    )
    # use extensions to filter files as 'extensions=(".txt", ".pdf", ...)'
    data_resource = resource(data_dir=data_dir)
    # run the pipeline with your parameters
    load_info = pipeline.run(data_resource)

    return pipeline, load_info


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
class TestLoadFromLocalFolder:
    @pytest.fixture
    def data_dir(self) -> str:
        return "./test_data"

    def test_load_info(self, destination_name: str, data_dir: str) -> None:
        _, load_info = run_pipeline(destination_name, data_dir, local_folder)
        # make sure all data were loaded
        assert_load_info(load_info)

    def test_tables(self, destination_name: str, data_dir: str) -> None:
        pipeline, _ = run_pipeline(destination_name, data_dir, local_folder)
        # now let's inspect the generated schema. it should contain just
        # one table with filepaths
        schema = pipeline.default_schema
        tables = schema.data_tables()
        assert len(tables) == 1
        # tables are typed dicts
        filepaths_table = tables[0]

        assert filepaths_table["name"] == "local_folder"

    def test_local_folder_content(self, destination_name: str, data_dir: str) -> None:
        pipeline, _ = run_pipeline(destination_name, data_dir, local_folder)
        with pipeline.sql_client() as c:
            # you can use parametrized queries as well, see python dbapi
            # you can use unqualified table names
            with c.execute_query("SELECT file_path FROM local_folder") as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 3  # 3 files in local folder
