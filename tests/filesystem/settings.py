from typing import Union
from os import PathLike
from pathlib import Path
from tempfile import gettempdir


TEST_SAMPLES_PATH: str = "tests/filesystem/samples"
REPO_FIXTURE_PATH: Union[str, PathLike] = Path(
    gettempdir(), "dlt_test_repo_t8hY3x"
).absolute()
REPO_SAFE_PREFIX: str = "test-"
REPO_GOOD_REF = "good-ref"

FACTORY_ARGS = [
    {"bucket_url": str(Path(TEST_SAMPLES_PATH).absolute())},
    {"bucket_url": "s3://dlt-ci-test-bucket/standard_source/samples"},
    {"bucket_url": "gs://ci-test-bucket/standard_source/samples"},
    {"bucket_url": "az://dlt-ci-test-bucket/standard_source/samples"},
    {
        "bucket_url": f"gitpythonfs://{REPO_SAFE_PREFIX}samples",
        "kwargs": {
            "repo_path": REPO_FIXTURE_PATH,
            "ref": REPO_GOOD_REF,
        },
    },
]

FACTORY_TEST_IDS = [
    f"url={factory_args['bucket_url'][:15]}..." for factory_args in FACTORY_ARGS
]

GLOBS = [
    {
        "glob": None,
        "file_names": ["sample.txt"],
    },
    {
        "glob": "*/*",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
            "gzip/taxi.csv.gz",
            "jsonl/mlb_players.jsonl",
            "parquet/mlb_players.parquet",
        ],
    },
    {
        "glob": "**/*.csv",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
            "met_csv/A801/A881_20230920.csv",
            "met_csv/A803/A803_20230919.csv",
            "met_csv/A803/A803_20230920.csv",
        ],
    },
    {
        "glob": "*/*.csv",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "csv/*",
        "file_names": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "csv/mlb*",
        "file_names": [
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "*",
        "file_names": ["sample.txt"],
    },
]

GLOB_TEST_IDS = [f"glob={glob_result['glob']}" for glob_result in GLOBS]
