import os

FACTORY_ARGS = [
    {"bucket_url": os.path.abspath("tests/filesystem/samples")},
    {"bucket_url": "s3://dlt-ci-test-bucket/standard_source/samples"},
    {"bucket_url": "gs://ci-test-bucket/standard_source/samples"},
    {"bucket_url": "az://dlt-ci-test-bucket/standard_source/samples"},
    {
        "bucket_url": "gitpythonfs://samples",
        "kwargs": {
            "repo_path": "tests/filesystem/cases/git",
            "ref": "unmodified-samples",
        },
    }
]

GLOB_RESULTS = [
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
