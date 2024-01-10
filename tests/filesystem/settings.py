import os

TESTS_BUCKET_URLS = [
    os.path.abspath("tests/filesystem/samples"),
    # ToDo: test s3 locally
    # "s3://dlt-ci-test-bucket/standard_source/samples",
    # ToDo: uncomment gs, az before PR
    # "gs://ci-test-bucket/standard_source/samples",
    # "az://dlt-ci-test-bucket/standard_source/samples",
    # ToDo: 
    #   a) repo ref. relative to current working directory for tess runner?
    #   b) ref tag with testable history. Do we need such system test?
    "gitpythonfs://~/dlt-verified-sources:HEAD@tests/filesystem/samples",
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
