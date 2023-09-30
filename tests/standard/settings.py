import posixpath

TESTS_BUCKET_URLS = [
    posixpath.abspath("tests/standard/samples"),
    "s3://dlt-ci-test-bucket/standard_source/samples",
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
