import dlt
from chess import chess, chess_dlt_config_example


def load_players_games_example():
    """Constructs a pipeline that will load chess games of specific players for a range of months."""

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(pipeline_name="chess_players_games", destination="duckdb", dataset_name="chess_players_games_data")
    # create the data source by providing a list of players and start/end month in YYYY/MM format
    data = chess(
        ['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'],
        start_month='2022/10',
        end_month='2022/12'
    )
    # load the "players_games" and "players_profiles" out of all the possible resources
    info = pipeline.run(data.with_resources("players_games", "players_profiles"))
    print(info)


def load_players_online_status():
    """Constructs a pipeline that will append online status of selected players"""

    pipeline = dlt.pipeline(pipeline_name="chess_players_games", destination="postgres", dataset_name="chess_players_online_status")
    data = chess(['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'])
    info = pipeline.run(data.with_resources("players_online_status"))
    print(info)


if __name__ == "__main__" :
    # run our main example
    list(chess_dlt_config_example())
    load_players_games_example()
    load_players_online_status()
