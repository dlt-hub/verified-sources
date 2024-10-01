import pytest

import dlt

from sources.chess import source, chess_dlt_config_example

from tests.utils import ALL_DESTINATIONS, assert_load_info

PLAYERS = ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_players_games(destination_name: str) -> None:
    # mind the dev_mode flag - it makes sure that data is loaded to unique dataset.
    # this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="chess_players_games",
        destination=destination_name,
        dataset_name="chess_players_games_data",
        dev_mode=True,
    )
    data = source(
        PLAYERS,
        start_month="2022/11",
        end_month="2022/12",
    )
    # load the "players_games" out of the data source
    info = pipeline.run(data.with_resources("players_games"))
    # lets print it (pytest -s will show it)
    print(info)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with user data
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert len(user_tables) == 1
    # tables are typed dicts
    players_games_table = user_tables[0]
    assert players_games_table["name"] == "players_games"
    # TODO: if we have any columns of interest ie. that should be timestamps
    #  or have certain performance hints, we can also check it
    assert players_games_table["columns"]["end_time"]["data_type"] == "timestamp"
    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query(
            "SELECT white__username, COUNT(1) FROM players_games WHERE white__username IN (%s) GROUP BY white__username",
            "MagnusCarlsen",
        ) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 1
            assert rows[0][1] == 374  # magnus has 374 games


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_players_profiles(destination_name: str) -> None:
    # mind the dev_mode flag - it makes sure that data is loaded to unique dataset.
    # this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="chess_players_profiles",
        destination=destination_name,
        dataset_name="chess_players_profiles_data",
        dev_mode=True,
    )
    data = source(PLAYERS)
    # load the "players_games" out of the data source
    info = pipeline.run(data.with_resources("players_profiles"))
    # lets print it (pytest -s will show it)
    print(info)
    # make sure all jobs were loaded
    assert_load_info(info)
    # now let's inspect the generates schema. it should contain just one table with user data
    schema = pipeline.default_schema
    user_tables = schema.data_tables()
    assert len(user_tables) == 1
    # tables are typed dicts
    players_table = user_tables[0]
    assert players_table["name"] == "players_profiles"
    # TODO: if we have any columns of interest ie. that should be timestamps
    #  or have certain performance hints, we can also check it
    assert players_table["columns"]["last_online"]["data_type"] == "timestamp"
    assert players_table["columns"]["joined"]["data_type"] == "timestamp"
    # we can also test the data
    with pipeline.sql_client() as c:
        # you can use parametrized queries as well, see python dbapi
        # you can use unqualified table names
        with c.execute_query("SELECT * FROM players_profiles") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == len(PLAYERS)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_games_load(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="chess_players_games",
        destination=destination_name,
        dataset_name="chess_players_games_data",
        dev_mode=True,
    )
    data = source(["magnuscarlsen"], start_month="2022/11", end_month="2022/11")
    info = pipeline.run(data.with_resources("players_games"))
    assert_load_info(info)

    def get_magnus_games() -> int:
        with pipeline.sql_client() as c:
            with c.execute_query(
                "SELECT white__username, COUNT(1) FROM players_games WHERE white__username IN ('MagnusCarlsen') GROUP BY white__username"
            ) as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 1
                return rows[0][1]

    magnus_games_no = get_magnus_games()
    assert magnus_games_no > 0  # should have games

    # do load with the same range into the existing dataset
    data = source(["magnuscarlsen"], start_month="2022/11", end_month="2022/11")
    info = pipeline.run(data.with_resources("players_games"))
    # the dlt figured out that there's no new data at all and skipped the loading package
    assert_load_info(info, expected_load_packages=0)
    # there are no more games as pipeline is skipping existing games
    assert get_magnus_games() == magnus_games_no

    # get some new games
    data = source(["magnuscarlsen"], start_month="2022/12", end_month="2022/12")
    info = pipeline.run(data.with_resources("players_games"))
    # we have new games in December!
    assert_load_info(info)
    assert get_magnus_games() > magnus_games_no


def test_config_values_source() -> None:
    # all the input arguments will be passed from secrets/config toml by dlt
    config_values = list(chess_dlt_config_example())
    # the values below are configured in secrets and config in tests/.dlt
    assert config_values == [
        "secret string",
        {"secret_key": "key string", "key_index": 1},
        123,
    ]
