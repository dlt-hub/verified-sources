import dlt
from quickbooks_online import quickbooks_online
from quickbooks_online.settings import sandbox_env, production_env


def load_customer() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="quickbooks_customer",
        destination="duckdb",
        dataset_name="quickbooks_online",
    )
    load_info = pipeline.run(quickbooks_online(environment=sandbox_env))
    print(load_info)


if __name__ == "__main__":
    load_customer()
