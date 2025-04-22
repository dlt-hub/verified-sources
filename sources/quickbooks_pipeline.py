import dlt
from quickbooks_online import quickbooks_online

def load_customer():
    pipeline = dlt.pipeline(
        pipeline_name='quickbooks_customer',
        destination='duckdb',
        dataset_name='quickbooks_online'
    )
    load_info = pipeline.run(quickbooks_online())
    print(load_info)

if __name__ == "__main__":
    load_customer()