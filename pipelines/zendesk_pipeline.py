import time
import dlt
from zendesk.zendesk import zendesk


if __name__ == "__main__":
    start = time.time()
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_zendesk_data2")
    data = zendesk(load_all=True)
    info = pipeline.run(data)
    end = time.time()
    print(f"Time taken: {end-start}")
