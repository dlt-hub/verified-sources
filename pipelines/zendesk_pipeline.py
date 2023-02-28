import time
import dlt
from zendesk.zendesk import zendesk, zendesk_chat


if __name__ == "__main__":
    start = time.time()
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_zendesk_data2")
    data = zendesk(load_all=True)
    data_chat = zendesk_chat(credentials=dlt.secrets["sources.zendesk_chat.credentials"].value)
    info = pipeline.run([data, data_chat])
    end = time.time()
    print(f"Time taken: {end-start}")
