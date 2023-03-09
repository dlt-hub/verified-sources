import time
import dlt
from zendesk import zendesk, zendesk_chat, zendesk_talk


if __name__ == "__main__":
    start = time.time()
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_zendesk_pipeline", destination="postgres", full_refresh=False, dataset_name="sample_zendesk_data")
    data = zendesk(load_all=True)
    data_chat = zendesk_chat()
    data_talk = zendesk_talk()
    info = pipeline.run(data=[data, data_chat, data_talk])
    end = time.time()
    print(f"Time taken: {end-start}")
