import time
import dlt
from zendesk.zendesk import zendesk, zendesk_chat, zendesk_talk, ZendeskCredentials


if __name__ == "__main__":
    start = time.time()
    ze_value = dlt.secrets.value
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_zendesk_data2")
    data = zendesk(load_all=True)
    data_chat = zendesk_chat()
    data_talk = zendesk_talk(credentials=ZendeskCredentials(dlt.secrets["sources.zendesk.zendesk.credentials"].value))
    info = pipeline.run([data_talk])
    end = time.time()
    print(f"Time taken: {end-start}")
