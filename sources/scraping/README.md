# 🕸️ Scraping source

Scraping source allows you to scrape content from web and uses [Scrapy](https://doc.scrapy.org/en/latest/)
to enable this capability.

## 🧠 How it works?

Under the hood we run DLT [pipeline](https://dlthub.com/docs/api_reference/pipeline) in a separate thread while scrapy is running in the main thread.

Communication between the two is done via the queue, where

* Spider is responsible to put the results in the queue,
* DLT resource collects and batches results from the queue.

![simple diagram](./diagram.png)

## 🎲 Configuration

It is possible to provide configuration via `.dlt/config.toml` below you can see an example

```toml
[sources.scraping]
# Batch size - how many scraped results to collect
# before dispatching to DLT pipeline
batch_size = 20
# Defaul queue size
queue_size = 3000
# How log to wait before exiting
queue_result_timeout = 5
start_urls = [
    "https://quotes.toscrape.com/page/1/"
]
```

## 🏎️ Running the pipeline

Install requirements and run the pipeline

```sh
pip install -r requirements.txt
python scraping_pipeline.py
```

## 🧐 Introspection using streamlit

NOTE: you might need to set up `streamlit`, `pip install streamlit`

```sh
dlt pipeline <pipeline_name> show
```

## 🕷️ Using custom spider

You just need to implement a new spider and pass it to `create_pipeline_runner(spider=MySpider)`,
note it has to be a class not an instance of it.


## 💈 Using custom queue

You can provide custom queue to `build_scrapy_source` via `queue=CustomQueue` parameter and it is up to you
to initialize and prepare it accordring your requriements.

Also please note that we have generic typing definition of queue in `types.py`

```py
class BaseQueue(_Queue[T]):
    pass
```


<p align="center"><strong>Enjoy it!<strong></p>
<hr>
<p align="center">✨ 🚀 ✨</p>
