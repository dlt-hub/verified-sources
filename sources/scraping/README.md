# 🕸️ Scraping source

Scraping source allows you to scrape content from web and uses [Scrapy](https://doc.scrapy.org/en/latest/)
to enable this capability.

## 🧠 How it works?

Under the hood we run DLT [pipeline](https://dlthub.com/docs/api_reference/pipeline) in a separate thread while scrapy uses [`scrapy.CrawlerRunner`](https://docs.scrapy.org/en/latest/topics/api.html#scrapy.crawler.CrawlerRunner) is running in the main thread.

Communication between the two is done via the queue, where

* Spider is responsible to put the results in the queue,
* DLT resource is constantly reading from the queue and terminates upon receiving `done` in the message.

![simple diagram](./diagram.png)

## 🎲 Configuration

It is possible to provide configuration via `.dlt/config.toml` below you can see an example

```toml
[sources.scraping]
batch_size = 20
queue_size = 3000
queue_result_timeout = 5
start_urls = [
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

## 🕷️ Defining a spider

When you develop your own spider please inherit it from `spider.DltSpider` because this base spider also accepts
queue instance. Otherwise if you wish to derive from `scrapy.Spider` please make sure to accept additional `queue`
parameter in constructor `__init__(self, queue, ...)` and save it in the instance.
For more information about implementing a custom spider please see the official documentation on their [website](https://docs.scrapy.org/en/latest/topics/spiders.html).

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
