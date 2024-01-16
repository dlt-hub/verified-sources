# 🕸️ Scraping source

Scraping source allows you to scrape content from web and uses [Scrapy](https://doc.scrapy.org/en/latest/)
to enable this capability.

## 🧠 How it works?

Under the hood we run DLT [pipeline](https://dlthub.com/docs/api_reference/pipeline) in a separate thread
while scrapy uses [`scrapy.CrawlerRunner`](https://docs.scrapy.org/en/latest/topics/api.html#scrapy.crawler.CrawlerRunner) is running in the main thread.
Communication between the two is done via the queue, where

* Spider is responsible to put the results in the queue,
* DLT resource is constantly reading from the queue and terminates upon receiving `done` in the message.

### 🛡️ Custom spider vs callbacks

`build_scrapy_source` accepts callbacks and a custom spider implementation. If custom spider is provided then it will be used while callbacks are skipped, if you instead resort to custom callbacks then we will use our generic `spider.DLTSpider` which takes care of calling them and
facilitates scraping continuation as well as sending chunks of data to pipeline over the queue.

```py
build_scrapy_source(
    spider=spider,
    on_result=parse,
    on_next_page=next_page,
)
```

## 🎲 Configuration

It is possible to provide configuration via `.dlt/config.toml` below you can see an example

```toml
[sources.scraping]
queue_size = 3000
include_headers = false
start_urls = [
    "https://quotes.toscrape.com/page/1/"
]
```

## 🔮 Parsing and paging

In the `scraping_pipeline.py` you can see we defined the following callbacks, where
1. `parse` should be a generator or iterable,
2. `next_page` shoudl return the next page url absolute or relative (scrapy will figure out the rest),

Both callbacks receive [`scrapy.http.Response`](https://docs.scrapy.org/en/latest/topics/request-response.html#response-objects) instance so you can take full advantage of parsing and extracting data
from `text`, `xml`, `html`, `json` ([parsel](https://github.com/scrapy/parsel) is used unde the hood).

```py
def parse(response: Response) -> Generator[Dict, None, None]:
    for quote in response.css("div.quote"):
        yield {
            "headers": {
                "status": response.status,
                **dict(response.headers.to_unicode_dict()),
            },
            "quote": {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
                "tags": quote.css("div.tags a.tag::text").getall(),
            },
        }


def next_page(response: Response) -> Optional[str]:
    return response.css("li.next a::attr(href)").get()
```

## 🕷️ Custom spiders

All custom spiders must inherit from `spider.DLTSpiderBase` because this way they will be able to access
queue instance which is used send data to DLT resource.

Example spider

```py
class CustomSpider(DLTSpiderBase):
    def parse(self, response: Response, **kwargs: Any) -> Any:
        # Extract data and send structured dictionary
        data = response.xpath("@//dataRoot").get()
        self.send_data({"data": data})

        # If next page is available 
        next_page = get_next_page(response)
        if next_page:
            yield scrapy.Request(next_page, callback=self.parse)
        else:
            # Send message that there is no more data
            self.done()
```

## 🚦 Swapping queue

It is possible specify some other type of `queue.*`` to `build_scrapy_source(queue=XYZ)`

<hr>
<p align="center">✨ 🚀 ✨</p>
