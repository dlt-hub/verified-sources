---
title: Bing Webmaster
description: dlt source for Bing Webmaster API
keywords: [bing, bing webmasters, bing webmaster tools]
---


# Bing Webmaster

This source allows site owners to retrieve the organic traffic to their pages via Bing.
[Bing Webmaster](https://www.bing.com/webmasters/tools/) is a free service as part of Microsoft's Bing search engine. It allows webmasters to add their websites to the Bing index crawler, see their site's performance in Bing searches (including chat). This source reports the clicks, impressions and their respective average ranks.

Resources that can be loaded using this verified source are:

| Name             | Description                                                                     |
| ---------------- | --------------------------------------------------------------------------------|
| page_stats       | retrieves weekly traffic statistics for top pages belonging to a site_url       |
| page_query_stats | retrieves weekly trafic statistics per query for each pair of page and site_url |


## Initialize the pipeline

```bash
dlt init bing_webmaster duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or
any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Add credentials

1. [Bing Webmaster API](https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi) is an API that
   requires authentication or including secrets in `secrets.toml`. Create an account and generate the API key by clicking on the cog wheel in the [Bing Webmaster Web UI](https://www.bing.com/webmasters/home).

2. Add the obtained API key into `secrets.toml` as follows:
```toml
[sources.bing_webmaster]
api_key = "Please set me up!" # please set me up!
```

3. Ensure to add your sites and also verify with Bing that you have ownership of the domains you want to fetch statics for. Follow this [Documentation on add and verify site](https://www.bing.com/webmasters/help/add-and-verify-site-12184f8b). It describes how you can achieve that by importing your sites from Google Search Console or by adding your sites manually.

4. Follow the instructions in the
   [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) document to add credentials
   for your chosen destination.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

2. Substitute the example domain with your domain in the pipeline file `bing_webmaster_pipeline.py`.

3. Now the pipeline can be run by using the command:

   ```bash
   python3 bing_webmaster_pipeline.py
   ```

3. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline bing_webmaster_pipeline show
   ```

💡 To explore the API documentation, we recommend referring to the official API documentation for the two implemented resources:
- [GetPageStats](https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi.getpagestats)
- [GetPageQueryStats](https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi.getpagequerystats)
The documentation is very sparse and can be potentially misleading. Therefore, we have added more information into the docstrings of our implementation. We gained this additional information comes from our practical observations while working with this data source.
