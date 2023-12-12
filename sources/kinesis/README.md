# Amazon Kinesis

[Amazon Kinesis](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html) is a
cloud-based service for real-time data streaming and analytics, enabling the processing and analysis
of large streams of data in real time.

Resources that can be loaded using this verified source are:

| Name             | Description                                                                              |
|------------------|------------------------------------------------------------------------------------------|
| kinesis_stream   | Load messages from the specified stream                                                  |

## Initialize the pipeline

```bash
dlt init kinesis duckdb
```

Here, we chose `duckdb` as the destination. Alternatively, you can also choose `redshift`,
`bigquery`, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Setup verified source

To grab Kinesis credentials and configure the verified source, please refer to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/amazon_kinesis#grab-credentials)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   # Put your secret values and credentials here.
   # Note: Do not share this file and do not push it to GitHub!
   [sources.kinesis.credentials]
   aws_access_key_id = "AKIA********"
   aws_secret_access_key = "K+o5mj********"
   region_name = "please set me up!" # aws region name
   ```

1. Optionally, you can configure `stream_name`. Update `.dlt/config.toml`:

   ```toml
   [sources.kinesis]
   stream_name = "please set me up!" # Stream name (Optional).
   ```

1. Replace the value of `aws_access_key_id` and `aws_secret_access_key`. This will ensure that the verified source can access your
   Kinesis resource securely.

1. Next, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

1. Now the pipeline can be run by using the command:

   ```bash
   python kinesis_pipeline.py
   ```

1. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `kinesis_pipeline`, you may
   also use any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` [Kinesis](https://dlthub.com/docs/dlt-ecosystem/verified-sources/amazon_kinesis)
documentation. It provides comprehensive information and guidance on how to further customize and
tailor the pipeline to suit your specific needs.