# Amazon Kinesis

Amazon Kinesis is a fully managed service for real-time processing of streaming data at any scale,
you can read more about it in the official [docs](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html).

Using this `dlt` verified source you can load Amazon kinesis streams to a
[destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) of your choice. The resource
that loads the data is read_kinesis_stream, where you can specify the stream that you want to load:

## Initialize the pipeline

```bash
dlt init kinesis duckdb
```

Here, we chose duckdb as the destination. Alternatively, you can also choose redshift, bigquery, or
any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Add credentials

1. Open `.dlt/secrets.toml`.

2. Add the Kinesis credentials as follows:

   ```toml
   # Put your secret values and credentials here.
   # Note: Do not share this file and do not push it to GitHub!
   aws_access_key_id="AKIA********"
   aws_secret_access_key="K+o5mj********"
   aws_region="eu-central-1"
   ```

3. Update ".dlt/config.toml" with database and collection names:

   ```
   [your_pipeline_name]  # Set your pipeline name!
   stream_name = "stream" # Stream name (Optional).
   ```

   > Optionally, you can set stream_name in ".dlt/secrets.toml" under [sources.kinesis] without listing the pipeline name.

4. Enter credentials for your chosen destination as per the
   [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

2. Now the pipeline can be run by using the command:

   ```bash
   python kinesis_pipeline.py
   ```

3. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the pipeline_name for the above pipeline example is `kinesis`, you may also use
   any custom name instead.
