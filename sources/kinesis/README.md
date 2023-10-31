# Amazon Kinesis

Amazon Kinesis is a fully managed service for real-time processing of streaming data at any scale,
you can read more about it in the official [docs](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html).

Using this `dlt` verified source you can load Amazon kinesis streams to a
[destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) of your choice. The resource
that loads the data is **kinesis_stream**, where you can specify the stream that you want to load:

## How does it work
You create a resource `kinesis_stream` by passing the stream name and a few other options. The resource will have the same name as the stream. When
you iterate this resource (or pass it to `pipeline.run` records) it will query Kinesis for all the shards in the requested stream.
For each shard it will create an iterator to read messages:
1. If `initial_at_timestamp` is present, the resource will read all messages after this timestamp
2. If `initial_at_timestamp` is 0, only the messages at the tip of the stream are read
3. If no initial timestamp is provided, all messages will be retrieved (from the TRIM HORIZON)

The resource stores all message sequences per shard in the state. If you run the resource again, it will load messages incrementally:
1. For all shards that had messages, only messages after last message are retrieved
2. For shards that didn't have messages (or new shards), the last run time is used to get messages

Please check the `kinesis_stream` docstring for additional options ie. to limit number of messages returned or to automatically parse JSON messages.

### Kinesis message format
The message envelope is stored in `_kinesis` dictionary of the message, it contains ie. shard id, message sequence, partition key etc.
Message contains `_kinesis_msg_id` which is primary key: a hash over (shard_id, message sequence).
If you set `parse_json` to True (default), you'll get parsed **Data** field as message, otherwise you'll get `data` as bytes.

### Supplied examples
1. Load past hour of messages on first run and then incrementally in the next
2. Add custom **data** field decoder with `add_map`
3. Use **kinesis_stream** without pipeline to stream data to an api.


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
   [sources.kinesis.credentials]
   aws_access_key_id="AKIA********"
   aws_secret_access_key="K+o5mj********"
   region_name="eu-central-1"
   ```

   Mind that **region_name** must be set explicitly (or present in the default credentials on the machine).

3. Optionally, you can configure **stream_name**. Update ".dlt/config.toml":

   ```
   [sources.kinesis]
   stream_name = "stream" # Stream name (Optional).
   ```

   > Optionally, you can set stream_name in ".dlt/secrets.toml" under [sources.kinesis].

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

   For example, the pipeline_name for the above pipeline example is `kinesis_pipeline`, you may also use
   any custom name instead.
