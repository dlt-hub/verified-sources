# Kafka

> **Warning!**
>
> This source is a Community source and was tested only once. Currently, we **don't test** it on a regular basis.
> If you have any problem with this source, ask for help in our [Slack Community](https://dlthub.com/community).

[Kafka](https://www.confluent.io/) is an open-source distributed event streaming platform, organized
in the form of a log with message publishers and subscribers.
The Kafka `dlt` verified source loads data using Confluent Kafka API to the destination of your choice,
see a [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/kafka_pipeline.py).

The resource that can be loaded:

| Name              | Description                                |
| ----------------- |--------------------------------------------|
| kafka_consumer    | Extracts messages from Kafka topics        |


## Initialize the pipeline

```bash
dlt init kafka duckdb
```

Here, we chose `duckdb` as the destination. Alternatively, you can also choose `redshift`,
`bigquery`, or any of the other [destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Setup verified source

To grab Kafka credentials and configure the verified source, please refer to the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/kafka#grab-kafka-cluster-credentials)

## Add credential

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Use the following format for service account authentication:

   ```toml
   [sources.kafka.credentials]
   bootstrap_servers="web.address.gcp.confluent.cloud:9092"
   group_id="test_group"
   security_protocol="SASL_SSL"
   sasl_mechanisms="PLAIN"
   sasl_username="example_username"
   sasl_password="example_secret"
   ```

1. Next, follow the instructions in [Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```bash
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```bash
   python kafka_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `kafka_pipeline`, you may also use
   any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official
`dlt` [Kafka](https://dlthub.com/docs/dlt-ecosystem/verified-sources/kafka) documentation. It
provides comprehensive information and guidance on how to further customize and tailor the pipeline
to suit your specific needs.
