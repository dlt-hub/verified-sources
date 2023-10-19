# Inbox Source


This source provides functionalities to collect inbox emails, get the attachment as a [file items](../filesystem/README.md#the-fileitem-file-representation),
and store all relevant email information in a destination. It utilizes the `imaplib` library to
interact with the IMAP server, and `dlt` library to handle data processing and transformation.

Sources and resources that can be loaded using this verified source are:

| Name              | Description                               |
|-------------------|-------------------------------------------|
| inbox_source      | Gathers inbox emails and saves attachments locally |
| uids | Retrieves messages UUIDs from the mailbox |
| messages | Retrieves emails from the mailbox using given UIDs |
| attachments | Downloads attachments from emails using given UIDs |

## Initialize the pipeline

```bash
dlt init inbox duckdb
```

Here, we chose `duckdb` as the destination. Alternatively, you can also choose `redshift`, `bigquery`, or
any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Grab Inbox credentials

To learn about grabbing the Inbox credentials and configuring the verified source, please refer to
the
[full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/inbox#grab-credentials)

## Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here
   # do not share this file and do not push it to github
   [sources.inbox]
   host = "Please set me up!" # The host address of the email service provider.
   email_account = "Please set me up!" # Email account associated with the service.
   password = "Please set me up!" # # APP Password for the above email account.
   ```

1. Replace the host, email and password value to
   ensure secure access to your Inbox resources.

   > When adding the App Password, remove any spaces. For instance, "abcd efgh ijkl mnop" should be
   > "abcdefghijklmnop".

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to
   add credentials for your chosen destination, ensuring proper routing of your data to the final
   destination.

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```bash
   pip install -r requirements.txt
   ```

   Prerequisites for fetching messages differ by provider.

    - For Gmail:

      `pip install google-api-python-client>=2.86.0`

      `pip install google-auth-oauthlib>=1.0.0`

      `pip install google-auth-httplib2>=0.1.0`

    - For pdf parsing:

      `pip install PyPDF2`

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `standard_inbox`, you may also
   use any custom name instead.

For more information, read the [Walkthrough: Run a pipeline.](../../walkthroughs/run-a-pipeline)

💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT
Inbox documentation. It provides comprehensive information and guidance on how to further customize
and tailor the pipeline to suit your specific needs. You can find the DLT Inbox documentation in
the [Setup Guide: Inbox.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/inbox)
