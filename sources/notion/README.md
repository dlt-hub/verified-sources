---
title: Notion
description: dlt source for Notion API
keywords: [notion api, notion source, notion]
---

# Notion

Use this source to fetch databases from Notion API.

## Account Setup

1. Create a Notion account if you don't already have one.
2. Create an integration in Notion. You can do this by going to [My Integrations](https://www.notion.so/my-integrations) and clicking `+ New Integration`.
3. Give your integration a name, select the workspace you want to use, and click `Submit`.
4. Copy the `Internal Integration Token` and save it somewhere safe. You will need it later.

## Source wiht example pipeline Initialization

Use the following command to initialize the pipeline, replacing `[destination]` with your preferred [destination](../general-usage/glossary.md#destination):

```bash
dlt init notion [destination]
```

This command generates the following directory structure:

```bash
notion_pipeline
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── notion
│   └── __init__.py
│   └── client.py
│   └── database.py
│   └── README.md
├── .gitignore
├── notion_pipeline.py
└── requirements.txt
```

## Add Credentials

To authenticate with the Notion API, you'll need to provide the integration token you generated earlier:

1. Open the `secrets.toml` file located in the `.dlt` directory. You'll see a section like this:

    ```bash
    # Put your secret values and credentials here
    # Note: Do not share this file and do not push it to GitHub!
    [source.notion]
    api_key = "NOTION_API_TOKEN" # Notion API token (e.g. secret_XXX...)

    [destination.bigquery.credentials] # the credentials required will change based on the destination
    project_id = "set me up" # GCP project ID
    private_key = "set me up" # Unique private key (including `BEGINand END PRIVATE KEY`)
    client_email = "set me up" # Service account email
    location = "set me up" # Project location (e.g. “US”)
    ```

2. Replace `NOTION_API_TOKEN` with your Notion integration token.
3. Provide the necessary credentials for your chosen destination (e.g., for [Google BigQuery](http://localhost:3000/docs/destinations#google-bigquery), you would provide your GCP project ID, private key, service account email, and project location).

## Running the Pipeline

1. Install the pipeline's dependencies by executing the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Run the pipeline with this command:

    ```bash
    python3 notion_pipeline.py
    ```

3. To verify successful execution, use the `dlt pipeline notion_pipeline show` command.

You can find more information about Notion API token authentication in the [official Notion API documentation](https://developers.notion.com/reference).