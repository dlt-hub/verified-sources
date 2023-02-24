# Pipedrive dlt Pipeline

# **Pipedrive Setup**

1. Set up a Pipedrive account.
2. Pipedrive provides a unique domain name. The domain name is generally [yourbusiness].pipedrive.com
3. For ex. If a company name is dlthub, then the domain name shall be dlthub.pipedrive.com or something similar.

## API Token for Authentication

1. Go to your name(in the top right corner) and company settings.
2. Go to Personal Preferences
3. Select the API tab and copy your API token( to be used in the dlt configuration)

## Clone Git Repo

1. Open the link below and clone this to clone the repository.

[https://github.com/dlt-hub/pipelines/tree/master/pipelines](https://github.com/dlt-hub/pipelines/tree/master/pipelines) 

# Pipeline Files

The file structure for the cloned repository shall look like:

```
pipelines
├── _helpers
│   └── __init__.py
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── pipedrive
│   └── __init__.py
├── __init__.py
├── pipedrive_pipeline.py
└── requirements.txt
```

## Configuring Credentials

1. The virtual environment  will look like below:

![Environment_view.png](https://github.com/AmanGuptAnalytics/pipelines/blob/master/pipelines/pipedrive/pipedrive_docs_imgs/Environment_view1.png)

1. In .dlt folder is example.secrets.toml. Rename it to secrets.toml.
2. The secrets.toml should look like the code below, delete everything else.

```python
# put your secret values and credentials here. do not share this file and do not push it to github
pipedrive_api_key = "Pipedrive API Token" # please set me up!
# Copy API token from Name in top right corner > Company Settings > Personal Pre-frences > API token

[destination.bigquery.credentials]
project_id = "set me up" # GCP project ID!
private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
client_email = "set me up" # Email for service account
location = "set me up" #Project Location For ex. “US”

```

1. For the Pipedrive API token go to your name(in the top right corner) > company settings> Personal Preferences > API Token *(Copy that and paste above in pipedrive_api_key)* 
2. Google cloud credentials may be copied from Secrets Manager in GCP.

## Pipeline Setup

1. Open the pipedrive_pipeline.py file in pipelines directory.
2. Rename the ***destination = ‘postgres’*** highlighted below to ***destination=’bigquery’***
    
    ![rename_postgres_ineditor.png](https://github.com/AmanGuptAnalytics/pipelines/blob/master/pipelines/pipedrive/pipedrive_docs_imgs/rename_postgres_ineditor1.png)
    
3. The above code should look like

![rename_tobigquery_ineditor.png](https://github.com/AmanGuptAnalytics/pipelines/blob/master/pipelines/pipedrive/pipedrive_docs_imgs/rename_tobigquery_ineditor1.png)

1. Save the changes (Command+S, or Ctrl+S)

## Running the Pipeline

1. In terminal type the following to **run** the pipeline. 

```bash
python3 pipedrive_pipeline.py
```

1. Make sure the pipeline completed the run successfully without any errors or warnings, if not recheck the steps above.
2. dlt will load the Data to the pipedrive_data dataset in the Bigquery.
3. To confirm the successful run of the pipeline, check that data is loaded in Bigquery as below.

![pipedrive_schema.png](https://github.com/AmanGuptAnalytics/pipelines/blob/master/pipelines/pipedrive/pipedrive_docs_imgs/pipedrive_schema1.png)

---

