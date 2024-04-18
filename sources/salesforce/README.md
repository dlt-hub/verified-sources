# Salesforce

Salesforce is a cloud-based platform that helps businesses manage customer relationships and optimize various operational aspects. This Salesforce `dlt` verified source and pipeline example offers the capability to load Salesforce endpoints such as "User" and "Opportunity" to a destination of your choosing. It enables you to conveniently load the following endpoints:

### Single loading endpoints (replace mode)

| Endpoint | Mode | Description |
| --- | --- | --- |
| User | replace | refers to an individual who has access to a Salesforce org or instance |
| UserRole | replace | a standard object that represents a role within the organization's hierarchy |
| Lead | replace | prospective customer/individual/org. that has shown interest in a company's products/services |
| Contact | replace | an individual person associated with an account or organization |
| Campaign | replace | marketing initiative or project designed to achieve specific goals, such as generating leads etc. |
| Product2 | replace | for managing and organizing your product-related data within the Salesforce ecosystem |
| Pricebook2 | replace | used to manage product pricing and create price books |
| PricebookEntry | replace | an object that represents a specific price for a product in a price book |

### Incremental endpoints (merge mode)

| Endpoint | Mode | Description |
| --- | --- | --- |
| Opportunity | merge | represents a sales opportunity for a specific account or contact |
| OpportunityLineItem | merge | represents individual line items or products associated with an Opportunity |
| OpportunityContactRole | merge | represents the association between an Opportunity and a Contact |
| Account | merge | individual or organization that interacts with your business |
| CampaignMember | merge | association between a Contact or Lead and a Campaign |
| Task | merge | used to track and manage various activities and tasks within the Salesforce platform |
| Event | merge | used to track and manage calendar-based events, such as meetings, appointments calls, or any other time-specific activities |

To learn more about the write depositions i.e. merge, append and replace please refer to our official [documentation.](https://dlthub.com/docs/general-usage/incremental-loading)
## Initialize the pipeline with Salesforce verified source
```bash
dlt init salesforce bigquery
```

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source and pipeline example

To grab credentials and initialize the verified source, please refer to the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce)

## Add credentials

1. Open `.dlt/secrets.toml`.
2. Enter the username, password and security token:
    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.salesforce]
    username = "please set me up!" # Salesforce user name
    password = "please set me up!" # Salesforce password
    security_token = "please set me up!" # Salesforce security token generated
    ```
    
3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now the pipeline can be run by using the command:
    ```bash
    python3 salesforce_pipeline.py
    ```
    
3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline is `salesforce`, you may also use any custom name instead.

💡 To explore additional customizations for this pipeline, we recommend referring to the official dlt Salesforce verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the dlt Salesforce documentation in [Setup Guide: Salesforce.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce)
    
