# Workable

Workable is an online platform for posting jobs and managing the recruitment process. With Workable, employers can post jobs, receive applications, track candidates, collaborate with team members, schedule interviews and manage the entire hiring workflow.

With Workable `dlt` verified source and pipeline example, you have the flexibility to load different endpoints such as:

| Endpoints | Description |
| --- | --- |
| members | can have different roles and permissions such as owner, admin, hiring manager, recruiter, etc. |
| recruiters | are involved in sourcing and screening candidates, conducting interviews and coordinating the recruitment process. |
| stages | different steps or stages of the recruitment process through which candidates progress. |
| requisitions | refer to job openings or positions that an organization is actively looking to fill. |
| jobs | are the individual positions or roles within an organisation that need to be filled. |
| custom_attributes | refer to additional fields or data points that you can define and include in your candidate or job records. |
| events | refer to specific events or activities related to the recruitment process that can be planned and managed. |

The following sub-endpoints can also be loaded as details of the main endpoints:

| Endpoint | Sub-endpoint | Description                                                                                                               |
| --- | --- |---------------------------------------------------------------------------------------------------------------------------|
| candidates | activities | are actions or interactions that occur between recruiters, hiring managers, and candidates throughout the hiring process. |
| candidates | offer | refers to the stage in the hiring process where a formal job offer is extended to a selected candidate.                   |
| jobs | activities | retrieves the activities or events associated with a specific job.                                                        |
| jobs | application_form | accesses the application form details and settings for a specific job.                                                    |
| jobs | questions | manages the interview questions associated with a specific job.                                                           |
| jobs | stages | handles the stages or steps in the hiring process for a specific job.                                                     |
| jobs | custom_attributes | manages the custom attributes or additional data fields associated with a specific job.                                   |
| jobs | members | manages the members or users with access to a specific job.                                                               |
| jobs | recruiters | manages the recruiters or individuals responsible for handling a specific job in Workable.                                |

## Initialize the pipeline
```bash
dlt init workable bigquery
```
Here, we chose bigquery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source

To get the full list of supported endpoints, grab credentials and initialise the verified source and pipeline example, read the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/workable)

## Add credentials

1. Open `.dlt/secrets.toml`.
2. Enter the access_token key:
    ```toml
    # put your secret values and credentials here. do not share this file and do not upload it to github.
    [sources.workable]
    access_token = "access_token" # your workable token copied above.
    ```
3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)
4. Within the `.dlt` folder you'll find a file called `config.toml` where you can safely store your pipeline configuration details.

   This is what `config.toml` looks like:

   ```toml
   [sources.workable]
   subdomain = "subdomain" # Please set me up!
   ```

5. Replace the value of subdomain with the one in the address bar, for example, if your URL is `https://my-company.workable.com/`,  your subdomain would be "my-company".

## Running the pipeline example

1. Install the required dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```

2. Now you can run the pipeline using the command:
    ```bash
    python3 workable_pipeline
    ```

3. To make sure that everything is loaded as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline example is `workable`, you may also use any custom name instead.



💡 To explore additional customizations for this pipeline, we recommend referring to the official `dlt` Workable verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the `dlt` Workable documentation in [Setup Guide: Workable.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/workable)
