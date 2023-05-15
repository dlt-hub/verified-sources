
# Youtube Analytics API


The YouTube Analytics API is a tool that allows developers to retrieve YouTube channel and video data such as views, likes, comments, and other metrics. With this API, developers can access data on a granular level and create custom dashboards or tools to analyze YouTube data.

To use the YouTube Analytics API, developers need to authenticate with their Google account and obtain an API key. They can then use the API to retrieve data in either JSON or XML format.

The YouTube Analytics API is a powerful tool for content creators, marketers, and developers who want to gain insights into their YouTube channel or videos' performance and user engagement.


# YouTube analytics pipeline setup guide

## API token for authentication

1. Create Google Cloud Platform. [link](https://cloud.google.com/) 
2. Create your own project. [link](https://developers.google.com/workspace/guides/create-project)
3. Go to search bar on top and type "# YouTube Data API v3"
4. Enable the API.
5. Go to tab Credentials and Create credentials then choose API key.
6. Save the API key to be used later.

## Initialize the pipeline

  

1. Initialize the pipeline by using the following command

  

```bash
dlt init  youtube  destination
```
*note: currently working destination is duckdb*

  

To know more about destinations, please read the [docs](https://dlthub.com/docs/destinations).

  

1. The file structure for the inited repository shall look like below:

  

```bash

youtube_data_pipeline

├── .dlt

│ ├──  config.toml

│ └──  secrets.toml

├── youtube_data

│ └──  __init__.py

│ └──  ReadMe.md

├── requirements.txt

└── youtube_data_pipeline.py
```

 

1. To learn more about initiating please read the docs [“Adding a pipeline”](https://dlthub.com/docs/walkthroughs/add-a-pipeline).

## Configuring credentials

  

1. In .dlt folder is secrets.toml. It should look like the code below

  

```bash
# put your secret values and credentials here. do not share this file and do not push it to github
api_secrets_key="set me up"
```
*note: currently working destination is duckdb, therefore make sure you have duckdb installed*
  
## Running the pipeline

  

1. To install requirements for the pipeline, run the following command

  

```bash
pip install  -r  requirements.txt
```

  

1. To run the pipeline, run the following command.

  

```bash
python3 youtube_data_pipeline.py
```

  

1. After a successful pipeline run, the load job shall be completed and data shall be loaded to destination.