# Edamam Nutirion Analysis API

Edamam Nutrition Analysis API is a web-based API that provides nutrition data for a wide variety of foods. It allows developers to easily access information such as the macronutrient and micronutrient composition of foods, as well as information on allergens and food intolerances.

The API is based on an extensive food database that contains information on over a million different foods, including common grocery items, fast food items, and restaurant dishes. Developers can use the API to build applications that help people track their nutrition intake, create meal plans, or manage food allergies.


# Edamam pipeline setup guide

## API token for authentication

1. Create account at edamam website. [link](https://www.edamam.com/)
2. Log in, and then go to accounts >> dashboard.
3. Open application tab, choose create a new application.
4. Choose Nutrition Analysis API, complete required form.
5. Click View on the newly created Nutrition Analysis, you can see the Application ID and Application Keys that will be used later for credentials.

## Initialize the pipeline

  

1. Initialize the pipeline by using the following command

  

```bash
dlt init  edamam  destination
```
*note: currently working destination is duckdb*

  

To know more about destinations, please read the [docs](https://dlthub.com/docs/destinations).

  

1. The file structure for the inited repository shall look like below:

  

```bash

edamam_pipeline

├── .dlt

│ ├──  config.toml

│ └──  secrets.toml

├── edamam

│ └──  __init__.py

│ └──  ReadMe.md

├── requirements.txt

└── edamam_pipeline.py
```

 

1. To learn more about initing please read the docs [“Adding a pipeline”](https://dlthub.com/docs/walkthroughs/add-a-pipeline).

## Configuring credentials

  

1. In .dlt folder is secrets.toml. It should look like the code below

  

```bash
# put your secret values and credentials here. do not share this file and do not push it to github

app_id="set me up"
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
python3 edamam_pipeline.py
```

  

1. After a successful pipeline run, the load job shall be completed and data shall be loaded to destination.
