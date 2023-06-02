
# Firebase Database

  

[Firebase](https://firebase.google.com/)  is an app development platform that helps you build and grow apps and games users love. Backed by Google and trusted by millions of businesses around the world.

## Create Google Cloud Service Account

1.  Go to the Google Cloud Console: Visit the Google Cloud Console website at [https://console.cloud.google.com/](https://console.cloud.google.com/).
    
2.  Sign in to your Google Cloud account: Sign in using your Google account credentials. If you don't have a Google Cloud account, you will need to create one.
    
3.  Select a project (or create a new one): If you have an existing project, select it from the project dropdown menu at the top of the page. If you don't have a project yet, click on the project dropdown menu and choose "New Project." Follow the prompts to create a new project.
    
4.  Open the IAM & Admin page: In the Cloud Console, click on the navigation menu (☰) in the upper-left corner, then select "IAM & Admin" from the menu.
    
5.  Click on "Service Accounts": In the IAM & Admin page, click on the "Service Accounts" tab in the left sidebar.
    
6.  Click on "Create Service Account": On the Service Accounts page, click on the "Create Service Account" button.
    
7.  Provide a name and ID for the service account: Enter a name and ID for the service account. Optionally, you can also provide a description. Click on "Create" to proceed.
    
8.  Configure the service account's permissions (optional): On the "Service account permissions" step, you can choose the roles and permissions you want to assign to the service account. You can either select predefined roles or create custom roles. Once you have configured the permissions, click on "Continue" to proceed.
    
9.  Create and download a key pair (JSON key file): On the "Create key" step, select "JSON" as the key type and click on the "Create" button. This will generate a JSON key file for the service account and download it to your computer. Keep this file secure, as it grants access to your Google Cloud resources.
    
10.  Finish creating the service account: Review the summary of the service account details and click on the "Done" button to finish creating the service account. 

  

## Setup Firebase Account and Project

  
To set up a Firebase account, follow these steps:

1.  Visit the Firebase website: Go to the Firebase website at [https://firebase.google.com/](https://firebase.google.com/).
    
2.  Sign in with your Google account: Click on the "Get started" button and sign in with your Google account. If you don't have a Google account, you'll need to create one.
    
3.  Create a new Firebase project: Click on the "Add project" button to create a new Firebase project. Enter a project name and select your country/region. You can also choose to enable Google Analytics for your project if desired. Click on "Continue."
    
4.  Set up Google Analytics (optional): If you chose to enable Google Analytics, you will need to set up your analytics account. Follow the on-screen instructions to complete the setup process.
    
5.  Accept the terms and conditions: Review the terms and conditions and check the box to accept them. Then click on the "Create project" button.
    
6.  Navigate to the Firebase console: Once your project is created, you will be taken to the Firebase console. Here, you can manage your project's settings and configure various Firebase services.


## Initialize the pipeline

  

We can now create the pipeline.

  

Initialize a `dlt` project with the following command:

  

`dlt init firebase duckdb`

  

Here, we chose duckdb as the destination. To choose a different destination, replace `duckdb` with your choice of destination.

  

Running this command will create a directory with the following structure:

  

```sql

directory

├── .dlt

│ ├── config.toml

│ └── secrets.toml

└── firebase

├── __init__.py

├── README.md

└── requirements.txt

```

  

## Add credentials

  

1. Open `.dlt/secrets.toml`.

2. From the .json that you downloaded earlier, copy `project_id`, `private_key`, and `client_email` under `[sources.google_spreadsheet.credentials]`.

  

```python

[sources.google_analytics.credentials]

project_id = "set me up"  # GCP Source project ID!

private_key = "set me up"  # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)

client_email = "set me up"  # Email for source service account

location = "set me up"  #Project Location For ex. “US”

[sources.firebase]
project_id="set me up!" # GCP Source project ID

private_key_id="set me up!" 

private_key="set me up!" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)

database_url="set me up!" # Database URL that will be used for pipeline usually comes in https://<project_name>.<server_locationb>.firebasedatabase.app/


```

3. Alternatively, if you're using service account credentials, replace the the fields and values with those present in the credentials .json that you generated above.

4. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/destinations#google-bigquery).



## Run the pipeline

  

1. Install the requirements by using the following command

  

`pip install -r requirements.txt`

  

2. Run the pipeline by using the following command

  

`python3 firebase.py`

  

3. Make sure that everything is loaded as expected, by using the command:

  

`dlt pipeline <pipeline_name> show`

  

For example, the pipeline_name for the above pipeline is `dlt_firebase_pipeline`

To change this, you can replace it in

`dlt.pipeline(pipeline_name="dlt_firebase_pipeline", ... )`

