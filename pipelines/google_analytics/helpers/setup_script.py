from dlt.sources.helpers.requests import client

client_id = ""
client_secret = ""
get_code_url = f"https://accounts.google.com/o/oauth2/v2/auth?redirect_uri=https%3A%2F%2Flocalhost&prompt=consent&response_type=code&client_id={client_id}&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fanalytics.readonly&access_type=offline"
print(f"Click on the following url to get the authorization code and then input the user code: {get_code_url}")
authorization_code = input("Enter the authorization code: ")


url = 'https://oauth2.googleapis.com/token'
data = {
    'code': authorization_code,
    'client_id': client_id,
    'client_secret': client_secret,
    'redirect_uri': 'https://localhost',
    'grant_type': 'authorization_code'
}
response = client.post(url, data=data)
response_data = response.json()
# access_token = response_data['access_token']
# refresh_token = response_data['refresh_token']
print(response_data)
