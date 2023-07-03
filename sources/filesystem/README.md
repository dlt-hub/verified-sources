## Google Drive API

[Read Quick Start with Google Drive:](https://developers.google.com/drive/api/quickstart/python?hl=en)

1. Enable Google Drive API.
2. Configure the OAuth consent screen.
3. Create credentials json.
4. Save the path to json in .dlt/secrets.toml:
    ```
    [sources.filesystem.google_drive]
    credentials_path = '/path/to/your/credentials.json'
    folder_id = 'folder_id'
    storage_folder_path = './temp'
    ```