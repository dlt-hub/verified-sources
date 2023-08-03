# Inbox Source

This source provides functionalities to collect inbox emails, download attachments to a local
folder, and store all relevant email information in a destination. It utilizes the `imaplib` library
to interact with the IMAP server, and `dlt` library to handle data processing and transformation.

## Prerequisites

- Python 3.x
- `dlt` library (you can install it using `pip install dlt`)
- destination dependencies, e.g. `duckdb` (`pip install duckdb`)

## Installation

Make sure you have Python 3.x installed on your system.

Install the required library by running the following command:

```shell
pip install dlt[duckdb]
```

## Initialize the source

Initialize the source with dlt command:

```shell
dlt init inbox duckdb
```

## Set email account credentials

1. Open `.dlt/secrets.toml`.
1. Enter the email account secrets:
   ```toml
   [sources.inbox]
   host = 'imap.example.com'
   email_account = "example@example.com"
   password = 'set me up!'
   ```

Use [App password](#getting-gmail-app-password) to set the password for a Gmail account.

## Usage

1. Ensure that the email account you want to access allows access by less secure apps (or use an
   [app password](#getting-gmail-app-password)).
1. Replace the placeholders in `.dlt/secrets.toml` with your IMAP server hostname, email account
   credentials.
1. Customize the FILTER_EMAILS list in the `inbox/settings.py` file if you want to fetch emails only
   from specific senders.
1. Customize the GMAIL_GROUP in the `inbox/settings.py` file if you want to fetch emails
   for specific Google Group.
1. Set the STORAGE_FOLDER_PATH in the `inbox/settings.py` file to the folder where you want to save
   attachments (if required).
1. Set the DEFAULT_START_DATE in the `inbox/settings.py` file to the date you want to fetch emails from.

## Functionality

### inbox_source source

This is a dlt source that collects inbox emails and, if `attachments=True`, downloads attachments to a
local folder (STORAGE_FOLDER_PATH) based on the specified parameters.

### messages_uids resource

This is a dlt resource that connects to the IMAP server, logs in to the email account, and fetches email messages
from the specified folder ('INBOX' by default). It yields a dictionary containing only message UID.

### read_messages resource

This dlt transformer resource takes an email message items from another resource (e.g. `messages_uids`)
and fetches the corresponding email messages using their UID. It yields a dictionary containing email metadata
such as message UID, message ID, sender, subject, date, modification date, content type, and email body.

### get_attachments_by_uid resource

This dlt transformer resource takes an email message items from another resource (e.g. `messages_uids`)
and extracts attachments from the email message using their UID.
It connects to the IMAP server,
fetches the email message by its UID, and saves attachments to the specified STORAGE_FOLDER_PATH.
It yields a dictionary containing email metadata such as message UID, sender,
date, content type, etc., and under the key "envelope" it returns dict with
the attachment content type, file name, and local file path.

## Example

Here's an example of how to use the `inbox_source` to fetch inbox emails and save attachments to a
local folder:

```python
import dlt

from inbox import inbox_source


# configure the pipeline with your destination details
pipeline = dlt.pipeline(
    pipeline_name="inbox",
    destination="duckdb",
    dataset_name="inbox_data",
    full_refresh=False,
)

data_source = inbox_source(attachments=True, filter_by_mime_type=("application/pdf",))
# run the pipeline with your parameters
load_info = pipeline.run(data_source)
# pretty print the information on data that was loaded
print(load_info)
```

## Accessing Gmail Inbox

To connect to the Gmail server, we need the below information.

- SMTP server DNS. Its value will be 'imap.gmail.com' in our case.
- SMTP server port. The value will be 993. This port is used for Internet message access protocol
  over TLS/SSL.

### Set up Gmail with a third-party email client

An app password is a 16-digit passcode that gives a less secure app or device permission to access
your Google Account. App passwords can only be used with accounts that have 2-Step Verification
turned on.

Step 1: Create and use App Passwords
1. Go to your Google Account.
1. Select Security.
1. Under "How you sign in to Google", select **2-Step Verification** -> Turn it on.
1. Select again **2-Step Verification**.
1. At the bottom of the page, select App passwords.
1. Enter a name of device that helps you remember where you’ll use the app password.
1. Select Generate.
1. To enter the app password, follow the instructions on your screen. The app password is the
   16-character code that generates on your device.
1. Select Done.

Read more in
[this article](https://pythoncircle.com/post/727/accessing-gmail-inbox-using-python-imaplib-module/)
or
[Google official documentation.](https://support.google.com/mail/answer/185833#zippy=%2Cwhy-you-may-need-an-app-password)

Step 2: Turn on IMAP in Gmail
1. In Gmail, in the top right, click Settings -> See all settings.
1. At the top, click the Forwarding and POP/IMAP tab.
1. In the IMAP Access section, select Enable IMAP.
1. At the bottom, click Save Changes.

Read more in [official Google documentation.](https://support.google.com/a/answer/9003945#zippy=%2Cstep-turn-on-imap-in-gmail)