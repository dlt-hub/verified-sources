# Inbox Source

This source provides functionalities to collect inbox emails, get the attachment as a [file items](../filesystem/README.md#the-fileitem-file-representation),
and store all relevant email information in a destination. It utilizes the `imaplib` library to
interact with the IMAP server, and `dlt` library to handle data processing and transformation.

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
2. Replace the placeholders in `.dlt/secrets.toml` with your IMAP server hostname, email account
   credentials.


## Functionality
You access the messages and attachments via `inbox_source` `dlt` source. This source exposes the resources
as described below. Typically you'll pick one of those (ie. **messages**) and load it into a table with
a specific name:
```python
# get messages resource from the source
messages = inbox_source(
    filter_emails=("astra92293@gmail.com", "josue@sehnem.com")
).messages
# configure the messages resource to not get bodies of the messages
messages = messages(include_body=False).with_name("my_inbox")
# load messages to "my_inbox" table
load_info = pipeline.run(messages)
```
This way you can create several extract pipelines with different combination of filters and processing steps from a single `inbox_source`.

### Additional `inbox_source` arguments
Please refer to `inbox_source()` docstring for options to filter email messages and attachments by sender, date or mime type.

### messages resource

This resource fetches the corresponding email messages using their UID. It yields a dictionary containing email
metadata such as message UID, message ID, sender, subject, date, modification date, content type,
and email body.

Please refer to **imap_read_messages()** example pipeline in **inbox_pipeline.py** that loads messages from particulars into
**duckdb**.

### attachments resource

This resource extracts attachments from the email message using their UID. It connects to
the IMAP server, fetches the email message by its UID, parses body and looks for attachments.
It yields [file items](../filesystem/README.md#the-fileitem-file-representation) where attachments
are loaded in the **file_content** field. The original email message is present in **message** filed.

Please refer to **imap_get_attachments()** example pipeline in **inbox_pipeline.py** that get **pdf** attachments
from emails from particular senders, parsed pdfs and writes content into **duckdb**.

### uids resource

This is a dlt resource that connects to the IMAP server, logs in to the email account, and fetches
email messages from the specified folder ('INBOX' by default). It yields a dictionary containing
only message UID. You typically do not need to use this resource.

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
2. Select Security.
3. Under "How you sign in to Google", select **2-Step Verification** -> Turn it on.
4. Select again **2-Step Verification**.
5. At the bottom of the page, select App passwords.
6. Enter a name of device that helps you remember where you’ll use the app password.
7. Select Generate.
8. To enter the app password, follow the instructions on your screen. The app password is the
   16-character code that generates on your device.
9. Select Done.

Read more in
[this article](https://pythoncircle.com/post/727/accessing-gmail-inbox-using-python-imaplib-module/)
or
[Google official documentation.](https://support.google.com/mail/answer/185833#zippy=%2Cwhy-you-may-need-an-app-password)

Step 2: Turn on IMAP in Gmail
1. In Gmail, in the top right, click Settings -> See all settings.
2. At the top, click the Forwarding and POP/IMAP tab.
3. In the IMAP Access section, select Enable IMAP.
4. At the bottom, click Save Changes.

Read more in [official Google documentation.](https://support.google.com/a/answer/9003945#zippy=%2Cstep-turn-on-imap-in-gmail)