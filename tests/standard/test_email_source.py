from sources.standard.inbox import get_attachments, get_message_content, messages_uids


def test_load_attachments() -> None:
    filter_emails = ("josue@sehnem.com",)
    uids = messages_uids(filter_emails=filter_emails)

    attachments = list(uids | get_attachments)

    for item in attachments:
        # Make sure just filtered emails are processed
        assert "josue@sehnem.com" in item["From"]
        if item["file_name"] == "sample.txt":
            # Find the attachment with the file name and assert the loaded content
            content = item.read_bytes()
            assert item["file_name"] == "sample.txt"
            assert content == b"dlthub content"


def test_load_messages() -> None:
    filter_emails = ("josue@sehnem.com",)
    uids = messages_uids(filter_emails=filter_emails)

    messages = list(uids | get_message_content(include_body=True))

    for item in messages:
        # Make sure just filtered emails are processed
        assert "josue@sehnem.com" in item["From"]
        if item["message_uid"] == 22:
            assert item["body"] == "test body\r\n"
            assert item["Subject"] == "test subject"
