"""Salesforce source settings and constants"""

from tenacity import stop_after_attempt, wait_exponential

# resource names and the sObjects they load in `salesforce_source`
SOURCE_SOBJECTS = {
    "sf_user": "User",
    "user_role": "UserRole",
    "opportunity": "Opportunity",
    "opportunity_line_item": "OpportunityLineItem",
    "opportunity_contact_role": "OpportunityContactRole",
    "account": "Account",
    "contact": "Contact",
    "lead": "Lead",
    "campaign": "Campaign",
    "campaign_member": "CampaignMember",
    "product_2": "Product2",
    "pricebook_2": "Pricebook2",
    "pricebook_entry": "PricebookEntry",
    "task": "Task",
    "event": "Event",
}

# fields changed on every record update, in order of preference
UPDATED_AT_FIELDS = ("SystemModstamp", "LastModifiedDate")
# fields of insert-only sObjects like field history or login history
CREATED_AT_FIELDS = ("CreatedDate", "LoginTime")
# records may be committed with a timestamp older than the newest one already read
DEFAULT_LAG = 600.0

# max seconds between query job status checks
MAX_POLL_INTERVAL = 10.0
# max seconds to wait for a query job, then it is aborted
MAX_JOB_WAIT = 24 * 3600.0
# REST API rejects longer request URIs
MAX_REST_URL_LENGTH = 16384
# records in a REST API result page, 2000 is the max
REST_BATCH_SIZE = 2000

# a Bulk API result page also ends at this size, so wide records do not fill the memory
MAX_PAGE_BYTES = 64 * 2**20
# bytes read from a streamed result chunk at once
RESPONSE_READ_BYTES = 2**20
# a broken download of a result chunk resumes after the records already read. tenacity strategies
# of the attempts to read the next page: give up after 5, waiting 1, 2, 4 and 8 s in between. The
# wait doubles up to a minute if you allow more attempts
CHUNK_DOWNLOAD_STOP = stop_after_attempt(5)
CHUNK_DOWNLOAD_WAIT = wait_exponential(multiplier=1, max=60)

# pyarrow fails on a record longer than two parse blocks, Salesforce records stay under ~6 MiB
ARROW_BLOCK_SIZE = 8 * 2**20
