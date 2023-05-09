import time
from typing import Dict
import humanize

from facebook_business import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.jobsjob import JobsJob
from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.lead import Lead

import dlt
from dlt.common import logger, pendulum
from dlt.common.configuration.inject import with_config
from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Client

from .exceptions import InsightsJobTimeout


@with_config(sections=("sources", "facebook_ads"))
def notify_on_token_expiration(
    access_token_expires_at: int = None
) -> None:
    if not access_token_expires_at:
        logger.warning("Token expiration time notification disabled. Configure token expiration timestamp in access_token_expires_at config value")
    else:
        expires_at = pendulum.from_timestamp(access_token_expires_at)
        if expires_at < pendulum.now().add(days=7):
            logger.error(f"Access Token expires in {humanize.precisedelta(pendulum.now() - expires_at)}. Replace the token now!")


@with_config(sections=("sources", "facebook_ads"))
def debug_access_token(
    access_token: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value
) -> str:
    debug_url = f'https://graph.facebook.com/debug_token?input_token={access_token}&access_token={client_id}|{client_secret}'
    response = requests.get(debug_url)
    data: Dict[str, str] = response.json()

    if 'error' in data:
        raise Exception(f"Error debugging token: {data['error']}")

    return data['data']


@with_config(sections=("sources", "facebook_ads"))
def get_long_lived_token(
    access_token: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value
) -> str:

    exchange_url = f"https://graph.facebook.com/v13.0/oauth/access_token?grant_type=fb_exchange_token&client_id={client_id}&client_secret={client_secret}&fb_exchange_token={access_token}"
    response = requests.get(exchange_url)
    data: Dict[str, str] = response.json()

    if 'error' in data:
        raise Exception(f"Error refreshing token: {data['error']}")

    return data["access_token"]


def get_ads_account(account_id: str, access_token: str, request_timeout: float) -> AdAccount:
    notify_on_token_expiration()

    def retry_on_limit(response: requests.Response, exception: BaseException) -> bool:
        try:
            code = response.json()["error"]["code"]
            return code in (1, 2, 4, 17, 341, 32, 613, *range(80000, 80007), 800008, 800009, 80014)
        except Exception:
            return False

    retry_session = Client(timeout=request_timeout, raise_for_status=False, condition=retry_on_limit, max_attempts=12, backoff_factor=2).session
    retry_session.params.update({  # type: ignore
            'access_token': access_token
        })
    # patch dlt requests session with retries
    API = FacebookAdsApi.init(account_id="act_" + account_id, access_token=access_token)
    API._session.requests = retry_session
    user = User(fbid='me')

    accounts = user.get_ad_accounts()
    account: AdAccount = None
    for acc in accounts:
        if acc['account_id'] == account_id:
            account = acc

    if not account:
        raise ValueError("Couldn't find account with id {}".format(account_id))

    return account


JOB_TIMEOUT_INFO = """This is an intermittent error and may resolve itself on subsequent queries to the Facebook API.
You should remove the fields in `fields` argument that are not necessary, as that may help improve the reliability of the Facebook API."""


def execute_job(
    job: JobsJob,
    insights_max_wait_to_start_seconds: int = 5 * 60,
    insights_max_wait_to_finish_seconds: int = 30 * 60,
    insights_max_async_sleep_seconds: int = 5 * 60
) -> JobsJob:
    status: str = None
    time_start = time.time()
    sleep_time = 10
    while status != "Job Completed":
        duration = time.time() - time_start
        job = job.api_get()
        status = job['async_status']
        percent_complete = job['async_percent_completion']

        job_id = job['id']
        logger.info('%s, %d%% done', status, percent_complete)

        if status == "Job Completed":
            return job

        if duration > insights_max_wait_to_start_seconds and percent_complete == 0:
            pretty_error_message = 'Insights job {} did not start after {} seconds. ' + JOB_TIMEOUT_INFO
            raise InsightsJobTimeout(
                "facebook_insights",
                pretty_error_message.format(job_id, insights_max_wait_to_start_seconds)
            )
        elif duration > insights_max_wait_to_finish_seconds and status != "Job Completed":
            pretty_error_message = 'Insights job {} did not complete after {} seconds. ' + JOB_TIMEOUT_INFO
            raise InsightsJobTimeout(
                "facebook_insights",
                pretty_error_message.format(job_id,insights_max_wait_to_finish_seconds//60)
            )

        logger.info("sleeping for %d seconds until job is done", sleep_time)
        time.sleep(sleep_time)
        if sleep_time < insights_max_async_sleep_seconds:
            sleep_time = 2 * sleep_time
    return job
