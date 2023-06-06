# How to create credentials

1. You must have Ads Manager active for your facebook account
2. Find your account id. It is a long number against Account Overview dropdown or in the link ie. https://adsmanager.facebook.com/adsmanager/manage/accounts?act=10150974068878324
3. Create new facebook app. For that you need developers account. The app must be a Business app.
4. To get short lived access token use https://developers.facebook.com/tools/explorer/
5. Select app you just created
6. Select get user access token
7. Add permissions: `ads_read`, `leads_retrieval` (to retrieve the leads)
8. Generate token
9. Exchange the token for the long lived access token.

## Access token rotation
The long lived token is valid for 60 days. The replacement process, to our best knowledge, is manual and exactly as the one above. We provide you a few help functions and an expiration notifications to make it slightly less painful.

## Credentials layout

You can place all the config values in `secrets.toml`:
```toml
[sources.facebook_ads]
access_token="set me up!"
account_id="set me up"
```

We strongly advice you to add the token expiration timestamp to get notified a week before token expiration that you need to rotate it. Right now the notifications are sent to logger with `error` level. (slack and mail support are coming soon!). In `config.toml` / `secrets.toml`
```toml
[sources.facebook_ads]
access_token_expires_at=1688821881
```

## Helper functions for long lived tokens and to get token expiration time
In `fb.py` we provide two functions that make your life easier. To use the functions you should add `app_id` and `app_secret` of the Facebook app that you use to generate credentials to your `secrets.toml`.
```toml
[sources.facebook_ads]
client_id="app_id"
client_secret="app_secret"
```

and then ie from Python interactive command prompt you can get the long lived access token
```python
from facebook_ads import get_long_lived_token
print(get_long_lived_token("your short lived token"))
```

or get the expiration date, scopes etc. of the token you use
```python
from facebook_ads.fb import debug_access_token
debug_access_token()
```

# Rate limits
This source detects facebook rate limits and retries with exponential back-off. Often it takes several minutes or longer for the source to make more requests. Retries based on facebook `error` documents are logged on warning level.

# Hacking the source

in `settings.py`
- change the default fields for all objects
- add new insights breakdown presets

in `fb.py`:
- change the retry settings and add new retry conditions
- change the timeouts when executing jobs
