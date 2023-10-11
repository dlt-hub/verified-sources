from typing import Optional
from dlt.common.configuration.specs import BaseConfiguration, configspec

@configspec
class KinesisResourceConfiguration(BaseConfiguration):
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    aws_region: Optional[str]