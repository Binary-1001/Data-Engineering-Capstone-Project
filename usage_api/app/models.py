from pydantic import BaseModel
from typing import List


class UsageEntry(BaseModel):
    """A single usage record — either data (bytes) or a call (seconds)."""
    category: str
    usage_type: str
    total: int
    measure: str
    start_time: str


class UsageResponse(BaseModel):
    """Full response body for the /data_usage endpoint."""
    msisdn: str
    start_time: str
    end_time: str
    usage: List[UsageEntry]
