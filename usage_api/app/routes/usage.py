import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.auth import verify_credentials
from app.database import get_db
from app.models import UsageResponse
from app.services.usage_service import get_usage

logger = logging.getLogger(__name__)
router = APIRouter()

DATETIME_FORMAT = "%Y%m%d%H%M%S"


def parse_datetime(value: str, field: str) -> datetime:
    """Parses a YYYYMMDDHHmmss string into a datetime. Raises 400 if invalid."""
    try:
        return datetime.strptime(value, DATETIME_FORMAT)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} format. Expected YYYYMMDDHHmmss e.g. 20240101000000")


@router.get("/data_usage", response_model=UsageResponse)
def data_usage(
    msisdn: str,
    start_time: str,
    end_time: str,
    db: Session = Depends(get_db),
    username: str = Depends(verify_credentials)
):
    """Returns daily usage summaries for a given MSISDN and time range."""
    logger.info(f"Request from [{username}] — msisdn={msisdn} start={start_time} end={end_time}")

    if not msisdn:
        raise HTTPException(status_code=400, detail="msisdn is required")

    start_dt = parse_datetime(start_time, "start_time")
    end_dt = parse_datetime(end_time, "end_time")

    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    try:
        usage = get_usage(db, msisdn, start_dt, end_dt)
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    if not usage:
        raise HTTPException(status_code=404, detail=f"No usage data found for msisdn {msisdn}")

    return UsageResponse(
        msisdn=msisdn,
        start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        usage=usage
    )
