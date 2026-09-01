import logging
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.auth import verify_credentials
from app.database import get_db
from app.models import UsageResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

app = FastAPI(title="Usage API", description="CDR usage statistics for external consumers")

DATETIME_FORMAT = "%Y%m%d%H%M%S"


def parse_datetime(value: str, field: str) -> datetime:
    """Parses a YYYYMMDDHHmmss string into a datetime. Raises 400 if invalid."""
    try:
        return datetime.strptime(value, DATETIME_FORMAT)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} format. Expected YYYYMMDDHHmmss e.g. 20240101000000")


def get_usage(db: Session, msisdn: str, start_time: datetime, end_time: datetime) -> list:
    """Queries data and voice summary tables for a given MSISDN and date range."""
    data_rows = db.execute(text("""
        SELECT usage_type, up_bytes + down_bytes AS total, date
        FROM prepared_layers.cdr_daily_data_summary
        WHERE msisdn = :msisdn
          AND date BETWEEN :start_date AND :end_date
        ORDER BY date
    """), {"msisdn": msisdn, "start_date": start_time.date(), "end_date": end_time.date()}).fetchall()

    voice_rows = db.execute(text("""
        SELECT usage_type, call_duration_sec AS total, date
        FROM prepared_layers.cdr_daily_voice_summary
        WHERE msisdn = :msisdn
          AND date BETWEEN :start_date AND :end_date
        ORDER BY date
    """), {"msisdn": msisdn, "start_date": start_time.date(), "end_date": end_time.date()}).fetchall()

    usage = []
    for row in data_rows:
        usage.append({"category": "data", "usage_type": row.usage_type, "total": row.total, "measure": "bytes", "start_time": str(row.date) + " 00:00:00"})
    for row in voice_rows:
        usage.append({"category": "call", "usage_type": row.usage_type, "total": row.total, "measure": "seconds", "start_time": str(row.date) + " 00:00:00"})

    return usage


@app.get("/data_usage", response_model=UsageResponse)
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
