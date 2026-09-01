from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text


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
