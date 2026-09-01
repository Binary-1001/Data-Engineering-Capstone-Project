from datetime import datetime

VOICE_RATE_PER_SEC = 1.0 / 60
DATA_RATE_PER_BYTE = 49.0 / 1e9
_wak_per_zar = 1.0


def set_forex_rate(rate: float):
    """Updates the WAK/ZAR conversion rate used for cost calculations.
    Call this whenever a new forex tick is received to keep costs accurate."""
    global _wak_per_zar
    _wak_per_zar = rate


def _to_wak(zar: float) -> float:
    """Converts a ZAR amount to WAK using the current forex rate."""
    return zar * _wak_per_zar


def aggregate_data_event(state: dict, record: dict):
    """Adds a single CDR data record into the in-memory aggregation state.
    Groups by (msisdn, date, data_type) and accumulates upload bytes,
    download bytes and cost in WAK. If a bucket for that key already
    exists the values are added on top — this naturally handles
    late-arriving events since they just increment the correct bucket."""
    msisdn = record["msisdn"]
    data_type = record["data_type"]
    date = datetime.fromisoformat(str(record["event_datetime"])).date().isoformat()
    key = (msisdn, date, data_type)

    entry = state.setdefault(key, {
        "msisdn": msisdn, "date": date, "usage_type": data_type,
        "up_bytes": 0, "down_bytes": 0, "cost_wak": 0.0
    })
    up, down = int(record["up_bytes"]), int(record["down_bytes"])
    entry["up_bytes"] += up
    entry["down_bytes"] += down
    entry["cost_wak"] += _to_wak((up + down) * DATA_RATE_PER_BYTE)


def aggregate_voice_event(state: dict, record: dict):
    """Adds a single CDR voice record into the in-memory aggregation state.
    Groups by (msisdn, date, call_type) and accumulates call duration in
    seconds and cost in WAK. Same late-arrival handling as aggregate_data_event
    — any event for a past date simply increments the right bucket."""
    msisdn = record["msisdn"]
    call_type = record["call_type"]
    date = datetime.fromisoformat(str(record["start_time"])).date().isoformat()
    key = (msisdn, date, call_type)

    entry = state.setdefault(key, {
        "msisdn": msisdn, "date": date, "usage_type": call_type,
        "call_duration_sec": 0, "cost_wak": 0.0
    })
    duration = int(record["call_duration_sec"])
    entry["call_duration_sec"] += duration
    entry["cost_wak"] += _to_wak(duration * VOICE_RATE_PER_SEC)
