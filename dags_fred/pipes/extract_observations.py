from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from dags.core.data_formats import RecordsList, RecordsListGenerator
from dags.core.extraction.connection import JsonHttpApiConnection
from dags.core.pipe import pipe
from dags.core.runnable import PipeContext
from dags.utils.common import utcnow


FRED_API_BASE_URL = "https://api.stlouisfed.org/fred/"
MIN_DATE = datetime(1776, 7, 4)  # 🦅🇺🇸🦅


@dataclass
class ExtractFredObservationsConfig:
    api_key: str
    series_id: str


@dataclass
class ExtractFredObservationsState:
    latest_fetched_at: datetime


@pipe(
    "extract_observations",
    module="fred",
    config_class=ExtractFredObservationsConfig,
    state_class=ExtractFredObservationsState,
)
def extract_fred_observations(ctx: PipeContext) -> RecordsListGenerator:
    api_key = ctx.get_config_value("api_key")
    series_id = ctx.get_config_value("series_id")
    latest_fetched_at = ctx.get_state_value("latest_fetched_at")
    if latest_fetched_at:
        # Two year curing window (to be safe)
        obs_start = latest_fetched_at - timedelta(days=365 * 2)
    else:
        obs_start = MIN_DATE
    params = {
        "file_type": "json",
        "api_key": api_key,
        "series_id": series_id,
        "observation_start": obs_start,
        "offset": 0,
        "limit": 100000,
    }
    conn = JsonHttpApiConnection(date_format="%Y-%m-%d")
    endpoint_url = FRED_API_BASE_URL + "series/observations"
    while True:
        resp = conn.get(endpoint_url, params)
        json_resp = resp.json()
        assert isinstance(json_resp, dict)
        records = json_resp["observations"]
        if len(records) == 0:
            # All done
            break
        for r in records:
            r["series_id"] = params[
                "series_id"
            ]  # Add series ID to data so we know what the data is
            r["value"] = (
                None if r["value"] == "." else r["value"]
            )  # FRED quirk, returns empty decimal number "." instead of null
        yield records
        num_returned = len(records)
        if num_returned < json_resp["limit"]:
            # we got back less than limit, so must be done (no other way to tell?)
            break
        params["offset"] += num_returned
    # We only update date if we have fetched EVERYTHING available as of now
    ctx.emit_state_value("latest_fetched_at", utcnow())


