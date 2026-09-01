import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app, get_usage
from app.database import get_db

import base64

AUTH = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
VALID_PARAMS = {"msisdn": "2712345678", "start_time": "20240101000000", "end_time": "20240101235959"}


@pytest.fixture
def client():
    db = MagicMock()
    app.dependency_overrides[get_db] = lambda: db
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def test_missing_auth_returns_401(client):
    response = client.get("/data_usage", params=VALID_PARAMS)
    assert response.status_code == 401


def test_wrong_credentials_returns_401(client):
    bad_auth = {"Authorization": "Basic " + base64.b64encode(b"wrong:creds").decode()}
    response = client.get("/data_usage", params=VALID_PARAMS, headers=bad_auth)
    assert response.status_code == 401


def test_invalid_start_time_format_returns_400(client):
    params = {**VALID_PARAMS, "start_time": "not-a-date"}
    response = client.get("/data_usage", params=params, headers=AUTH)
    assert response.status_code == 400


def test_invalid_end_time_format_returns_400(client):
    params = {**VALID_PARAMS, "end_time": "99999999"}
    response = client.get("/data_usage", params=params, headers=AUTH)
    assert response.status_code == 400


def test_start_time_after_end_time_returns_400(client):
    params = {**VALID_PARAMS, "start_time": "20240102000000", "end_time": "20240101000000"}
    response = client.get("/data_usage", params=params, headers=AUTH)
    assert response.status_code == 400


def test_no_usage_data_returns_404(client):
    with patch("app.main.get_usage", return_value=[]):
        response = client.get("/data_usage", params=VALID_PARAMS, headers=AUTH)
    assert response.status_code == 404


def test_valid_request_returns_200_with_correct_shape(client):
    mock_usage = [
        {"category": "data", "usage_type": "video", "total": 500000, "measure": "bytes", "start_time": "2024-01-01 00:00:00"},
        {"category": "call", "usage_type": "voice", "total": 120, "measure": "seconds", "start_time": "2024-01-01 00:00:00"},
    ]
    with patch("app.main.get_usage", return_value=mock_usage):
        response = client.get("/data_usage", params=VALID_PARAMS, headers=AUTH)

    assert response.status_code == 200
    body = response.json()
    assert body["msisdn"] == "2712345678"
    assert body["start_time"] == "2024-01-01 00:00:00"
    assert body["end_time"] == "2024-01-01 23:59:59"
    assert len(body["usage"]) == 2
    assert body["usage"][0]["category"] == "data"
    assert body["usage"][1]["category"] == "call"
