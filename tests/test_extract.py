import os
import pytest
from unittest.mock import patch, MagicMock
from scripts.extract import extract


# Fake CDC API rows to use in tests
FAKE_BATCH = [
    {
        "data_as_of": "2023-09-27T00:00:00.000",
        "jurisdiction_of_occurrence": "California",
        "mmwryear": "2020",
        "mmwrweek": "1",
        "week_ending_date": "2020-01-04",
        "all_cause": "60179",
        "natural_cause": "55010",
        "diseases_of_heart_i00_i09": "14204",
        "covid_19_u071_multiple_cause_of_death": "0",
        "covid_19_u071_underlying_cause_of_death": "0"
    },
    {
        "data_as_of": "2023-09-27T00:00:00.000",
        "jurisdiction_of_occurrence": "Texas",
        "mmwryear": "2020",
        "mmwrweek": "1",
        "week_ending_date": "2020-01-04",
        "all_cause": "60179",
        "natural_cause": "55010",
        "diseases_of_heart_i00_i09": "14204",
        "covid_19_u071_multiple_cause_of_death": "0",
        "covid_19_u071_underlying_cause_of_death": "0"
    }
]


def make_mock_response(json_data):
    """Helper that returns a fake requests.Response-like object."""
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.raise_for_status.return_value = None
    return mock


@patch("scripts.extract.requests.get")
def test_extract_creates_csv(mock_get, tmp_path, monkeypatch):
    """Extract should save a CSV file to the staging directory."""
    # First call returns a batch, second call returns empty (end of pagination)
    mock_get.side_effect = [
        make_mock_response(FAKE_BATCH),
        make_mock_response([])
    ]

    # Point staging dir to a temp folder so we don't pollute data/staging/
    monkeypatch.setattr("scripts.extract.STAGING_DIR", str(tmp_path))

    output_path = extract()

    assert os.path.exists(output_path), "CSV file was not created"


@patch("scripts.extract.requests.get")
def test_extract_row_count(mock_get, tmp_path, monkeypatch):
    """CSV should contain the same number of rows as records returned by the API."""
    import pandas as pd

    mock_get.side_effect = [
        make_mock_response(FAKE_BATCH),
        make_mock_response([])
    ]

    monkeypatch.setattr("scripts.extract.STAGING_DIR", str(tmp_path))

    output_path = extract()
    df = pd.read_csv(output_path)

    assert len(df) == len(FAKE_BATCH), f"Expected {len(FAKE_BATCH)} rows, got {len(df)}"


@patch("scripts.extract.requests.get")
def test_extract_paginates(mock_get, tmp_path, monkeypatch):
    """Extract should keep fetching until it gets an empty response."""
    mock_get.side_effect = [
        make_mock_response(FAKE_BATCH),
        make_mock_response(FAKE_BATCH),
        make_mock_response([])
    ]

    monkeypatch.setattr("scripts.extract.STAGING_DIR", str(tmp_path))

    output_path = extract()
    import pandas as pd
    df = pd.read_csv(output_path)

    # Two batches of 2 rows each = 4 total
    assert len(df) == 4, f"Expected 4 rows from 2 pages, got {len(df)}"
