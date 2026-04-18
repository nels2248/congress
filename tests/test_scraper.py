"""
tests/test_scraper.py
─────────────────────
Basic sanity tests — run with:  pytest tests/
No real API calls are made (everything is mocked).
"""

import json
import pytest
import duckdb
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper import get_db, scrape, export_json

# ── Fixtures ────────────────────────────────────────────────────────────────
MOCK_BILL = {
    "congress": 119,
    "type": "HR",
    "number": "1",
    "title": "A mock bill for testing",
    "introducedDate": "2025-01-15",
    "latestAction": {"text": "Referred to committee", "actionDate": "2025-01-16"},
    "sponsors": [{"fullName": "Jane Doe", "party": "D", "state": "MN"}],
    "originChamber": "House",
}


@pytest.fixture
def mem_db():
    """In-memory DuckDB for testing (does not touch congress.db)."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE bills (
            bill_id VARCHAR PRIMARY KEY, congress INTEGER, bill_type VARCHAR,
            bill_number VARCHAR, title VARCHAR, introduced_date DATE,
            latest_action VARCHAR, latest_action_date DATE,
            sponsor_name VARCHAR, sponsor_party VARCHAR, sponsor_state VARCHAR,
            origin_chamber VARCHAR, updated_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE scrape_log (run_at TIMESTAMP, congress INTEGER,
                                  bills_added INTEGER, status VARCHAR)
    """)
    yield con
    con.close()


# ── Tests ────────────────────────────────────────────────────────────────────
def test_db_schema(mem_db):
    tables = {r[0] for r in mem_db.execute("SHOW TABLES").fetchall()}
    assert "bills" in tables


def test_scrape_inserts_bill(mem_db, tmp_path):
    mock_response = MagicMock()
    mock_response.json.side_effect = [
        {"bills": [MOCK_BILL]},
        {"bills": []},          # second call returns empty → stop
    ]
    mock_response.raise_for_status = MagicMock()

    with patch("scraper.httpx.get", return_value=mock_response):
        count = scrape(mem_db, congress=119)

    assert count == 1
    row = mem_db.execute("SELECT * FROM bills WHERE bill_id = '119-HR-1'").fetchone()
    assert row is not None
    assert row[4] == "A mock bill for testing"


def test_export_json(mem_db, tmp_path, monkeypatch):
    mem_db.execute("""
        INSERT INTO bills VALUES
        ('119-HR-1',119,'HR','1','Test Bill','2025-01-15',
         'Referred','2025-01-16','Jane Doe','D','MN','House',NOW())
    """)
    monkeypatch.setattr("scraper.DATA_DIR", tmp_path)
    export_json(mem_db)

    bills = json.loads((tmp_path / "bills.json").read_text())
    assert len(bills) == 1
    assert bills[0]["sponsor_party"] == "D"

    party = json.loads((tmp_path / "by_party.json").read_text())
    assert party[0]["party"] == "D"
    assert party[0]["count"] == 1


def test_duplicate_bill_upsert(mem_db):
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    updated_bill = {**MOCK_BILL, "title": "Updated title"}
    mock_response.json.side_effect = [
        {"bills": [MOCK_BILL]}, {"bills": []},
        {"bills": [updated_bill]}, {"bills": []},
    ]
    with patch("scraper.httpx.get", return_value=mock_response):
        scrape(mem_db, congress=119)
        scrape(mem_db, congress=119)

    total = mem_db.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
    title = mem_db.execute("SELECT title FROM bills WHERE bill_id='119-HR-1'").fetchone()[0]
    assert total == 1                  # no duplicate rows
    assert title == "Updated title"   # upsert updated the row
