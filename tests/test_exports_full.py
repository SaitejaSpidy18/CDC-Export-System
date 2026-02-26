import csv
from pathlib import Path
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import engine
client = TestClient(app)
OUTPUT_DIR = Path("output")
def _get_non_deleted_user_count():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM users WHERE is_deleted = FALSE;"))
        return result.scalar_one()
def test_full_export_creates_csv_and_updates_watermark():
    consumer_id = "test-consumer-full"
    response = client.post(
        "/exports/full",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert response.status_code == 202
    data = response.json()
    filename = data["outputFilename"]
    csv_path = OUTPUT_DIR / filename
    assert csv_path.exists(), "CSV file not created"
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    assert rows, "CSV file is empty"
    header = rows[0]
    assert header == ["id", "name", "email", "created_at", "updated_at", "is_deleted"]
    data_rows = rows[1:]
    csv_rows = len(data_rows)
    db_count = _get_non_deleted_user_count()
    assert csv_rows == db_count
    wm_response = client.get(
        "/exports/watermark",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert wm_response.status_code == 200
    wm_data = wm_response.json()
    assert wm_data["consumerId"] == consumer_id
    assert isinstance(wm_data["lastExportedAt"], str)