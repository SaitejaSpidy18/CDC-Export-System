import csv
from datetime import datetime
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import engine
client = TestClient(app)
def test_incremental_export_exports_only_updated_rows():
    consumer_id = "test-consumer-incremental"
    # 1) Run full export to create watermark
    resp_full = client.post(
        "/exports/full",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert resp_full.status_code == 202
    # 2) Update 5 rows
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE users
            SET updated_at = NOW()
            WHERE id IN (SELECT id FROM users WHERE is_deleted = FALSE LIMIT 5);
        """))
    # 3) Trigger incremental export
    resp_incr = client.post(
        "/exports/incremental",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert resp_incr.status_code == 202
    incr_filename = resp_incr.json()["outputFilename"]
    from pathlib import Path
    csv_path = Path("output") / incr_filename
    assert csv_path.exists()
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    assert rows, "Incremental CSV is empty"
    header = rows[0]
    assert header == ["id", "name", "email", "created_at", "updated_at", "is_deleted"]
    data_rows = rows[1:]
    assert len(data_rows) == 5
    wm_resp = client.get(
        "/exports/watermark",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert wm_resp.status_code == 200
    wm_data = wm_resp.json()
    datetime.fromisoformat(wm_data["lastExportedAt"])