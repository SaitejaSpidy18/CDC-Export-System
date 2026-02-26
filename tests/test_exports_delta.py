import csv
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import engine
client = TestClient(app)
def test_delta_export_includes_insert_update_delete():
    consumer_id = "test-consumer-delta"
    # 1) Run full export to create watermark
    resp_full = client.post(
        "/exports/full",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert resp_full.status_code == 202
    # 2) Insert new, update one, soft-delete one
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO users (name, email, created_at, updated_at, is_deleted)
            VALUES ('New User', 'new_user_delta@example.com', NOW(), NOW(), FALSE);
        """))
        conn.execute(text("""
            UPDATE users
            SET name = 'Updated Name', updated_at = NOW()
            WHERE id = (SELECT id FROM users WHERE is_deleted = FALSE LIMIT 1);
        """))
        conn.execute(text("""
            UPDATE users
            SET is_deleted = TRUE, updated_at = NOW()
            WHERE id = (SELECT id FROM users WHERE is_deleted = FALSE LIMIT 1);
        """))
    # 3) Trigger delta export
    resp_delta = client.post(
        "/exports/delta",
        headers={"X-Consumer-ID": consumer_id},
    )
    assert resp_delta.status_code == 202
    filename = resp_delta.json()["outputFilename"]
    from pathlib import Path
    csv_path = Path("output") / filename
    assert csv_path.exists()
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    assert rows, "Delta CSV is empty"
    header = rows[0]
    assert header == ["operation", "id", "name", "email", "created_at", "updated_at", "is_deleted"]
    data_rows = rows[1:]
    ops = [r[0] for r in data_rows]
    assert "INSERT" in ops
    assert "UPDATE" in ops
    assert "DELETE" in ops