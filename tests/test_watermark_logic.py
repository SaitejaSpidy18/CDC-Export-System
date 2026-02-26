from datetime import datetime, timezone
from sqlalchemy import text
from app.database import engine, SessionLocal
from app.services.watermark import get_watermark, upsert_watermark
def test_upsert_watermark_insert_and_update():
    consumer_id = "test-consumer-watermark"
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM watermarks WHERE consumer_id = :cid"), {"cid": consumer_id})
    db = SessionLocal()
    try:
        upsert_watermark(db, consumer_id, now)
        db.commit()
        wm = get_watermark(db, consumer_id)
        assert wm is not None
        assert wm.consumer_id == consumer_id
        assert wm.last_exported_at == now
        later = now.replace(microsecond=0)
        upsert_watermark(db, consumer_id, later)
        db.commit()
        wm2 = get_watermark(db, consumer_id)
        assert wm2.last_exported_at == later
    finally:
        db.close()