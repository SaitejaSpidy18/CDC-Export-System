# app/services/watermark.py
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.models import Watermark

def get_watermark(db: Session, consumer_id: str) -> Watermark | None:
    stmt = select(Watermark).where(Watermark.consumer_id == consumer_id)
    return db.execute(stmt).scalar_one_or_none()

def upsert_watermark(db: Session, consumer_id: str, last_exported_at: datetime) -> None:
    wm = get_watermark(db, consumer_id)
    now = datetime.now(timezone.utc)

    if wm is None:
        wm = Watermark(
            consumer_id=consumer_id,
            last_exported_at=last_exported_at,
            updated_at=now,
        )
        db.add(wm)
    else:
        wm.last_exported_at = last_exported_at
        wm.updated_at = now

    db.flush()
