# app/services/exports.py

import csv
from datetime import datetime
from pathlib import Path
from typing import Literal

from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from app.models import User
from app.services.watermark import get_watermark, upsert_watermark

# Directory where CSV files will be written (mapped to ./output on host)
EXPORT_DIR = Path("output")

ExportType = Literal["full", "incremental", "delta"]


def _write_users_to_csv(rows, filepath: Path, include_operation: bool = False) -> int:
    """
    Write a list of User rows to a CSV file.
    Returns number of rows written (excluding header).
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if include_operation:
            header = ["operation", "id", "name", "email", "created_at", "updated_at", "is_deleted"]
        else:
            header = ["id", "name", "email", "created_at", "updated_at", "is_deleted"]

        writer.writerow(header)

        count = 0
        for user in rows:
            if include_operation:
                if user.is_deleted:
                    op = "DELETE"
                elif user.created_at == user.updated_at:
                    op = "INSERT"
                else:
                    op = "UPDATE"

                writer.writerow([
                    op,
                    user.id,
                    user.name,
                    user.email,
                    user.created_at.isoformat(),
                    user.updated_at.isoformat(),
                    user.is_deleted,
                ])
            else:
                writer.writerow([
                    user.id,
                    user.name,
                    user.email,
                    user.created_at.isoformat(),
                    user.updated_at.isoformat(),
                    user.is_deleted,
                ])
            count += 1

    return count


def run_full_export(db: Session, consumer_id: str, output_filename: str) -> int:
    """
    Full export:
    - Export all users where is_deleted = FALSE.
    - Write to CSV.
    - Update watermark for consumer to max(updated_at) of exported rows.
    Returns number of exported rows.
    """
    filepath = EXPORT_DIR / output_filename

    stmt = (
        select(User)
        .where(User.is_deleted == False)  # noqa: E712
        .order_by(User.updated_at)
    )
    users = list(db.execute(stmt).scalars())

    if not users:
        return 0

    rows_exported = _write_users_to_csv(users, filepath, include_operation=False)

    max_updated_at = max(u.updated_at for u in users)
    upsert_watermark(db, consumer_id, max_updated_at)

    return rows_exported


def run_incremental_export(db: Session, consumer_id: str, output_filename: str) -> int:
    """
    Incremental export:
    - Requires an existing watermark for the consumer.
    - Export users where updated_at > last_exported_at AND is_deleted = FALSE.
    - Write to CSV.
    - Update watermark to max(updated_at) of exported rows.
    Returns number of exported rows.
    """
    filepath = EXPORT_DIR / output_filename

    wm = get_watermark(db, consumer_id)
    if wm is None:
        # Spec expects a full export to be run first to create watermark.
        # Here we choose to export nothing if no watermark exists.
        return 0

    stmt = (
        select(User)
        .where(
            and_(
                User.updated_at > wm.last_exported_at,
                User.is_deleted == False,  # noqa: E712
            )
        )
        .order_by(User.updated_at)
    )
    users = list(db.execute(stmt).scalars())

    if not users:
        return 0

    rows_exported = _write_users_to_csv(users, filepath, include_operation=False)

    max_updated_at = max(u.updated_at for u in users)
    upsert_watermark(db, consumer_id, max_updated_at)

    return rows_exported


def run_delta_export(db: Session, consumer_id: str, output_filename: str) -> int:
    """
    Delta export:
    - Requires an existing watermark.
    - Export users where updated_at > last_exported_at (including soft-deleted).
    - Write to CSV with extra first column 'operation':
        - 'DELETE' if is_deleted = TRUE
        - 'INSERT' if created_at == updated_at
        - 'UPDATE' otherwise
    - Update watermark to max(updated_at) of exported rows.
    Returns number of exported rows.
    """
    filepath = EXPORT_DIR / output_filename

    wm = get_watermark(db, consumer_id)
    if wm is None:
        return 0

    stmt = (
        select(User)
        .where(User.updated_at > wm.last_exported_at)
        .order_by(User.updated_at)
    )
    users = list(db.execute(stmt).scalars())

    if not users:
        return 0

    rows_exported = _write_users_to_csv(users, filepath, include_operation=True)

    max_updated_at = max(u.updated_at for u in users)
    upsert_watermark(db, consumer_id, max_updated_at)

    return rows_exported
