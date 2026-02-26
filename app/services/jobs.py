# app/services/jobs.py
import logging
import time
import uuid
from typing import Literal

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.services.exports import (
    run_full_export,
    run_incremental_export,
    run_delta_export,
)

logger = logging.getLogger(__name__)

ExportType = Literal["full", "incremental", "delta"]

def run_export_job(job_id: str, consumer_id: str, export_type: ExportType, output_filename: str):
    start = time.time()
    rows_exported = 0

    logger.info({
        "event": "export_started",
        "jobId": job_id,
        "consumerId": consumer_id,
        "exportType": export_type,
    })

    db: Session = SessionLocal()
    try:
        if export_type == "full":
            rows_exported = run_full_export(db, consumer_id, output_filename)
        elif export_type == "incremental":
            rows_exported = run_incremental_export(db, consumer_id, output_filename)
        elif export_type == "delta":
            rows_exported = run_delta_export(db, consumer_id, output_filename)
        else:
            raise ValueError(f"Unknown export type: {export_type}")

        db.commit()

        duration = time.time() - start
        logger.info({
            "event": "export_completed",
            "jobId": job_id,
            "rowsExported": rows_exported,
            "durationSeconds": duration,
        })
    except Exception as e:
        db.rollback()
        logger.error({
            "event": "export_failed",
            "jobId": job_id,
            "error": str(e),
        })
        raise
    finally:
        db.close()
