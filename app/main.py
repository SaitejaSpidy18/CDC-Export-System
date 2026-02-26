# app/main.py

from datetime import datetime, timezone
import uuid

from fastapi import FastAPI, BackgroundTasks, Header, HTTPException, Depends
from sqlalchemy.orm import Session

from app.schemas import HealthResponse, ExportJobResponse, WatermarkResponse
from app.database import get_db
from app.services.jobs import run_export_job
from app.services.watermark import get_watermark

app = FastAPI()


@app.get("/health", response_model=HealthResponse)
def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def _require_consumer_id(x_consumer_id: str | None) -> str:
    if not x_consumer_id:
        raise HTTPException(status_code=400, detail="X-Consumer-ID header is required")
    return x_consumer_id


def _make_output_filename(export_type: str, consumer_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_consumer = consumer_id.replace(" ", "_")
    return f"{export_type}_{safe_consumer}_{ts}.csv"


@app.post("/exports/full", response_model=ExportJobResponse, status_code=202)
def trigger_full_export(
    background_tasks: BackgroundTasks,
    x_consumer_id: str | None = Header(default=None, alias="X-Consumer-ID"),
):
    consumer_id = _require_consumer_id(x_consumer_id)
    job_id = str(uuid.uuid4())
    filename = _make_output_filename("full", consumer_id)

    background_tasks.add_task(run_export_job, job_id, consumer_id, "full", filename)

    return {
        "jobId": job_id,
        "status": "started",
        "exportType": "full",
        "outputFilename": filename,
    }


@app.post("/exports/incremental", response_model=ExportJobResponse, status_code=202)
def trigger_incremental_export(
    background_tasks: BackgroundTasks,
    x_consumer_id: str | None = Header(default=None, alias="X-Consumer-ID"),
):
    consumer_id = _require_consumer_id(x_consumer_id)
    job_id = str(uuid.uuid4())
    filename = _make_output_filename("incremental", consumer_id)

    background_tasks.add_task(run_export_job, job_id, consumer_id, "incremental", filename)

    return {
        "jobId": job_id,
        "status": "started",
        "exportType": "incremental",
        "outputFilename": filename,
    }


@app.post("/exports/delta", response_model=ExportJobResponse, status_code=202)
def trigger_delta_export(
    background_tasks: BackgroundTasks,
    x_consumer_id: str | None = Header(default=None, alias="X-Consumer-ID"),
):
    consumer_id = _require_consumer_id(x_consumer_id)
    job_id = str(uuid.uuid4())
    filename = _make_output_filename("delta", consumer_id)

    background_tasks.add_task(run_export_job, job_id, consumer_id, "delta", filename)

    return {
        "jobId": job_id,
        "status": "started",
        "exportType": "delta",
        "outputFilename": filename,
    }


@app.get("/exports/watermark", response_model=WatermarkResponse)
def get_consumer_watermark(
    x_consumer_id: str | None = Header(default=None, alias="X-Consumer-ID"),
    db: Session = Depends(get_db),
):
    consumer_id = _require_consumer_id(x_consumer_id)
    wm = get_watermark(db, consumer_id)
    if wm is None:
        raise HTTPException(status_code=404, detail="No watermark for this consumer")

    return {
        "consumerId": wm.consumer_id,
        "lastExportedAt": wm.last_exported_at.isoformat(),
    }
