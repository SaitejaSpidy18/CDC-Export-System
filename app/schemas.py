# app/schemas.py

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    timestamp: str


class ExportJobResponse(BaseModel):
    jobId: str
    status: str
    exportType: str
    outputFilename: str


class WatermarkResponse(BaseModel):
    consumerId: str
    lastExportedAt: str
