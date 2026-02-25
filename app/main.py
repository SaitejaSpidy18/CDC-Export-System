from fastapi import FastAPI
from datetime import datetime, timezone
app = FastAPI()
@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }