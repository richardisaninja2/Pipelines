"""FastAPI wrapper for the incremental load pipeline (built for GCP Cloud Run).

Endpoints:
    GET  /health   liveness check (no DB access)
    GET  /ready    readiness check (verifies the DB connection)
    POST /load     multipart upload of the daily CSV -> runs the pipeline
"""
import logging

from fastapi import FastAPI, File, HTTPException, UploadFile
from sqlalchemy import text

import incremental_load as pipeline

logging.basicConfig(level=logging.INFO)  # Cloud Run ships stdout/stderr to Cloud Logging
logger = logging.getLogger("api")

app = FastAPI(title="Incremental Load Pipeline", version="1.0.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    try:
        with pipeline.get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.exception("Database readiness check failed")
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}")
    return {"status": "ready"}


# Plain `def` (not async): the pipeline is blocking (pandas + SQL), so FastAPI
# runs it in a threadpool instead of freezing the event loop.
@app.post("/load")
def load(file: UploadFile = File(..., description="Daily orders CSV")):
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    try:
        result = pipeline.run_pipeline(file.file)
    except KeyError as exc:
        # a required column is missing from the CSV
        raise HTTPException(status_code=422, detail=f"CSV is missing expected column: {exc}")
    except Exception as exc:
        logger.exception("Pipeline failed")
        raise HTTPException(status_code=500, detail=f"pipeline failed: {exc}")

    # 200 when merged; 409 when the row-count check blocked a merge (needs inspection)
    if not result["success"]:
        raise HTTPException(status_code=409, detail=result)
    return result
