"""FastAPI wrapper for the incremental load pipeline (built for GCP Cloud Run).

Endpoints:
    GET  /health   liveness check (no DB access)
    GET  /ready    readiness check (verifies the DB connection)
    POST /load     multipart upload of the daily CSV -> runs the pipeline
"""
import io
import logging

from fastapi import Body, FastAPI, File, HTTPException, UploadFile
from google.cloud import storage
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
        result = pipeline.run_pipeline(file.file, source_name=file.filename)
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


# Eventarc posts the Cloud Storage "object finalized" event here. The body is the
# object's metadata (bucket, name, ...), NOT the file, so we download it ourselves.
#
# Eventarc retries any non-2xx response, so:
#   - problems that a retry can't fix (wrong file type, bad columns, count mismatch)
#     are logged and answered with 200 so they are not retried forever
#   - unexpected errors (DB down, download failed) raise -> 500 -> Eventarc retries.
#     Retrying is safe because the watermark makes the load idempotent.
@app.post("/gcs-event")
def gcs_event(event: dict = Body(...)):
    bucket_name = event.get("bucket")
    object_name = event.get("name")

    if not bucket_name or not object_name:
        logger.warning("Ignoring request that is not a Cloud Storage event: %s", list(event))
        return {"status": "ignored", "reason": "not a storage event"}

    if not object_name.lower().endswith(".csv"):
        logger.info("Ignoring non-CSV object gs://%s/%s", bucket_name, object_name)
        return {"status": "ignored", "reason": "not a .csv file", "object": object_name}

    logger.info("Processing gs://%s/%s", bucket_name, object_name)
    data = storage.Client().bucket(bucket_name).blob(object_name).download_as_bytes()

    try:
        result = pipeline.run_pipeline(io.BytesIO(data), source_name=f"gs://{bucket_name}/{object_name}")
    except KeyError as exc:
        logger.error("gs://%s/%s is missing expected column: %s", bucket_name, object_name, exc)
        return {"status": "failed", "reason": f"missing column: {exc}", "object": object_name}

    if not result["success"]:
        logger.error("Merge blocked by row-count check for gs://%s/%s: %s", bucket_name, object_name, result)
        return {"status": "failed", "reason": "row count mismatch", "object": object_name, "result": result}

    logger.info("Loaded gs://%s/%s: %s", bucket_name, object_name, result)
    return {"status": "loaded", "object": object_name, "result": result}
