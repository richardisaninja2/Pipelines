# Incremental Orders Load Pipeline

An event-driven incremental ETL pipeline on Google Cloud. When a daily orders CSV lands in a Cloud Storage bucket, a Cloud Run service (FastAPI) cleans the data with pandas, stages it in PostgreSQL, validates row counts, and upserts it into `orders`, `customers` and `products` using `MERGE`.

It is built to be **safe to re-run**: loading the same file twice, or retrying after a failure, does not create duplicates.

## What this project demonstrates

- **Incremental loading** driven by a watermark (`max(updated_at)` already in the warehouse) instead of reloading everything each day.
- **Idempotent upserts** with PostgreSQL `MERGE`, so retries and re-uploads are harmless.
- **Data quality handling** for messy real-world input: mixed date formats, inconsistent casing, currency symbols, duplicate rows, missing values.
- **Validation before writing**: staged row counts must match the cleaned data before any merge runs, and invalid rows are flagged rather than silently dropped.
- **Serverless, event-driven design** on GCP: Cloud Storage, Eventarc and Cloud Run, with credentials in Secret Manager and a private (IAM-protected) service.
- **A small, well-defined API** with FastAPI, including health and readiness checks.

## Architecture

```mermaid
flowchart LR
    A[Daily CSV uploaded] --> B[(Cloud Storage bucket)]
    B -- object finalized --> C[Eventarc trigger]
    C -- POST /gcs-event --> D[Cloud Run: FastAPI]
    D -- download file --> B
    D --> E[Clean and validate with pandas]
    E --> F[(PostgreSQL staging tables)]
    F -- counts match --> G[MERGE into products, customers, orders]
    E -- rows with no amount --> H[Flagged orders CSV]
```

**Flow, step by step**

1. A CSV (for example `orders_20261108.csv`) is uploaded to the bucket.
2. Eventarc sends an HTTP POST with the bucket and file name to `/gcs-event`. It sends metadata only, not the file.
3. The service downloads the file from Cloud Storage.
4. The pipeline reads the watermark from the database and keeps only rows with `updated_at >= watermark`.
5. Data is cleaned (see [Data cleaning rules](#data-cleaning-rules)), split into three datasets, and each is written to a temporary staging table.
6. For each staging table, the pipeline compares the row count in the database with the row count in memory. If they match, it runs the `MERGE` and drops the staging table in a single transaction.
7. Load order is **products, customers, then orders**, so an order never lands before its product or customer.
8. The response reports what happened, including the watermark before and after the load.

## Repository layout

| File | Purpose |
|---|---|
| `incremental_load.py` | The pipeline itself: connection, watermark, cleaning, staging, merging. Usable as a library or from the command line. |
| `main.py` | FastAPI wrapper that exposes the pipeline as a web service for Cloud Run. |
| `requirements.txt` | Python dependencies for the buildpack. |
| `Procfile` | Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`. |
| `incremetal_load.ipynb` | The original notebook prototype the script was converted from. |

## API endpoints

| Method and path | Purpose |
|---|---|
| `GET /health` | Liveness check. Does not touch the database. |
| `GET /ready` | Readiness check. Runs `SELECT 1` against the database. Returns 503 if unreachable. |
| `POST /gcs-event` | Called by Eventarc. Downloads the file named in the event and runs the pipeline. Ignores non-CSV files. |
| `POST /load` | Manual upload of a CSV (multipart form field `file`). Runs the same pipeline. Returns 409 if a row-count check blocks a merge. |

The service needs to be a web server because Cloud Run only runs containers that listen for HTTP requests. A plain script would start, finish and exit, and the deployment would be marked as failed.

### Retry behavior on `/gcs-event`

Eventarc retries any non-2xx response, so the endpoint separates two kinds of failure:

- **Problems a retry cannot fix** (wrong file type, missing column, row-count mismatch) are logged and answered with `200`, so they are not retried forever.
- **Unexpected errors** (database unavailable, download failure) raise an error and return `500`, so Eventarc retries. Retrying is safe because the pipeline is idempotent.

## Input CSV format

Required columns:

`order_id, customer_id, customer_name, email, order_date, product, category, unit_price, total_amount, status, country, updated_at`

Notes:

- `updated_at` must use **one consistent format** across the whole file (for example `2026-11-08 10:54:40`). `order_date` may be in mixed formats.
- `total_amount` and `unit_price` may include a `$` prefix but should not contain thousands separators (no `$1,200.00`).
- `customer_id` must be a whole number with no blanks.
- Product names should be present on every row.

## Data cleaning rules

| Field | Rule |
|---|---|
| Orders: rows | Keep rows with `updated_at >= watermark`. |
| Orders: duplicates | Same `order_id` more than once: the last row in the file wins. |
| Orders: missing `total_amount` | Written to a flagged CSV and excluded from the load. |
| Orders: `order_date` | Parsed from mixed formats into a single date. |
| Orders: `status` / `country` | Lower-case status, upper-case country. |
| Orders: `total_amount` | `$` stripped, converted to a number. |
| Customers: name | Title case. Blank becomes `Missing`. |
| Customers: country / email | Country upper-case. Blank becomes `Missing`. |
| Customers: updates | A `Missing` value never overwrites an existing real value in the database. |
| Products: name / category | Title case, applied **before** duplicates are removed, so differently-capitalized names collapse into one row. |
| Products: `unit_price` | `$` stripped, converted to a number. |

## Design decisions

**Watermark from the database, not the file name.** The cutoff is the newest `updated_at` already in `orders`. A date in a file name is easy to get wrong (late files, re-uploads), while the table always reflects what has actually been loaded.

**`>=` instead of `>` on the watermark.** Rows tied with the last loaded timestamp are reprocessed, not skipped. This is safe because the `MERGE` is idempotent. The trade-off is that a re-run reloads the few rows at the boundary.

**Staging tables plus a count check.** Data is written to a temporary table first, and the merge only runs if the staged count equals the in-memory count. A mismatch is logged and the merge is skipped, so a partial or corrupted write never reaches the real tables.

**`MERGE` for upserts.** New rows are inserted and existing rows updated in one statement, so re-running a file converges to the same result.

**Merge and cleanup in one transaction.** Each `MERGE` and its `DROP TABLE` run together, so a failure mid-merge leaves the target table untouched.

**Invalid rows are flagged, not discarded.** Orders with no amount are written to `flagged_<file>.csv` so they can be inspected.

**Secrets and access.** The database URL is stored in Secret Manager and injected as an environment variable. The service is private and only callable by identities with the Cloud Run Invoker role.

**One request at a time.** The staging tables have fixed names, so the service should run with a concurrency of 1 to avoid two files colliding.

## Database requirements

- **PostgreSQL 15 or newer** (the `MERGE` statement was added in 15).
- Three existing tables with these columns:

| Table | Columns |
|---|---|
| `orders` | `order_id` (text), `customer_id` (integer), `order_date` (date), `product`, `status`, `country`, `total_amount` (double precision), `updated_at` (timestamp) |
| `customers` | `customer_id` (integer), `customer_name`, `email`, `country` |
| `products` | `product`, `category`, `unit_price` |

Making `order_id`, `customer_id` and `product` primary keys is recommended so duplicate rows can never be created.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | `postgresql://USER:PASSWORD@HOST:5432/DBNAME`. For Cloud SQL: `postgresql://USER:PASSWORD@/DBNAME?host=/cloudsql/PROJECT:REGION:INSTANCE`. Special characters in the password must be URL-encoded (`@` becomes `%40`). |
| `OUTPUT_DIR` | No | Where flagged-orders files and the error log are written. Defaults to the system temp directory. |

## Run locally

Requires Python 3.10 or newer.

```bash
pip install -r requirements.txt
echo 'DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME' > .env

# run the pipeline directly on a file
python incremental_load.py orders_20261108.csv

# or run the web service and upload through the API
uvicorn main:app --reload
curl -X POST -F "file=@orders_20261108.csv" http://localhost:8000/load
```

## Deploy to Cloud Run

```bash
PROJECT_ID="your-project-id"
REGION="us-central1"

gcloud config set project $PROJECT_ID
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com secretmanager.googleapis.com eventarc.googleapis.com

# store the database URL as a secret
echo -n "postgresql://USER:PASSWORD@HOST:5432/DBNAME" | gcloud secrets create DATABASE_URL --data-file=-

# build from source (uses the Procfile) and deploy as a private service
gcloud run deploy incremental-load \
  --source . \
  --region $REGION \
  --concurrency 1 \
  --memory 1Gi \
  --timeout 900 \
  --no-allow-unauthenticated \
  --set-secrets DATABASE_URL=DATABASE_URL:latest
```

### Permissions

Let `PROJECT_NUMBER` be your project's number. The default compute service account is `PROJECT_NUMBER-compute@developer.gserviceaccount.com`.

| Principal | Role | Where | Why |
|---|---|---|---|
| Compute service account | Secret Manager Secret Accessor | The `DATABASE_URL` secret | Service reads the database URL |
| Compute service account | Storage Object Viewer | The bucket | Service downloads the CSV |
| Compute service account | Eventarc Event Receiver, Cloud Run Invoker | Project | Trigger can call the service |
| `service-PROJECT_NUMBER@gs-project-accounts.iam.gserviceaccount.com` | Pub/Sub Publisher | Project | Cloud Storage can publish events |

### Create the trigger

The bucket and the trigger must be in the same region.

```bash
gcloud eventarc triggers create incremental-load-trigger \
  --location=$REGION \
  --destination-run-service=incremental-load \
  --destination-run-region=$REGION \
  --destination-run-path=/gcs-event \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=YOUR_BUCKET" \
  --service-account=PROJECT_NUMBER-compute@developer.gserviceaccount.com
```

The path `/gcs-event` must match the endpoint in `main.py` exactly. Without it, Eventarc posts to `/`, which does not exist.

### Watching runs

```bash
gcloud run services logs read incremental-load --region $REGION --limit 100
```

A successful run logs `Processing gs://...`, the watermark, one `local=... staged=...` line per table, and finally `Loaded gs://...`.

## Example response

Illustrative shape of a successful load (values are examples):

```json
{
  "source": "gs://your-bucket/incoming/orders_20261108.csv",
  "file_name": "orders_20261108.csv",
  "file_date": "2026-11-08",
  "watermark": "2026-10-25 23:10:00",
  "new_watermark": "2026-11-08 10:54:40",
  "rows_in_file": 100,
  "new_valid_orders": 82,
  "flagged_orders": 7,
  "flagged_file": "/tmp/flagged_orders_20261108.csv",
  "orders":    { "merged": true, "local_count": 82, "staged_count": 82 },
  "products":  { "merged": true, "local_count": 12, "staged_count": 12 },
  "customers": { "merged": true, "local_count": 28, "staged_count": 28 },
  "success": true
}
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Permission denied on secret ... DATABASE_URL` | The service account cannot read the secret. Grant it Secret Manager Secret Accessor on the secret, then deploy a new revision. |
| `storage.objects.get` denied (403) when processing a file | The service account cannot read the bucket. Grant Storage Object Viewer on the bucket. |
| `Could not parse SQLAlchemy URL` | `DATABASE_URL` is empty or malformed: stray quotes or spaces, wrong prefix, or an unencoded special character in the password. Fix the secret value and deploy a new revision. |
| Trigger fires but nothing loads, 404 in logs | The trigger's service URL path is wrong. It must be `/gcs-event`. |
| Trigger never fires | The bucket and the trigger are in different regions, or the Cloud Storage service agent lacks Pub/Sub Publisher. |
| `syntax error at or near "MERGE"` or similar | The database is older than PostgreSQL 15. |
| `MERGE command cannot affect row a second time` | Two source rows share the same key (for example product names that differ only by case, if the cleaning step is changed). |
| Response status 409 on `/load` | A row-count check blocked a merge. See `error_log.txt` and the logs. |
| Changes to code or secrets have no effect | Running instances keep old values. Deploy a new revision. |

## Known limitations and possible improvements

- **No automated tests** yet. The transform functions (`build_orders`, `build_customers`, `build_products`) are pure and would be straightforward to unit test.
- **Merges are separate transactions** for products, customers and orders. A failure partway can leave an earlier table loaded. A re-run heals it, but running all three in one transaction would make the load all-or-nothing.
- **Rejected files are only visible in logs.** A row-count mismatch or missing column returns a success status to Eventarc so it is not retried, but there is no alert. A load-log table and failure alerts would close this gap.
- **Flagged rows are written to ephemeral disk** (`/tmp`), which is lost when the instance recycles. Writing them to a bucket would preserve them.
- **Customers are built from every row in the file** with the first occurrence winning, regardless of `updated_at`.
- **Product names in `orders`** are not normalized the same way as in `products`, so a join on product name can miss if the source casing varies.
- **Duplicate customer identities** (different IDs, same person) are not resolved. A canonical customer ID in a view would handle this without altering source IDs.
