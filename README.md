# CDC Export System

This project is a containerized FastAPI + PostgreSQL service that exports user data using **Change Data Capture (CDC)** principles. It supports:

- Full exports of all active users
- Incremental exports (only changed rows since last export)
- Delta exports (with operation type: INSERT / UPDATE / DELETE)

Exports run as **asynchronous background jobs** and write CSV files into a shared `output/` directory on the host.

---

## 1. What this service does

In many systems you need to sync data from a production database into other systems (data warehouse, search index, etc.). Exporting the entire table every time is too slow and expensive for large tables.

This service demonstrates a common pattern:

- Track changes using `updated_at` timestamps and soft deletes (`is_deleted`).
- Store a **watermark** per consumer that remembers the last exported `updated_at`.
- On each export, only read rows with `updated_at` greater than the watermark.
- Update the watermark only after a successful export.

This gives you efficient, resumable exports without needing log-based CDC tools.[web:49][web:46][web:55]

---

## 2. Tech stack

- **Language:** Python (FastAPI)
- **Database:** PostgreSQL 13
- **ORM:** SQLAlchemy
- **Containerization:** Docker + Docker Compose
- **Testing:** pytest + pytest-cov

Everything runs in containers; you don’t need a local Postgres or Python environment.[web:11]

---

## 3. Data model

### users

The main table we export from.

- `id BIGSERIAL PRIMARY KEY`
- `name VARCHAR(255) NOT NULL`
- `email VARCHAR(255) NOT NULL UNIQUE`
- `created_at TIMESTAMPTZ NOT NULL`
- `updated_at TIMESTAMPTZ NOT NULL`
- `is_deleted BOOLEAN NOT NULL DEFAULT FALSE`
- Index on `updated_at` for efficient CDC queries:
  ```sql
  CREATE INDEX idx_users_updated_at ON users(updated_at);

watermarks
Tracks progress per consumer (downstream system).

id SERIAL PRIMARY KEY

consumer_id VARCHAR(255) NOT NULL UNIQUE

last_exported_at TIMESTAMPTZ NOT NULL

updated_at TIMESTAMPTZ NOT NULL

For each consumer_id, last_exported_at stores the max updated_at from that consumer’s last successful export.

4. Prerequisites
Docker installed (Docker Desktop on Windows/macOS, or Docker Engine + Compose on Linux)

Git installed

5. Getting started
5.1 Clone the repository
git clone <your-repo-url>
cd CDC-Export-System

5.2 Run the stack
docker-compose up --build

What happens:

The db service (Postgres) starts and runs seeds/001_schema.sql.

The app service waits for the DB to be healthy.

The app runs app/seed_users.py once, which:

Creates at least 100,000 fake users.

Distributes created_at and updated_at over the last ~30 days.

Marks at least 1% as is_deleted = TRUE.

After seeding, the FastAPI server starts on port 8080.[web:44][web:41][web:48]

5.3 Health check
Once the containers are up, you can verify the service:
# Linux / macOS / Git Bash
curl http://localhost:8080/health

# PowerShell
Invoke-WebRequest http://localhost:8080/health

Expected response (200 OK):
{
  "status": "ok",
  "timestamp": "2026-02-26T04:30:00.000000+00:00"
}
6. Inspecting the database (optional)
Open a psql shell into the Postgres container:
docker exec -it cdc-export-system-db-1 psql -U user -d mydatabase

Some checks you can run:
-- Total users
SELECT COUNT(*) FROM users;

-- Non-deleted users
SELECT COUNT(*) FROM users WHERE is_deleted = FALSE;

-- Soft-deleted users (should be at least 1% of total)
SELECT COUNT(*) FROM users WHERE is_deleted = TRUE;

-- Range of updated_at timestamps
SELECT MIN(updated_at), MAX(updated_at) FROM users;

This confirms seeding worked and timestamps are spread over multiple days.

Type \q to exit psql.

7. Environment variables
All required environment variables are documented in .env.example.

Typical values:
DATABASE_URL=postgresql://user:password@db:5432/mydatabase
PORT=8080
LOG_LEVEL=info

DATABASE_URL is used by the app to connect to the Postgres db service.

PORT is the port that Uvicorn listens on inside the container.

You normally don’t need a .env file when using docker-compose.yml, since it already sets these for the app service.

8. API endpoints
Base URL: http://localhost:8080

8.1 Health
GET /health

Response:

200 OK

Body:
{
  "status": "ok",
  "timestamp": "<ISO 8601 timestamp>"
}


8.2 Full export
Triggers a full export of all non-deleted users for a given consumer.

Endpoint:

POST /exports/full

Headers:

X-Consumer-ID: <consumer-id>

Behavior:

Starts a background job.

Exports all rows where is_deleted = FALSE.

Writes a CSV file into the output/ directory, with columns:

id,name,email,created_at,updated_at,is_deleted

Updates the watermark for that consumer-id to the max updated_at of the exported rows.

Response:

202 Accepted

Body example:
{
  "jobId": "a4a0d4fa-5d63-4c00-a07e-4a5aefddc23e",
  "status": "started",
  "exportType": "full",
  "outputFilename": "full_consumer-1_20260226T043000Z.csv"
}

8.3 Incremental export
Exports only rows changed since the last export for this consumer.

Endpoint:

POST /exports/incremental

Headers:

X-Consumer-ID: <consumer-id>

Behavior:

Looks up the consumer’s watermark in watermarks.last_exported_at.

Exports users where:

updated_at > last_exported_at

is_deleted = FALSE

Writes the same CSV format as full export.

Updates the watermark to the max updated_at of this export batch.[web:49][web:118]

Response:

202 Accepted

Body similar to:
{
  "jobId": "<uuid>",
  "status": "started",
  "exportType": "incremental",
  "outputFilename": "incremental_consumer-1_20260226T043500Z.csv"
}

8.4 Delta export
Exports changed rows since the last export, plus an operation column that describes the change.

Endpoint:

POST /exports/delta

Headers:

X-Consumer-ID: <consumer-id>

Behavior:

Looks up the consumer’s watermark.

Exports users where updated_at > last_exported_at (both active and soft-deleted).

Adds an operation column:

DELETE if is_deleted = TRUE

INSERT if created_at == updated_at

UPDATE otherwise

CSV columns:

operation,id,name,email,created_at,updated_at,is_deleted

Updates the watermark to the max updated_at of the exported rows.[web:49][web:118]

Response:

202 Accepted

Body similar to:
{
  "jobId": "<uuid>",
  "status": "started",
  "exportType": "delta",
  "outputFilename": "delta_consumer-1_20260226T043800Z.csv"
}

8.5 Get watermark
Returns the current watermark for a consumer.

Endpoint:

GET /exports/watermark

Headers:

X-Consumer-ID: <consumer-id>

Behavior:

If a watermark exists:

Returns 200 with the consumer ID and last exported timestamp.

If no watermark exists (consumer never exported):

Returns 404 with a descriptive message.

Responses:

200 OK
{
  "consumerId": "consumer-1",
  "lastExportedAt": "2026-02-26T04:30:00.000000+00:00"
}

404 Not Found

{
  "detail": "No watermark for this consumer"
}

9. Watermarking logic (how CDC works here)
This service uses timestamp-based CDC with per-consumer watermarks
For each consumer, watermarks.last_exported_at stores the last exported high-water mark.

Full export:

Reads all non-deleted rows.

Sets watermark to the max updated_at.

Incremental / delta export:

Reads rows where updated_at > last_exported_at.

After writing the CSV, updates the watermark to the new max updated_at.

Export + watermark update runs inside a transaction at the DB layer:

If CSV writing or any part fails, the job logs an error and the transaction is rolled back.

Watermark is not advanced on failure, so you never “skip” data.

This makes exports restartable and safe, at the cost of not capturing every intermediate update between exports (only the latest state of each row is exported).

10. Logs
The export job runner emits structured logs for each job:[web:104][web:111]

When a job starts:

event = "export_started"

jobId, consumerId, exportType

When a job completes:

event = "export_completed"

jobId, rowsExported, durationSeconds

When a job fails:

event = "export_failed"

jobId, error

You can view logs with:
docker logs cdc-export-system-app-1

11. Testing
Tests are written with pytest and FastAPI’s TestClient:[web:94][web:95][web:120]

tests/test_health.py – checks the /health endpoint.

tests/test_exports_full.py – verifies full export CSV content and watermark update.

tests/test_exports_incremental.py – verifies only updated rows are exported.

tests/test_exports_delta.py – verifies operation is correctly set to INSERT / UPDATE / DELETE.

tests/test_watermark_logic.py – checks watermark insert / update logic.

Run tests with coverage inside the app container:
docker-compose run --rm app pytest --cov=app --cov-report=term-missing

12. Project structure
For reference, the repository is organized as:
.
├── app
│   ├── __init__.py
│   ├── main.py              # FastAPI app & routes
│   ├── database.py          # SQLAlchemy engine & session
│   ├── models.py            # User & Watermark ORM models
│   ├── schemas.py           # Pydantic response models
│   ├── seed_users.py        # Seeder for 100k+ users
│   ├── services
│   │   ├── __init__.py
│   │   ├── exports.py       # Full/incremental/delta export logic
│   │   ├── jobs.py          # Background job runner + logging
│   │   └── watermark.py     # Watermark CRUD helpers
├── seeds
│   └── 001_schema.sql       # DB schema and index
├── tests
│   ├── test_health.py
│   ├── test_exports_full.py
│   ├── test_exports_incremental.py
│   ├── test_exports_delta.py
│   └── test_watermark_logic.py
├── output/                  # Generated export files (gitignored)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md

13. Extending this project
This implementation keeps everything in a single service for simplicity. In a real production environment, you might:

Move export jobs into a separate worker service.

Use a message broker (RabbitMQ, Kafka, SQS, etc.) to queue export jobs.

Send CSVs directly to cloud storage (S3, GCS, Azure Blob) instead of a local volume.

The patterns in this project (watermarking, async jobs, structured logs) provide a solid base for that kind of evolution.
-----------------------------conclusion--------------------------------------------