# Autonomous Enterprise Triage & Compliance Guard (AETCG)

AETCG is a FastAPI-based security operations platform for ingesting enterprise tickets, sanitizing sensitive data, triaging risk, and routing escalations through background workers and human approval gates.

License: MIT

This project is a working prototype that combines:

- a web API for intake and state tracking
- a Redis-backed RQ job queue for asynchronous execution
- a workflow orchestrator that performs sanitization, triage, and compliance checks
- Prometheus metrics and dashboard endpoints for operational visibility
- a supervisor approval flow for high-risk or policy-sensitive incidents

## Repository layout

- `main.py` — FastAPI application and HTTP endpoints
- `worker.py` — background worker process for queued jobs
- `pipeline/orchestrator.py` — orchestration workflow for ticket processing
- `pipeline/validation.py` — input validation and sanitization models
- `services/agents.py` — triage and compliance critique logic
- `app/metrics.py` — Prometheus metrics definitions and middleware
- `config/settings.py` — environment-driven application settings
- `docker-compose.yml` — local Redis + API + worker stack
- `requirements.txt` — Python dependencies
- `tests/` — automated tests for API and pipeline behavior
- `.env.example` — environment configuration template

## What the app does

The platform accepts incoming incident or compliance tickets through the API, validates them, strips obvious PII and unsafe markup, and then queues processing in Redis/RQ. The worker executes a pipeline that:

1. sanitizes the raw text
2. runs a triage agent to assign a risk score and verdict
3. runs a compliance critic to assess the outcome
4. pauses the ticket for manual review when the risk threshold is reached
5. stores state in Redis for later resume or approval handling

The design is intended to model an enterprise governance workflow, not a production-ready security product by itself. It is best used as a reference implementation and local demo platform.

## Architecture overview

### Ingestion layer
- `POST /api/v1/triage`
- validates ticket payloads using `TicketIngestionSchema`
- checks Redis for cached results
- sanitizes the ticket text before queueing

### Processing layer
- `worker.py` schedules RQ jobs with `Queue("aetcg_jobs", ...)`
- `pipeline/orchestrator.py` executes the workflow loop
- metrics are emitted on queue activity, processing time, risk score distribution, and compliance outcomes

### Review and governance layer
- `GET /api/v1/state/{job_id}` returns the stored workflow state
- `PUT /api/v1/approve/{job_id}` accepts an approval or rejection action from a supervisor token
- tickets with a risk score above the review threshold are paused for human intervention

### Observability layer
- `GET /health` returns health information for Redis and external API availability
- `GET /metrics` exposes Prometheus metrics when enabled
- `GET /dashboard` serves the HTML dashboard if assets are present

## Prerequisites

- Python 3.11+
- Docker and Docker Compose (recommended for local orchestration)
- Redis (provided automatically by Docker Compose)
- Git

## Quick start

### Option 1: Docker Compose

```bash
git clone https://github.com/adibuzz/aetcg-platform.git
cd aetcg-platform
cp .env.example .env
docker compose up --build
```

This starts:

- the FastAPI web service on port `8000`
- a background worker process
- a Redis broker service

Verify the app is running:

```bash
curl http://localhost:8000/health
```

### Option 2: Local Python development

```bash
git clone https://github.com/adibuzz/aetcg-platform.git
cd aetcg-platform
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
cp .env.example .env
```

Then start the API and worker separately:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

In another terminal:

```bash
python worker.py
```

## Configuration

The app reads settings from `.env` using `pydantic-settings` and falls back to defaults defined in `config/settings.py`.

Common values include:

- `APP_ENV` — `development`, `staging`, or `production`
- `API_PORT` — HTTP API port (default `8000`)
- `REDIS_HOST` — Redis host, usually `queue_broker` in Docker Compose
- `REDIS_PORT` — Redis port (default `6379`)
- `EXTERNAL_COMPLIANCE_API_URL` — compliance service endpoint
- `EXTERNAL_API_TIMEOUT_SEC` — request timeout for external validation calls
- `LLM_ENDPOINT_URL` — LLM endpoint used by triage/compliance agents
- `LLM_ACCESS_TOKEN` — access token for LLM calls
- `RATE_LIMIT_REQUESTS_PER_MINUTE` — per-IP rate limit
- `SUPERVISOR_TOKEN` — token required for approval endpoints
- `METRICS_ENABLED` — enables Prometheus metrics and dashboard endpoints

## Example API usage

Submit a triage request:

```bash
curl -X POST http://localhost:8000/api/v1/triage \
  -H 'Content-Type: application/json' \
  -d '{
    "ticket_id": "TCK-001",
    "reporter_email": "analyst@enterprise.com",
    "raw_text": "A user reported a suspicious login and possible privilege escalation on a finance application."
  }'
```

Response example:

```json
{
  "status": "queued",
  "job_id": "<job-id>",
  "message": "Ticket verification initiated.",
  "result": null
}
```

Duplicate requests with a cached result return HTTP `200` with the same fields:
`status` is `"cached"`, `job_id` is `null`, and `result` contains the cached workflow
state. Cache entries expire after 600 seconds; once expired, the request is queued
again and receives the same response shape as a new submission.

Check the state of a queued job:

```bash
curl http://localhost:8000/api/v1/state/<job-id>
```

Approve or reject a paused job:

```bash
curl -X PUT "http://localhost:8000/api/v1/approve/<job-id>?action=APPROVE&supervisor_token=<token>"
```

## Testing

Run the test suite with:

```bash
python -m pytest -q
```

The repository includes API tests, workflow tests, and validation tests under `tests/`.

## Troubleshooting

- If Redis is unavailable, ensure Docker Compose is running and the `queue_broker` service is healthy.
- If the API returns `503` on `/health`, check the Redis connectivity and external API state.
- If the worker does not pick up jobs, confirm `python worker.py` is running and the Redis host/port match your config.
- If requests are rate-limited unexpectedly, review `RATE_LIMIT_REQUESTS_PER_MINUTE` in `.env`.

## Contributing

Contributions are welcome. Suggested workflow:

1. fork the repository
2. create a feature branch
3. add or update tests for new behavior
4. run the relevant validation commands
5. open a pull request with a clear summary

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for the full text.
