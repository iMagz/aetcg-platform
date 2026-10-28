"""
PR 1 - Finding 1 (+5, 14): the job_id returned by POST /api/v1/triage must
work with GET /api/v1/state/{job_id} and PUT /api/v1/approve/{job_id}, for
BOTH low-risk (COMPLETED) and high-risk (PAUSED_FOR_REVIEW) tickets.
"""
import types
import uuid

import pytest
from fastapi.testclient import TestClient

import main
import worker
import pipeline.orchestrator as orch
from config.settings import settings


class InlineQueue:
    """Stands in for rq.Queue. Runs the job immediately and, like rq,
    uses a caller-supplied job_id if given, otherwise invents its own."""

    count = 0

    def enqueue(self, func, *args, job_id=None, **kwargs):
        job = types.SimpleNamespace(id=job_id or uuid.uuid4().hex)
        func(*args, **kwargs)
        return job


class StubOrchestrator:
    """Mimics the shape of StateMachineOrchestrator.execute_workflow output."""

    def execute_workflow(self, raw_job_data):
        high = "HIGH" in raw_job_data["raw_text"]
        return {
            "ticket_id": raw_job_data["ticket_id"],
            "sanitized_text": raw_job_data["raw_text"],
            "risk_score": 9 if high else 2,
            "triage_verdict": "stub",
            "critic_feedback": "stub",
            "human_approval": "PENDING",
            "status": "PAUSED_FOR_REVIEW" if high else "COMPLETED",
        }


@pytest.fixture
def client(fake_redis, monkeypatch):
    monkeypatch.setattr(main, "redis_client", fake_redis)
    monkeypatch.setattr(worker, "redis_connection", fake_redis)
    monkeypatch.setattr(worker, "task_queue", InlineQueue())
    monkeypatch.setattr(orch, "StateMachineOrchestrator", StubOrchestrator)
    return TestClient(main.app)  # no `with`: lifespan heartbeat thread not started


def _submit(client, ticket_id, text):
    r = client.post("/api/v1/triage", json={
        "ticket_id": ticket_id,
        "reporter_email": "engineer@enterprise.com",
        "raw_text": text,
    })
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def test_low_risk_state_retrievable_by_returned_job_id(client):
    job_id = _submit(client, "T-101", "routine disk allocation notice LOW-1")
    r = client.get(f"/api/v1/state/{job_id}")
    assert r.status_code == 200, f"GET by returned job_id gave {r.status_code}"
    assert r.json()["status"] == "COMPLETED"


def test_high_risk_state_retrievable_by_returned_job_id(client):
    job_id = _submit(client, "T-104", "HIGH severity credential exposure 1")
    r = client.get(f"/api/v1/state/{job_id}")
    assert r.status_code == 200, f"GET by returned job_id gave {r.status_code}"
    assert r.json()["status"] == "PAUSED_FOR_REVIEW"


def test_stored_state_records_ticket_to_job_mapping(client):
    job_id = _submit(client, "T-105", "routine printer queue note LOW-2")
    body = client.get(f"/api/v1/state/{job_id}").json()
    assert body["ticket_id"] == "T-105"
    assert body["job_id"] == job_id


def test_approve_using_returned_job_id(client):
    job_id = _submit(client, "T-106", "HIGH severity data exfiltration 2")
    r = client.put(f"/api/v1/approve/{job_id}",
                   params={"action": "APPROVE",
                           "supervisor_token": settings.SUPERVISOR_TOKEN})
    assert r.status_code == 200, r.text
    assert r.json()["final_disposition"] == "COMPLETED"
    assert client.get(f"/api/v1/state/{job_id}").json()["status"] == "COMPLETED"


# --- neighbours: behaviour that must NOT change -------------------------

def test_wrong_supervisor_token_still_403(client):
    job_id = _submit(client, "T-107", "HIGH severity ransomware note 3")
    r = client.put(f"/api/v1/approve/{job_id}",
                   params={"action": "APPROVE", "supervisor_token": "nope"})
    assert r.status_code == 403


def test_unknown_job_id_still_404(client):
    assert client.get("/api/v1/state/does-not-exist").status_code == 404


def test_each_submission_gets_distinct_job_id(client):
    a = _submit(client, "T-108", "routine note A LOW-3")
    b = _submit(client, "T-109", "routine note B LOW-4")
    assert a != b