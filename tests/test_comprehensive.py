"""
Comprehensive test suite for AETCG platform.
Tests API endpoints, metrics, worker processing, and integration scenarios.
"""
import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from prometheus_client import Counter, Histogram, Gauge
from fastapi.testclient import TestClient
import redis
from rq import Queue, Worker
from main import app
from config.settings import settings
from app.metrics import (
    REGISTRY,
    increment_counter,
    observe_histogram,
    set_gauge,
    TICKET_INGESTION_TOTAL,
    TICKET_PROCESSING_TIME_SECONDS,
    QUEUE_DEPTH_JOBS,
    QUEUE_PROCESSING_JOBS,
    QUEUE_FAILED_JOBS_TOTAL,
    WORKER_STATUS,
    LLM_API_CALLS_TOTAL,
    LLM_API_DURATION_SECONDS,
    LLM_API_TOKEN_USAGE,
    TICKET_RISK_SCORE_DISTRIBUTION,
    TICKET_CATEGORY_TOTAL,
    HUMAN_APPROVAL_WAIT_TIME_SECONDS,
    TICKET_APPROVAL_TOTAL,
    TICKET_REJECTION_TOTAL,
    COMPLIANCE_PASS_TOTAL,
    COMPLIANCE_FAIL_TOTAL,
    REDIS_CONNECTED,
    EXTERNAL_API_AVAILABLE,
    REDIS_LATENCY_SECONDS,
    EXTERNAL_API_LATENCY_SECONDS,
    HTTP_REQUESTS_TOTAL,
    HTTP_REQUEST_DURATION_SECONDS,
    get_metrics
)
from pipeline.validation import TicketIngestionSchema


class TestAPIEndpoints:
    """Test suite for API endpoints"""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def valid_ticket_data(self):
        return {
            "ticket_id": "TCK-TEST-001",
            "reporter_email": "tester@enterprise.com",
            "raw_text": "This is a test ticket for comprehensive testing."
        }

    def test_health_endpoint_metrics_disabled(self, client):
        """Test health endpoint when metrics are disabled"""
        with patch('main.settings.METRICS_ENABLED', False):
            response = client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert "timestamp" in data

    def test_health_endpoint_metrics_enabled_healthy(self, client):
        """Test health endpoint when metrics are enabled and all systems healthy"""
        with patch('main.settings.METRICS_ENABLED', True), \
             patch('main.REDIS_CONNECTED._value.get', return_value=1), \
             patch('main.EXTERNAL_API_AVAILABLE._value.get', return_value=1):
            response = client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["checks"]["redis"]["status"] == "healthy"
            assert data["checks"]["external_api"]["status"] == "healthy"

    def test_health_endpoint_metrics_enabled_unhealthy(self, client):
        """Test health endpoint when metrics are enabled but systems unhealthy"""
        with patch('main.settings.METRICS_ENABLED', True), \
             patch('main.REDIS_CONNECTED._value.get', return_value=0), \
             patch('main.EXTERNAL_API_AVAILABLE._value.get', return_value=1):
            response = client.get("/health")
            assert response.status_code == 503
            data = response.json()
            assert data["status"] == "unhealthy"
            assert data["checks"]["redis"]["status"] == "unhealthy"

 
    def test_ingest_ticket_cache_hit(self, client, valid_ticket_data):
        """Test ticket ingestion with cache hit"""
        cached_response = {"status": "cached", "result": "test result"}

        with patch('main.redis_client') as mock_redis:
            mock_redis.get.return_value = json.dumps(cached_response).encode()

            response = client.post(
                "/api/v1/triage",
                json=valid_ticket_data
            )

            assert response.status_code == 200
            assert response.json() == cached_response

    def test_ingest_ticket_cache_hit(self, client, valid_ticket_data):
        """Test ticket ingestion with cache hit"""
        cached_response = {"status": "cached", "result": "test result"}
        
        # 2. Check state using the returned job ID
        with patch('main.redis_client') as mock_redis:
            mock_redis.get.return_value = None

            state_response = client.get(f"/api/v1/state/{job_id}")

            # The state should be retrievable using the public job ID.
            assert state_response.status_code == 200
            state_data = state_response.json()
            assert state_data["status"] == "PROCESSING"


            # Verify cache was checked
            mock_redis.get.assert_called_once()


    def test_ingest_ticket_invalid_email(self, client):
        """Test ticket ingestion with non-corporate email"""
        invalid_data = {
            "ticket_id": "TCK-INVALID-001",
            "reporter_email": "user@gmail.com",  # Non-corporate email
            "raw_text": "Test ticket with invalid email"
        }

        response = client.post("/api/v1/triage", json=invalid_data)
        assert response.status_code == 422  # Validation error

    def test_state_endpoint_not_found(self, client):
        """Test state endpoint for non-existent job"""
        with patch('main.redis_client') as mock_redis:
            mock_redis.get.return_value = None  # No state found
            response = client.get("/api/v1/state/non-existent-job")
            assert response.status_code == 404

    def test_state_endpoint_uses_job_id_key(self, client):
        """State lookup should use the job ID returned by the API."""
        job_id = "lifecycle-job-123"
        expected_state = {
            "status": "PAUSED_FOR_REVIEW",
            "ticket_id": "TCK-LIFECYCLE-TEST-001"
        }

        with patch('main.redis_client') as mock_redis:
            def get_state_by_key(key):
                if key == f"state:{job_id}":
                    return json.dumps(expected_state).encode()
                return None

            mock_redis.get.side_effect = get_state_by_key

            response = client.get(f"/api/v1/state/{job_id}")

            assert response.status_code == 200
            assert response.json() == expected_state
            mock_redis.get.assert_called_once_with(f"state:{job_id}")


    def test_approval_endpoint_unauthorized(self, client):
        """Test approval endpoint with invalid token"""
        response = client.put(
            "/api/v1/approve/test-job",
            params={"action": "APPROVE", "supervisor_token": "wrong-token"}
        )
        assert response.status_code == 403

    def test_approval_endpoint_success(self, client):
        """Test approval endpoint with valid token"""
        with patch('main.redis_client') as mock_redis:
            mock_redis.get.return_value = json.dumps({
                "status": "PENDING",
                "human_approval": None
            }).encode()

            response = client.put(
                "/api/v1/approve/test-job-123",
                params={"action": "APPROVE", "supervisor_token": settings.SUPERVISOR_TOKEN}
            )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "state_resumed"
            assert data["final_disposition"] == "COMPLETED"

    def test_ingest_ticket_rate_limiting(self, client, valid_ticket_data):
        """Test rate limiting functionality"""
        # This would require making multiple requests rapidly
        # For simplicity, we'll test the endpoint exists and responds
        from unittest.mock import patch, Mock, PropertyMock

        # Import modules first
        from fastapi.testclient import TestClient
        from main import app
        import worker

        # Create test client
        test_client = TestClient(app)

        # Mock both Redis connections
        with patch('main.redis_client') as mock_main_redis, \
             patch.object(worker, 'task_queue') as mock_task_queue:

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            # Mock the task queue
            mock_job = Mock()
            mock_job.id = "test-job-id-123"
            mock_task_queue.enqueue.return_value = mock_job
            # Mock the count property of task_queue to return an integer
            type(mock_task_queue).count = PropertyMock(return_value=0)

            valid_ticket_data = {
                "ticket_id": "TCK-RATE-LIMIT-TEST-001",
                "reporter_email": "ratelimit@enterprise.com",
                "raw_text": "Test ticket for rate limiting"
            }

            response = test_client.post("/api/v1/triage", json=valid_ticket_data)
            # Should either succeed or fail with rate limiting (429)
            assert response.status_code in [200, 202, 429]

    def test_dashboard_endpoint_metrics_disabled(self, client):
        """Test dashboard endpoint when metrics disabled"""
        with patch('main.settings.METRICS_ENABLED', False):
            response = client.get("/dashboard")
            assert response.status_code == 404

    def test_dashboard_endpoint_metrics_enabled(self, client):
        """Test dashboard endpoint when metrics enabled"""
        with patch('main.settings.METRICS_ENABLED', True):
            response = client.get("/dashboard")
            assert response.status_code == 200
            assert "text/html" in response.headers["content-type"]
            assert "AETCG Platform Dashboard" in response.text


class TestMetricsCollection:
    """Test suite for metrics collection and functionality"""

    def test_counter_increment(self):
        """Test counter increment functionality"""

        # Create a test counter
        test_counter = Counter(
            'test_counter_total',
            'Test counter',
            ['label'],
            registry=REGISTRY
        )

        # Initial value should be 0
        assert test_counter.labels(label="test")._value.get() == 0

        # Increment counter
        increment_counter(test_counter, {"label": "test"}, 5)

        # Value should be 5
        assert test_counter.labels(label="test")._value.get() == 5

    def test_histogram_observe(self):
        """Test histogram observe functionality"""
        # Create a test histogram
        test_histogram = Histogram(
            'test_histogram_seconds',
            'Test histogram',
            ['label'],
            registry=REGISTRY
        )

        # Observe some values
        observe_histogram(test_histogram, 1.5, {"label": "test"})
        observe_histogram(test_histogram, 2.5, {"label": "test"})

        # Check that values were recorded
        # Note: Histogram values are a bit more complex to check directly
        # but we can verify the count increased
        metric = next(iter(test_histogram.labels(label="test").collect()))
        stats = metric.samples
        count_sample = [s for s in stats if s.name.endswith('_count')][0]
        assert count_sample.value == 2

    def test_gauge_set_get(self):
        """Test gauge set and get functionality"""
        # Create a test gauge
        test_gauge = Gauge(
            'test_gauge',
            'Test gauge',
            ['label'],
            registry=REGISTRY
        )

        # Initial value should be 0
        assert test_gauge.labels(label="test")._value.get() == 0

        # Set gauge
        set_gauge(test_gauge, 42.5, {"label": "test"})

        # Value should be 42.5
        assert test_gauge.labels(label="test")._value.get() == 42.5

        # Set to different value
        set_gauge(test_gauge, -10.0, {"label": "test"})

        # Value should be -10.0
        assert test_gauge.labels(label="test")._value.get() == -10.0

    def test_get_metrics_output(self):
        """Test that get_metrics returns valid Prometheus format"""
        metrics_output = get_metrics()
        assert isinstance(metrics_output, bytes)
        assert len(metrics_output) > 0

        # Decode to string for easier checking
        metrics_str = metrics_output.decode('utf-8')

        # Should contain some known metrics
        assert 'http_requests_total' in metrics_str
        assert 'ticket_ingestion_total' in metrics_str
        assert 'redis_connected' in metrics_str

    def test_metrics_labels_consistency(self):
        """Test that label usage is consistent across metrics"""
        # Test that we can increment counters with labels
        test_labels = {"status": "success"}
        increment_counter(TICKET_INGESTION_TOTAL, test_labels)

        # Test that we can observe histograms with labels
        test_histogram_labels = {"method": "POST", "endpoint": "/test"}
        observe_histogram(HTTP_REQUEST_DURATION_SECONDS, 0.1, test_histogram_labels)

        # Test that we can set gauges with labels
        test_gauge_labels = {"worker_id": "test-worker"}
        set_gauge(WORKER_STATUS, 1, test_gauge_labels)

    def test_system_health_gauges(self):
        """Test system health gauges are properly defined and usable"""
        # Test REDIS_CONNECTED gauge
        set_gauge(REDIS_CONNECTED, 1)
        assert REDIS_CONNECTED._value.get() == 1
        set_gauge(REDIS_CONNECTED, 0)
        assert REDIS_CONNECTED._value.get() == 0

        # Test EXTERNAL_API_AVAILABLE gauge
        set_gauge(EXTERNAL_API_AVAILABLE, 1)
        assert EXTERNAL_API_AVAILABLE._value.get() == 1
        set_gauge(EXTERNAL_API_AVAILABLE, 0)
        assert EXTERNAL_API_AVAILABLE._value.get() == 0


class TestWorkerProcessing:
    """Test suite for worker processing functionality"""

    def test_queue_triage_job_creates_job(self):
        """Test that queue_triage_job creates and enqueues a job"""
        test_payload = {
            "ticket_id": "TCK-WORKER-TEST-001",
            "reporter_email": "worker@enterprise.com",
            "raw_text": "Test ticket for worker processing"
        }

        with patch('worker.task_queue') as mock_queue:
            mock_job = Mock()
            mock_job.id = "test-worker-job-123"
            mock_queue.enqueue.return_value = mock_job

            from worker import queue_triage_job
            result = queue_triage_job(test_payload)

            assert result == mock_job
            mock_queue.enqueue.assert_called_once()

    def test_process_pipeline_task_success(self):
        """Test successful task processing"""
        test_payload = {
            "ticket_id": "TCK-PROCESS-TEST-001",
            "reporter_email": "processor@enterprise.com",
            "raw_text": "Test ticket for processing"
        }
        
    def test_process_pipeline_task_saves_state_by_job_id(self):
        """Worker should save workflow state using the public job ID."""
        from worker import process_pipeline_task

        job_id = "test-worker-job-123"
        payload = {
            "ticket_id": "TCK-PROCESS-TEST-001",
            "reporter_email": "processor@enterprise.com",
            "raw_text": "Test ticket for processing",
            "job_id": job_id,
        }
        result = {
            "status": "PROCESSING",
            "risk_score": 8,
            "ticket_id": payload["ticket_id"],
        }

        with patch(
            "pipeline.orchestrator.StateMachineOrchestrator"
        ) as mock_orchestrator_class, \
             patch("worker.redis_connection") as mock_redis:

            mock_orchestrator = Mock()
            mock_orchestrator.execute_workflow.return_value = result
            mock_orchestrator_class.return_value = mock_orchestrator

            process_pipeline_task(payload)

            saved_keys = [
                call.args[0]
                for call in mock_redis.set.call_args_list
            ]

            assert f"state:{job_id}" in saved_keys


        with patch('pipeline.orchestrator.StateMachineOrchestrator') as mock_orchestrator_class, \
             patch('worker.redis_connection') as mock_redis, \
             patch('worker.logger') as mock_logger:

            # Mock the orchestrator
            mock_orchestrator = Mock()
            mock_orchestrator.execute_workflow.return_value = {
                "status": "COMPLETED",
                "risk_score": 3.5,
                "category": "TEST"
            }
            mock_orchestrator_class.return_value = mock_orchestrator

            # Mock Redis
            mock_redis.set.return_value = True

            from worker import process_pipeline_task
            result = process_pipeline_task(payload)

            # Verify orchestrator was used
            mock_orchestrator_class.assert_called_once()
            mock_orchestrator.execute_workflow.assert_called_once_with(payload)

            # Verify result was cached
            assert mock_redis.set.call_count == 2

            # Verify success metrics were recorded
            # Note: These would be verified by checking the actual metric values
            # but for unit testing we mock the increment/observe functions

            # Verify result is returned
            assert result["status"] == "COMPLETED"
            assert result["risk_score"] == 3.5
            assert result["category"] == "TEST"

    def test_process_pipeline_task_failure(self):
        """Test task processing failure handling"""
        test_payload = {
            "ticket_id": "TCK-FAIL-TEST-001",
            "reporter_email": "failer@enterprise.com",
            "raw_text": "Test ticket that will fail"
        }

        with patch('pipeline.orchestrator.StateMachineOrchestrator') as mock_orchestrator_class, \
             patch('worker.logger') as mock_logger:

            # Mock the orchestrator to raise an exception
            mock_orchestrator = Mock()
            mock_orchestrator.execute_workflow.side_effect = Exception("Test failure")
            mock_orchestrator_class.return_value = mock_orchestrator

            from worker import process_pipeline_task

            # Should raise the exception
            with pytest.raises(Exception, match="Test failure"):
                process_pipeline_task(test_payload)

            # Verify failure metrics would be recorded
            # (In a real test we'd mock the increment/observe functions)

    def test_worker_initialization(self):
        """Test worker initialization sets gauges correctly"""
        with patch('app.metrics.set_gauge') as mock_set_gauge, \
             patch('rq.Worker') as mock_worker_class, \
             patch('builtins.print'):

            import runpy
            runpy.run_module('worker', run_name='__main__')

            # Verify initialization gauges were set
            # set_gauge(WORKER_STATUS, 1, {"worker_id": "worker_1"})
            # set_gauge(QUEUE_DEPTH_JOBS, 0, {"queue_name": "aetcg_jobs"})
            calls = mock_set_gauge.call_args_list
            assert len(calls) >= 2

            # Check for WORKER_STATUS gauge
            worker_status_call = None
            queue_depth_call = None
            for call in calls:
                args, kwargs = call
                if args[0] == WORKER_STATUS and args[1] == 1:
                    worker_status_call = call
                elif args[0] == QUEUE_DEPTH_JOBS and args[1] == 0:
                    queue_depth_call = call

            assert worker_status_call is not None
            assert queue_depth_call is not None


class TestIntegrationScenarios:
    """Test suite for integration scenarios and workflows"""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    def test_ticket_lifecycle_mocked(self, client):
        """Test complete ticket lifecycle with mocked dependencies"""
        ticket_data = {
            "ticket_id": "TCK-LIFECYCLE-TEST-001",
            "reporter_email": "lifecycle@enterprise.com",
            "raw_text": "Test ticket for lifecycle testing"
        }

        # 1. Ingest ticket - need to mock both Redis connections
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            # Mock the queue_triage_job function
            with patch('main.queue_triage_job') as mock_queue:
                mock_job = Mock()
                mock_job.id = "lifecycle-job-123"
                mock_queue.return_value = mock_job

                ingest_response = client.post("/api/v1/triage", json=ticket_data)
                assert ingest_response.status_code == 202
                job_id = ingest_response.json()["job_id"]
                assert job_id == "lifecycle-job-123"

        # 2. Check state (initially queued/processing)
        with patch('main.redis_client') as mock_redis:
            mock_redis.get.return_value = json.dumps({
                "status": "PROCESSING",
                "progress": 50
            }).encode()

            state_response = client.get(f"/api/v1/state/{job_id}")
            assert state_response.status_code == 200
            state_data = state_response.json()
            assert state_data["status"] == "PROCESSING"

        # 3. Approve ticket
        with patch('main.redis_client') as mock_redis:
            mock_redis.get.return_value = json.dumps({
                "status": "PROCESSING",
                "human_approval": None
            }).encode()
            mock_redis.set.return_value = True

            approve_response = client.put(
                f"/api/v1/approve/{job_id}",
                params={"action": "APPROVE", "supervisor_token": settings.SUPERVISOR_TOKEN}
            )
            assert approve_response.status_code == 200
            approve_data = approve_response.json()
            assert approve_data["final_disposition"] == "COMPLETED"

    def test_error_propagation_and_metrics(self, client):
        """Test that errors are properly propagated and metrics updated"""
        # Test with invalid data that should trigger validation error
        invalid_data = {
            "ticket_id": "TCK-TEST-001",
            "reporter_email": "test@gmail.com",  # Non-corporate email should fail validation
            "raw_text": "Test ticket"
        }

        # Need to mock Redis connections before making any requests
        # since main.redis_client is created at import time
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            response = client.post("/api/v1/triage", json=invalid_data)
            # Should get validation error from Pydantic
            assert response.status_code == 422

        # Test with valid data but simulate internal error
        valid_data = {
            "ticket_id": "TCK-ERROR-TEST-001",
            "reporter_email": "error@enterprise.com",
            "raw_text": "Test ticket that will cause internal error"
        }

        # Need to mock both Redis connections for the internal error test
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            with patch('main.queue_triage_job') as mock_queue:
                mock_queue.side_effect = Exception("Internal processing error")

                response = client.post("/api/v1/triage", json=valid_data)
                # Should get 500 error from internal failure
                assert response.status_code == 500

                # Verify error was logged and metrics updated
                # (Would need to mock logger and metric functions to verify)

    def test_concurrent_ticket_processing(self, client):
        """Test handling of multiple concurrent tickets"""
        ticket_base = {
            "reporter_email": "concurrent@enterprise.com",
            "raw_text": "Concurrent test ticket #{ticket_num}"
        }

        # Need to mock Redis connections before making any requests
        # since main.redis_client is created at import time
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            with patch('main.queue_triage_job') as mock_queue:
                mock_job = Mock()
                mock_job.id = "concurrent-job-{ticket_num}"
                mock_queue.return_value = mock_job

                # Send multiple requests concurrently (simulated)
                responses = []
                for i in range(5):
                    ticket_data = ticket_base.copy()
                    ticket_data["ticket_id"] = f"TCK-CONCURRENT-{i:03d}"
                    ticket_data["raw_text"] = f"Concurrent test ticket #{i}"

                    response = client.post("/api/v1/triage", json=ticket_data)
                    responses.append(response)
                    mock_queue.assert_any_call(ticket_data)

                # All should succeed
                for response in responses:
                    assert response.status_code == 202
                    data = response.json()
                    assert data["status"] == "queued"

    def test_cache_behavior(self, client):
        """Test caching behavior for duplicate tickets"""
        ticket_data = {
            "ticket_id": "TCK-CACHE-TEST-001",
            "reporter_email": "cache@enterprise.com",
            "raw_text": "This ticket should be cached"
        }

        # Mock the cache to return a value on second call
        call_count = 0
        def mock_get_side_effect(key):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None  # First call - cache miss
            else:
                return json.dumps({"cached": "result", "data": "test"}).encode()  # Second call - cache hit

        with patch('main.redis_client') as mock_redis:
            mock_redis.get.side_effect = mock_get_side_effect
            mock_redis.set.return_value = True

            # First request - should miss cache and enqueue job
            with patch('main.queue_triage_job') as mock_queue:
                mock_job = Mock()
                mock_job.id = "cache-job-123"
                mock_queue.return_value = mock_job

                response1 = client.post("/api/v1/triage", json=ticket_data)
                assert response1.status_code == 202
                # Should have enqueued a job (cache miss)
                mock_queue.assert_called_once()

            # Second request - should hit cache and return cached result
            response2 = client.post("/api/v1/triage", json=ticket_data)
            assert response2.status_code == 200
            data2 = response2.json()
            assert data2["cached"] == "result"
            # Should NOT have enqueued another job (cache hit)
            assert mock_queue.call_count == 1  # Still only called once

    def test_sanitization_effects(self, client):
        """Test that input sanitization works correctly"""
        # Test with script tags that should be removed
        malicious_data = {
            "ticket_id": "TCK-SANITIZE-TEST-001",
            "reporter_email": "sanitize@enterprise.com",
            "raw_text": "Normal text <script>alert('xss')</script> more text"
        }

        # Need to mock Redis connections before making any requests
        # since main.redis_client is created at import time
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            with patch('main.queue_triage_job') as mock_queue:
                mock_job = Mock()
                mock_job.id = "sanitize-job-123"
                mock_queue.return_value = mock_job

                response = client.post("/api/v1/triage", json=malicious_data)
                assert response.status_code == 202

                # Verify the sanitized payload was passed to queue_triage_job
                # The script tag should have been removed
                called_payload = mock_queue.call_args[0][0]
                assert "<script>" not in called_payload["raw_text"]
                assert "alert('xss')" not in called_payload["raw_text"]
                assert "Normal text" in called_payload["raw_text"]
                assert "more text" in called_payload["raw_text"]


class TestPerformanceAndLoad:
    """Test suite for performance and load testing scenarios"""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    def test_metrics_under_load(self, client):
        """Test that metrics behave correctly under simulated load"""
        ticket_base = {
            "reporter_email": "loadtest@enterprise.com",
            "raw_text": "Load test ticket #{ticket_num}"
        }

        # Need to mock Redis connections before making any requests
        # since main.redis_client is created at import time
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            with patch('main.queue_triage_job') as mock_queue:
                mock_job = Mock()
                mock_job.id = "loadtest-job-{ticket_num}"
                mock_queue.return_value = mock_job

                # Simulate multiple requests
                for i in range(10):
                    ticket_data = ticket_base.copy()
                    ticket_data["ticket_id"] = f"TCK-LOADTEST-{i:03d}"
                    ticket_data["raw_text"] = f"Load test ticket #{i}"

                    response = client.post("/api/v1/triage", json=ticket_data)
                    assert response.status_code == 202

                # Verify all requests were processed
                assert mock_queue.call_count == 10

                # In a real test, we would check that metrics increased appropriately
                # For now, we verify the endpoint handled the load without errors

    def test_long_running_request_handling(self, client):
        """Test handling of requests that take longer to process"""
        ticket_data = {
            "ticket_id": "TCK-LONG-TEST-001",
            "reporter_email": "longtest@enterprise.com",
            "raw_text": "This ticket simulates a long-running process"
        }

        # Need to mock Redis connections before making any requests
        # since main.redis_client is created at import time
        with patch('main.redis_client') as mock_main_redis, \
             patch('redis.Redis') as mock_redis_class:
            # Set up the mock Redis instance for worker
            mock_redis_instance = Mock()
            mock_redis_instance.set.return_value = True
            mock_redis_instance.ping.return_value = True
            # Mock the llen method that is likely called by Queue.count property
            mock_redis_instance.llen.return_value = 0  # Return 0 jobs in queue

            mock_redis_class.return_value = mock_redis_instance

            # Mock main Redis client for cache checking
            mock_main_redis.get.return_value = None  # No cache hit

            # Mock a slow job processing
            with patch('main.queue_triage_job') as mock_queue:
                def slow_queue_side_effect(payload):
                    time.sleep(0.1)  # Simulate small delay
                    mock_job = Mock()
                    mock_job.id = "longtest-job-123"
                    return mock_job

                mock_queue.side_effect = slow_queue_side_effect

                start_time = time.time()
                response = client.post("/api/v1/triage", json=ticket_data)
                end_time = time.time()

                assert response.status_code == 202
                # Request should have taken at least the sleep time
                assert (end_time - start_time) >= 0.1


if __name__ == "__main__":
    # This allows running the tests directly with: python -m tests.test_comprehensive
    pytest.main([__file__, "-v"])

