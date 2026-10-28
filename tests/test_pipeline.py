import pytest
from pipeline.validation import TicketIngestionSchema, DataSanitizationPipeline
import os
def test_pydantic_corporate_domain_enforcement():
    valid_data = {
        "ticket_id": "TCK-101",
        "reporter_email": "engineer@enterprise.com",
        "raw_text": "System logs show standard disk allocations exceeding 85% thresholds."
    }
    schema = TicketIngestionSchema(**valid_data)
    assert schema.reporter_email == "engineer@enterprise.com"

    invalid_data = {
        "ticket_id": "TCK-102",
        "reporter_email": "adversary@gmail.com",
        "raw_text": "Attempted arbitrary cross-domain payload pipeline inject."
    }
    with pytest.raises(ValueError, match="Ingestion restricted to internal corporate system users."):
        TicketIngestionSchema(**invalid_data)

def test_regex_pii_scrubbing_pipeline(): #Test3
    pipeline = DataSanitizationPipeline(dlq_path="tests/mock_dlq.log")
    dirty_text = "Urgent: Customer identity verification details matched SSN: 000-12-3456 profile."

    clean_text = pipeline.scrub_pii(dirty_text)
    assert "000-12-3456" not in clean_text
    assert "[SSN_REDACTED]" in clean_text

    if os.path.exists("tests/mock_dlq.log"):
        os.remove("tests/mock_dlq.log")

def test_enhanced_pii_scrubbing():
    """Test enhanced PII scrubbing for additional patterns"""
    pipeline = DataSanitizationPipeline(dlq_path="tests/mock_dlq.log")

    # Test phone number scrubbing
    dirty_text = "Contact us at 555-123-4567 for immediate assistance."
    clean_text = pipeline.scrub_pii(dirty_text)
    assert "555-123-4567" not in clean_text
    assert "[PHONE_REDACTED]" in clean_text

    # Test IP address scrubbing
    dirty_text = "Malicious traffic detected from 192.168.1.100"
    clean_text = pipeline.scrub_pii(dirty_text)
    assert "192.168.1.100" not in clean_text
    assert "[IP_ADDRESS_REDACTED]" in clean_text

    # Test that corporate emails are NOT scrubbed (whitelist protection)
    dirty_text = "Please contact admin@enterprise.com for approval"
    clean_text = pipeline.scrub_pii(dirty_text)
    assert "admin@enterprise.com" in clean_text  # Should remain unchanged

    # Test that non-corporate emails ARE scrubbed
    dirty_text = "Please contact attacker@gmail.com for approval"
    clean_text = pipeline.scrub_pii(dirty_text)
    assert "attacker@gmail.com" not in clean_text
    assert "[EMAIL_REDACTED]" in clean_text

    if os.path.exists("tests/mock_dlq.log"):
        os.remove("tests/mock_dlq.log")