import hashlib


def ticket_cache_key(raw_text: str) -> str:
    digest = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    return f"cache:{digest}"
