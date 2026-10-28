"""Shared test doubles. No real Redis, LLM, or network is used."""
import pytest


class FakeRedis:
    """Minimal in-memory stand-in for redis.Redis (get/set/ex only)."""

    def __init__(self):
        self.store = {}

    def get(self, key):
        v = self.store.get(key)
        return v.encode() if isinstance(v, str) else v

    def set(self, key, value, ex=None, *args, **kwargs):
        self.store[key] = value
        return True

    def keys_with_prefix(self, prefix):
        return sorted(k for k in self.store if k.startswith(prefix))


@pytest.fixture
def fake_redis():
    return FakeRedis()