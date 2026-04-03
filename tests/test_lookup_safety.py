from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pynetbox2 import CacheBackend, NetBoxExtendedClient


class DictCacheBackend(CacheBackend):
    """Simple dict-backed cache for unit tests."""

    def __init__(self):
        self._store: dict = {}

    def get(self, key: str):
        return self._store.get(key)

    def set(self, key: str, value, ttl_seconds=None):
        self._store[key] = value

    def delete(self, key: str):
        self._store.pop(key, None)

    def delete_prefix(self, key_prefix: str):
        for key in list(self._store):
            if key.startswith(key_prefix):
                del self._store[key]

    def clear(self):
        self._store.clear()

    def count(self) -> int:
        return len(self._store)

    def keys(self) -> list:
        return list(self._store.keys())

    def get_ttl(self, key: str):
        return None

    def close(self):
        pass


def _make_client() -> NetBoxExtendedClient:
    client = NetBoxExtendedClient.__new__(NetBoxExtendedClient)
    cfg_stub = MagicMock()
    cfg_stub.retry_attempts = 0
    cfg_stub.cache_backend = "none"
    cfg_stub.prewarm_sentinel_ttl_seconds = None
    client.config = cfg_stub

    client.adapter = MagicMock()
    client.cache = DictCacheBackend()
    client._cache_metrics_lock = threading.Lock()
    client._cache_metrics = {
        "get_hits": 0,
        "get_misses": 0,
        "get_bypass": 0,
        "list_hits": 0,
        "list_misses": 0,
        "list_bypass": 0,
    }
    client._cache_key_locks_guard = threading.Lock()
    client._cache_key_locks = {}
    return client


class TestExplicitLookupValidation:
    def test_raises_when_explicit_lookup_field_missing(self):
        client = _make_client()

        with pytest.raises(ValueError, match="missing/blank values"):
            client.upsert_with_outcome(
                "dcim.sites",
                {"slug": "site-a"},
                lookup_fields=["name"],
            )

        assert client.adapter.get.call_count == 0
        assert client.adapter.create.call_count == 0
        assert client.adapter.update.call_count == 0

    def test_raises_when_explicit_lookup_field_blank(self):
        client = _make_client()

        with pytest.raises(ValueError, match="missing/blank values"):
            client.upsert_with_outcome(
                "dcim.sites",
                {"name": "   ", "slug": "site-a"},
                lookup_fields=["name"],
            )

        assert client.adapter.get.call_count == 0
        assert client.adapter.create.call_count == 0
        assert client.adapter.update.call_count == 0

    def test_raises_when_lookup_is_partial(self):
        client = _make_client()

        with pytest.raises(ValueError, match="missing/blank values"):
            client.upsert_with_outcome(
                "dcim.module_bays",
                {"device": 99},
                lookup_fields=["device", "name"],
            )

        assert client.adapter.get.call_count == 0
        assert client.adapter.create.call_count == 0
        assert client.adapter.update.call_count == 0

    def test_raises_when_object_lookup_unwraps_to_none(self):
        client = _make_client()

        with pytest.raises(ValueError, match="missing/blank values"):
            client.upsert_with_outcome(
                "dcim.module_bays",
                {"device": SimpleNamespace(id=None), "name": "Bay 1"},
                lookup_fields=["device", "name"],
            )

        assert client.adapter.get.call_count == 0
        assert client.adapter.create.call_count == 0
        assert client.adapter.update.call_count == 0

    def test_missing_fk_resource_mapping_does_not_raise_keyerror(self):
        client = _make_client()
        client.adapter.get.return_value = None
        client.adapter.create.return_value = {"id": 1, "site": 7}

        result = client.upsert_with_outcome(
            "extras.tags",
            {"name": "tag-a", "weight": 7},
            lookup_fields=["weight"],
        )

        assert result.outcome == "created"
        assert client.adapter.get.call_count == 1
        assert client.adapter.create.call_count == 1


class TestImplicitLookupBehavior:
    def test_implicit_lookup_with_blank_name_keeps_legacy_path(self):
        client = _make_client()
        client.adapter.get.return_value = None
        created = {"id": 101, "name": "   ", "slug": "site-a"}
        client.adapter.create.return_value = created

        result = client.upsert_with_outcome(
            "dcim.sites",
            {"name": "   ", "slug": "site-a"},
        )

        assert result.object == created
        assert result.outcome == "created"
        assert client.adapter.get.call_count == 1
        assert client.adapter.create.call_count == 1
