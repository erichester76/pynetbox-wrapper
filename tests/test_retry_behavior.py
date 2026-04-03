"""Tests for retry, cooldown, and retry ownership behavior."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pynetbox2 import BackendAdapter, PynetboxAdapter, RateLimiter


class _ConcreteAdapter(BackendAdapter):
    """Minimal concrete subclass for testing BackendAdapter._call()."""

    def get(self, resource, **filters):
        pass

    def list(self, resource, **filters):
        return []

    def create(self, resource, data):
        pass

    def update(self, resource, object_id, data):
        pass

    def delete(self, resource, object_id):
        return False


def _make_adapter(cooldown=1.0, retry_attempts=1, jitter=0.0) -> _ConcreteAdapter:
    limiter = RateLimiter()
    return _ConcreteAdapter(
        rate_limiter=limiter,
        retry_attempts=retry_attempts,
        retry_initial_delay_seconds=0.0,
        retry_backoff_factor=1.0,
        retry_max_delay_seconds=0.0,
        retry_jitter_seconds=jitter,
        retry_5xx_cooldown_seconds=cooldown,
    )


def _exc_with_status(code: int) -> Exception:
    exc = Exception(f"HTTP {code}")
    exc.status_code = code  # type: ignore[attr-defined]
    return exc


class TestTriggerCooldown:
    def test_cooldown_blocks_acquire(self):
        limiter = RateLimiter()
        limiter.trigger_cooldown(0.1)
        t0 = time.perf_counter()
        limiter.acquire()
        elapsed = time.perf_counter() - t0
        assert elapsed >= 0.09

    def test_trigger_only_extends_never_shortens(self):
        limiter = RateLimiter()
        long_until = time.perf_counter() + 10.0
        with limiter.lock:
            limiter._cooldown_until = long_until
        limiter.trigger_cooldown(0.001)
        with limiter.lock:
            assert limiter._cooldown_until == long_until

    def test_zero_cooldown_is_noop(self):
        limiter = RateLimiter()
        with limiter.lock:
            before = limiter._cooldown_until
        limiter.trigger_cooldown(0)
        with limiter.lock:
            assert limiter._cooldown_until == before

    def test_cooldown_expired_does_not_block(self):
        limiter = RateLimiter()
        with limiter.lock:
            limiter._cooldown_until = time.perf_counter() - 1.0
        t0 = time.perf_counter()
        limiter.acquire()
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.1


class TestCooldownCrossThread:
    def test_all_threads_wait_for_cooldown(self):
        limiter = RateLimiter()
        limiter.trigger_cooldown(0.15)

        wake_times: list[float] = []
        lock = threading.Lock()

        def worker():
            limiter.acquire()
            with lock:
                wake_times.append(time.perf_counter())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        t0 = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3.0)

        assert len(wake_times) == 4
        for wake_time in wake_times:
            assert wake_time - t0 >= 0.12

    def test_cooldown_from_one_thread_blocks_others(self):
        limiter = RateLimiter()

        wake_times: list[float] = []
        lock = threading.Lock()

        def slow_worker():
            limiter.trigger_cooldown(0.15)

        def fast_worker():
            limiter.acquire()
            with lock:
                wake_times.append(time.perf_counter())

        t0 = time.perf_counter()
        slow_thread = threading.Thread(target=slow_worker)
        slow_thread.start()
        slow_thread.join()

        threads = [threading.Thread(target=fast_worker) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3.0)

        assert len(wake_times) == 3
        for wake_time in wake_times:
            assert wake_time - t0 >= 0.12


class TestBackendAdapterCooldown:
    def test_503_triggers_global_cooldown(self):
        adapter = _make_adapter(cooldown=5.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(503)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_called_with(5.0)

    def test_504_triggers_global_cooldown(self):
        adapter = _make_adapter(cooldown=5.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(504)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_called_with(5.0)

    def test_cooldown_scales_with_attempt(self):
        adapter = _ConcreteAdapter(
            rate_limiter=MagicMock(spec=RateLimiter),
            retry_attempts=2,
            retry_initial_delay_seconds=0.0,
            retry_backoff_factor=2.0,
            retry_max_delay_seconds=0.0,
            retry_5xx_cooldown_seconds=10.0,
        )
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def always_fails():
            raise _exc_with_status(503)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(always_fails)

        calls = [call.args[0] for call in adapter.rate_limiter.trigger_cooldown.call_args_list]
        assert calls == pytest.approx([10.0, 20.0])

    def test_4xx_does_not_trigger_cooldown(self):
        adapter = _make_adapter(cooldown=5.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(404)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_not_called()

    def test_zero_cooldown_skips_trigger(self):
        adapter = _make_adapter(cooldown=0.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(503)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_not_called()

    def test_succeeds_after_cooldown_clears(self):
        limiter = RateLimiter()
        adapter = _ConcreteAdapter(
            rate_limiter=limiter,
            retry_attempts=1,
            retry_initial_delay_seconds=0.0,
            retry_backoff_factor=1.0,
            retry_max_delay_seconds=0.0,
            retry_5xx_cooldown_seconds=0.1,
        )

        calls = []

        def sometimes_fails():
            calls.append(1)
            if len(calls) == 1:
                raise _exc_with_status(503)
            return "ok"

        result = adapter._call(sometimes_fails)
        assert result == "ok"
        assert len(calls) == 2


class TestPynetboxListRetryOwnership:
    def test_list_page_fetch_uses_only_backend_retry_loop(self):
        with patch("pynetbox2.pynetbox.api"):
            adapter = PynetboxAdapter(
                url="http://nb.example.com",
                token="token",
                rate_limiter=RateLimiter(),
                retry_attempts=1,
                retry_initial_delay_seconds=0.0,
                retry_backoff_factor=1.0,
                retry_max_delay_seconds=0.0,
                retry_jitter_seconds=0.0,
                retry_5xx_cooldown_seconds=0.0,
            )

        endpoint = MagicMock()
        endpoint.filter.side_effect = lambda **kwargs: (
            SimpleNamespace(count=1001)
            if kwargs["limit"] == 0
            else (_ for _ in ()).throw(_exc_with_status(503))
        )

        with patch.object(adapter, "_endpoint", return_value=endpoint), patch("time.sleep") as mock_sleep:
            with pytest.raises(Exception):
                adapter.list("dcim.sites")

        assert endpoint.filter.call_count == 3
        assert mock_sleep.call_count == 1


class TestRetry5xxCooldownClientSurface:
    def test_passed_to_client(self):
        from pynetbox2 import NetBoxExtendedClient

        client = NetBoxExtendedClient(
            "http://nb.example.com",
            "token",
            retry_5xx_cooldown_seconds=77.0,
        )
        assert client.config.retry_5xx_cooldown_seconds == pytest.approx(77.0)
        assert client.adapter.retry_5xx_cooldown_seconds == pytest.approx(77.0)

    def test_api_factory_forwards_retry_5xx_cooldown(self):
        import pynetbox2 as pynetbox

        client = pynetbox.api(
            "http://nb.example.com",
            "token",
            retry_5xx_cooldown_seconds=33.0,
        )
        assert client.config.retry_5xx_cooldown_seconds == pytest.approx(33.0)
        assert client.adapter.retry_5xx_cooldown_seconds == pytest.approx(33.0)
