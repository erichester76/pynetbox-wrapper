EARLY DEVELOPMENT BUT HEAVILY USED BY ME

# pynetbox-wrapper

`pynetbox-wrapper` is an unofficial helper wrapper around `pynetbox` that was
written to extend NetBox client functionality for real-world sync and discovery
workloads.

This is **not** a NetBox-supported module, is **not** an official NetBox
project, and should be treated as a community-maintained helper library.

The project was extracted from
[`erichester76/hcl-netbox-discovery`](https://github.com/erichester76/hcl-netbox-discovery)
so the wrapper can evolve independently and be reused by other projects.

## Why This Exists

Upstream `pynetbox` is a solid low-level client, but many automation and sync
workloads need extra behavior around it:

- cache-aware reads
- upsert helpers for create-or-update flows
- diff-aware updates that skip no-op writes
- retry, backoff, and shared cooldown behavior
- rate limiting for high-concurrency collectors
- optional alternative write backends

`pynetbox-wrapper` keeps the familiar `pynetbox.api(...)` style while layering
those behaviors on top.

## What It Provides

The top-level import exposed by this repo is still:

```python
import pynetbox2 as pynetbox
```

The main public surface is:

- `pynetbox2.api(...)`
  - factory that returns a `NetBoxAPI` client with a `pynetbox`-like shape
- `NetBoxAPI`
  - compatibility facade for common dotted-resource access such as
    `nb.dcim.devices.get(...)`
- `NetBoxExtendedClient`
  - higher-level client with cache, retry, and upsert helpers
- `UpsertOutcome`
  - structured result object with `object` and `outcome`
- cache backends
  - `NullCacheBackend`
  - `RedisCacheBackend`
  - `SQLiteCacheBackend`
- transport helpers
  - `RateLimiter`
  - `BackendAdapter`
  - `PynetboxAdapter`
  - `DiodeAdapter`

## Installation

### Poetry

From a Git repository dependency:

```toml
[tool.poetry.dependencies]
pynetbox-wrapper = { git = "https://github.com/erichester76/pynetbox-wrapper.git", rev = "<commit-or-tag>" }
```

Even though the package name is `pynetbox-wrapper`, the import name remains
`pynetbox2`.

### pip

```bash
pip install "git+https://github.com/erichester76/pynetbox-wrapper.git@<commit-or-tag>"
```

## Quick Start

### 1. Create a client

```python
import pynetbox2 as pynetbox

nb = pynetbox.api(
    url="https://netbox.example.com",
    token="YOUR_TOKEN",
)
```

### 2. Use it like `pynetbox`

```python
device = nb.dcim.devices.get(name="edge01")
all_devices = nb.dcim.devices.filter(site="rdu1")
```

### 3. Use wrapper-specific helpers

```python
result = nb.upsert_with_outcome(
    "dcim.devices",
    {
        "name": "edge01",
        "site": 7,
        "device_role": 4,
        "device_type": 12,
        "status": "active",
    },
    lookup_fields=("name", "site"),
)

print(result.outcome)  # created | updated | noop
print(result.object)
```

## Core Features

### Upsert Helpers

Use `upsert()` when you want the object back and do not care whether it was
created or updated:

```python
device = nb.upsert(
    "dcim.devices",
    {"name": "edge01", "site": 7, "status": "active"},
    lookup_fields=("name", "site"),
)
```

Use `upsert_with_outcome()` when the caller needs structured reporting:

```python
outcome = nb.upsert_with_outcome(
    "ipam.vlans",
    {"name": "prod-100", "vid": 100, "site": 7},
    lookup_fields=("vid", "site"),
)

if outcome.outcome == "created":
    print("new VLAN created")
elif outcome.outcome == "updated":
    print("existing VLAN updated")
elif outcome.outcome == "noop":
    print("no change needed")
```

Behavior:

- normalizes common foreign-key payload shapes before write
- looks up existing objects using `lookup_fields`
- creates when no match exists
- updates only when a real diff exists
- skips no-op updates and reports `noop`

### Cache-Aware Reads

The wrapper supports three cache backends:

- `none`
- `redis`
- `sqlite`

Backend behavior and practical cache levels:

- `none`
  - disables persistence and always reads from NetBox
  - `use_cache=True` and `use_cache=False` both effectively bypass cache
- `redis`
  - shared cache across processes/hosts, good for concurrent collectors
  - supports key TTL inspection used by `cache_stats()`
- `sqlite`
  - local file-backed cache for single-host jobs and local development
  - isolated to the configured SQLite file path

Per-operation cache behavior:

- `get(..., use_cache=True)` and `list(..., use_cache=True)`
  - read-through cache (check cache, then fetch and backfill on miss)
- `get(..., use_cache=False)` and `list(..., use_cache=False)`
  - bypass lookup cache and refresh values from backend
- `prewarm(...)`
  - bulk-fills list and common get keys
  - writes a prewarm sentinel key so repeated prewarm calls can be skipped
  - controlled by `prewarm_sentinel_ttl_seconds`
- `create`, `update`, `delete`, and `upsert*`
  - invalidate resource cache keys and prewarm sentinel keys

Example:

```python
import pynetbox2 as pynetbox

nb = pynetbox.api(
    url="https://netbox.example.com",
    token="YOUR_TOKEN",
    cache_backend="redis",
    redis_url="redis://localhost:6379/0",
    cache_ttl_seconds=300,
    cache_key_prefix="nbx:",
)
```

Helpful methods:

- `prewarm(resources)`
  - primes list/get cache entries for selected resources
- `cache_stats()`
  - returns a summary of cached entries
- `cache_flush(resource=None)`
  - flushes one resource or the whole cache

Example:

```python
nb.prewarm(["dcim.devices", "ipam.vlans"])
print(nb.cache_stats())
```

Optional TurboBulk-backed prewarm/export:

```python
nb = pynetbox.api(
    url="https://netbox.example.com",
    token="YOUR_TOKEN",
    cache_backend="redis",
    turbobulk_export_for_prewarm=True,
)
```

When enabled, `prewarm()` will try TurboBulk export first and fall back to the
normal REST list path if TurboBulk is unavailable, unsupported for the
resource, or the client is branch-scoped. Install the optional extra for this
path:

```bash
pip install "pynetbox-wrapper[turbobulk]"
```

### Configuration Variable Index

These are the public `pynetbox2.api(...)` configuration variables, with defaults:

| Variable | Default | Description |
| --- | --- | --- |
| `url` | _(required)_ | NetBox base URL. |
| `token` | _(required)_ | NetBox API token. |
| `branch` | `None` | Optional branch scope for read/write operations. |
| `backend` | `"pynetbox"` | Transport backend (`"pynetbox"` or `"diode"`). |
| `cache_backend` | `"none"` | Cache backend (`"none"`, `"redis"`, `"sqlite"`). |
| `cache_ttl_seconds` | `300` | Default TTL for cached list/get records. |
| `cache_key_prefix` | `"nbx:"` | Prefix for cache keys. |
| `redis_url` | `"redis://localhost:6379/0"` | Redis connection URL (when `cache_backend="redis"`). |
| `sqlite_path` | `".nbx_cache.sqlite3"` | SQLite cache DB file (when `cache_backend="sqlite"`). |
| `rate_limit_per_second` | `0.0` | Request rate limit; `0` disables throttling. |
| `rate_limit_burst` | `1` | Token bucket burst size for rate limiting. |
| `retry_attempts` | `3` | Number of retries after initial request attempt. |
| `retry_initial_delay_seconds` | `0.3` | Initial retry delay. |
| `retry_backoff_factor` | `2.0` | Exponential backoff multiplier. |
| `retry_max_delay_seconds` | `15.0` | Maximum retry sleep per attempt. |
| `retry_jitter_seconds` | `0.0` | Extra random jitter added to backoff delay. |
| `retry_on_4xx` | `(408, 409, 425, 429)` | Retriable 4xx status codes. |
| `retry_5xx_cooldown_seconds` | `60.0` | Shared cooldown duration after retriable 5xx responses. |
| `prewarm_sentinel_ttl_seconds` | `None` | TTL for prewarm sentinel keys; `None` uses `cache_ttl_seconds`. |
| `diode_target` | `"grpcs://localhost:8080"` | Diode target endpoint. |
| `diode_client_id` | `""` | Diode client ID. |
| `diode_client_secret` | `""` | Diode client secret. |
| `diode_cert_file` | `None` | Optional client certificate path for Diode. |
| `diode_skip_tls_verify` | `False` | Skip TLS verification for Diode connections. |
| `diode_read_fallback` | `False` | Use `pynetbox` read adapter with Diode write backend. |
| `diode_entity_builder` | `None` | Optional custom builder mapping resource/payload to Diode entities. |
| `diode_batch_size` | `1` | Batch size for Diode ingestion (`1` disables batching). |
| `turbobulk_export_for_prewarm` | `False` | Attempt TurboBulk export path during prewarm before REST fallback. |
| `turbobulk_verify_ssl` | `True` | Enable TLS verification for TurboBulk export requests. |

### Retry, Backoff, and Cooldown

The wrapper includes request retry controls intended for noisy or
high-concurrency environments.

Available knobs include:

- `retry_attempts`
- `retry_initial_delay_seconds`
- `retry_backoff_factor`
- `retry_max_delay_seconds`
- `retry_jitter_seconds`
- `retry_on_4xx`
- `retry_5xx_cooldown_seconds`

Example:

```python
nb = pynetbox.api(
    url="https://netbox.example.com",
    token="YOUR_TOKEN",
    retry_attempts=5,
    retry_initial_delay_seconds=0.25,
    retry_backoff_factor=2.0,
    retry_max_delay_seconds=10.0,
    retry_5xx_cooldown_seconds=30.0,
)
```

### Rate Limiting

For multi-threaded or bursty collectors, the wrapper can throttle outgoing
requests:

```python
nb = pynetbox.api(
    url="https://netbox.example.com",
    token="YOUR_TOKEN",
    rate_limit_per_second=20,
    rate_limit_burst=10,
)
```

### Alternative Backend Support

The default backend is normal `pynetbox` HTTP transport:

```python
nb = pynetbox.api(url="https://netbox.example.com", token="YOUR_TOKEN")
```

The wrapper also contains a `DiodeAdapter` path for deployments that want an
alternative write backend. The same top-level client API is preserved while the
backend behavior changes underneath.

Important:

- Diode backend support is still in active development
- it should be treated as fragile and not yet a stable compatibility promise
- downstream users should expect rough edges, behavior changes, and incomplete
  documentation while this path matures
- the default `pynetbox` backend remains the stable and recommended path

## Integration Pattern

This wrapper works best when your application keeps policy in the app layer and
keeps transport mechanics here.

A good separation looks like:

- application decides which fields are required
- application decides whether a failed item should skip, retry, or stop
- `pynetbox-wrapper` handles lookup, caching, retry, diffing, and write calls

In practice:

```python
import pynetbox2 as pynetbox

nb = pynetbox.api(
    url="https://netbox.example.com",
    token="YOUR_TOKEN",
    cache_backend="sqlite",
    sqlite_path=".nbx_cache.sqlite3",
    retry_attempts=3,
    rate_limit_per_second=15,
)

for device_payload in device_payloads:
    result = nb.upsert_with_outcome(
        "dcim.devices",
        device_payload,
        lookup_fields=("name", "site"),
    )
    print(device_payload["name"], result.outcome)
```

## Current Scope

This repository currently exposes the extracted `pynetbox2.py` module as a
package-managed dependency. Over time it may be reorganized into a more formal
package layout, but the current priority is preserving a stable import surface
for downstream projects.

Current maturity by area:

- stable focus:
  - `pynetbox`-backed reads and writes
  - cache-aware lookups
  - upsert helpers and outcome reporting
  - retry, cooldown, and rate-limit behavior
- in-development / fragile:
  - Diode backend support

## Compatibility Notes

- package name: `pynetbox-wrapper`
- import name: `pynetbox2`
- this project is community-maintained and unofficial
- downstream projects should pin to a commit or release tag rather than a
  floating branch

## Development

The wrapper began life inside a larger application, so some behavior is aimed
at automation-heavy NetBox sync workloads rather than generic SDK ergonomics.
That is intentional.

If you build on it, prefer:

- adding tests alongside behavior changes
- preserving the `pynetbox2` import surface unless you are doing an explicit
  breaking release
- documenting any new cache, retry, or write semantics in this README
