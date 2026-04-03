# pynetbox-wrapper

`pynetbox-wrapper` is an unofficial helper wrapper around `pynetbox` that was
written to extend NetBox client functionality for real-world sync and discovery
workloads.

This is **not** a NetBox-supported module, is **not** an official NetBox
project, and should be treated as a community-maintained helper library.

The current wrapper adds functionality such as:

- cached reads
- upsert helpers
- retry and throttling behavior
- higher-level convenience methods for sync workflows

At the moment, the primary module provided by this repo is:

- `pynetbox2.py`

This repository was extracted from the
`erichester76/hcl-netbox-discovery` project so the wrapper can evolve more
cleanly on its own and be reused by other projects.
