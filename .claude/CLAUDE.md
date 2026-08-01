# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Cross-repo context — including which host (`wikidata-kg` vs `gpu-dslab-apa`) this repo is developed on — lives in the meta-repo at [`/data/projects/ba/CLAUDE.md`](../../CLAUDE.md).

## Project

`ba-observability-client` (package `observability_client`) — a reusable Python 3.12+ client that bundles five telemetry concerns behind one class:

1. Structured JSON logs to stdout (`log_event`, `log_sparql`)
2. Asynchronous Loki push delivery (background worker thread, bounded queue)
3. OpenTelemetry HTTP tracing with W3C propagation (FastAPI inbound + httpx outbound)
4. OpenTelemetry metrics — operational counters/histograms via a `MeterProvider` (OTLP → `/v1/metrics`)
5. MLflow run/param/metric/artifact tracking

It is intended to be installed (editable) into downstream projects — there is no application here, only a library plus runnable examples under [examples/](examples/).

## Common commands

This project uses `uv` for dependency management. There is no test suite, lint config, or CI defined.

```bash
# editable install (pip)
pip install -e .

# editable install (uv)
uv pip install -e .

# run examples (from repo root)
python examples/minimal_script.py
python examples/helper_script.py
python examples/python_observability.py --mode batch
python examples/multi_agent_demo.py

# FastAPI httpx-forwarding example
uvicorn examples.fastapi_httpx_forwarding:app --host 127.0.0.1 --port 8010
```

## Architecture

Everything lives in two files. Knowing how they fit together avoids accidental duplication:

- [observability_client/client.py](observability_client/client.py) — the entire implementation: `ObservabilityClient`, `BoundObservabilityClient`, `ObservabilitySettings`, `JsonFormatter`, and the private Loki worker. ~1500 lines, single module by design.
- [observability_client/__init__.py](observability_client/__init__.py) — public re-exports plus a thread-safe process-global default client (`get_default_client`, `reset_default_client`, and module-level `log_event` / `log_info` / `log_warning` / `log_error` shortcuts).

### Configuration model

Three construction paths, all converging on `ObservabilityClient.__init__`:

- `ObservabilityClient(...)` — explicit kwargs.
- `ObservabilityClient.from_env(...)` — reads env vars (see README table for the lookup chain and aliases like `OBSERVABILITY_SERVICE_NAME`/`SERVICE_NAME`, `ALLOY_LOG_PUSH_ENDPOINT`/`LOKI_PUSH_ENDPOINT`/`LOKI_URL`, etc.).
- `quick_script_mode(...)` / `full_mode(...)` — convenience presets.

Precedence is **always** `explicit kwargs > environment variables > hardcoded defaults`. Preserve this order when adding new settings. Settings land in the frozen `ObservabilitySettings` dataclass.

### Profiles

`profile` ∈ {`script`, `service`, `agent`} only sets defaults for `enable_mlflow` / `enable_tracing` (via `_PROFILE_DEFAULTS`). It is **not** a class hierarchy — same `ObservabilityClient` instance, different toggles. `script` disables both; `service`/`agent` enable both. Explicit `enable_mlflow=` / `enable_tracing=` always wins over the preset.

### Bound context (`bind`)

`client.bind(**ctx)` returns an immutable `BoundObservabilityClient` wrapper. The wrapper stores its own context dict and activates it via a `contextvars.ContextVar` whenever a delegated method runs (`_activate()` → `_bind_log_context()`). Calling `bind` again returns a *new* wrapper — no shared mutable state, safe to fan out across async tasks. Bound fields are deep-merged (see `_deep_merge_dicts`) into log payloads and into the `request_id` carried through HTTP propagation.

When extending the bound wrapper, prefer adding an explicit method (like `httpx_request`) over relying on the `__getattr__` fallback — the fallback only activates context for *callable* attributes.

### Loki push delivery

Default mode is **asynchronous**:

- `push_log` / `log_event(push=True)` enqueues a `_LokiQueueItem` on a bounded `Queue` (max 2000).
- A daemon thread (`_loki_worker_loop`) drains the queue and POSTs to the Loki push endpoint via `urllib`.
- Queue overflow is dropped silently except for a throttled warning every ~30s (`_maybe_warn_dropped_events`).
- `close()` calls `_stop_loki_worker()`, which signals stop and joins with a 1s timeout — the worker drains remaining items before exiting.

Strict mode is `raise_on_error=True`, which bypasses the queue and calls `_push_log_sync` directly. Use this in tests or critical paths only.

If you touch the worker lifecycle, remember: the worker keeps draining until both the stop flag is set *and* the queue is empty (see the loop guard at the top of `_loki_worker_loop`).

### Tracing

`_configure_tracer` is called once during `__init__` if tracing is enabled and OpenTelemetry is importable. It installs a process-global `TracerProvider` via `trace.set_tracer_provider(...)` — multiple `ObservabilityClient` instances in the same process will clobber each other's provider. This is intentional for the current use cases (one client per process) but worth knowing before adding multi-client tests.

`fastapi_request_span` extracts W3C headers via `propagate.extract`, resolves a `request_id` (explicit > header > generated UUID), starts a SERVER-kind span, and binds `request_id` into the log context for the span's lifetime. `httpx_request` mirrors this on the client side: it injects W3C headers via `propagate.inject` and forwards the bound `request_id` as an `x-request-id` header unless one is already set.

OTel imports are wrapped in `try/except ImportError` (the `_OTEL_AVAILABLE` flag). If tracing is requested but the deps are missing, the client logs `tracing_dependency_missing` and continues with structured logging only — don't raise in that path.

### MLflow

MLflow is also optional (`try/except ImportError`). When `enable_mlflow=True` is resolved but `mlflow` is `None`, `__init__` raises `RuntimeError` — fail loud, because the user explicitly asked for it. `start_run` raises `RuntimeError` if MLflow is disabled. `log_metrics` / `log_params` / `log_artifact` are no-ops when MLflow is disabled (silent), so they're safe to sprinkle through code paths that run in both `script` and `service` profiles.

### Metrics

OTel **metrics** answer a different question than MLflow: operational, continuous signals (request rates, latency histograms, Loki delivery health) for Grafana dashboards/alerts — not per-run experiment metrics. `enable_metrics` defaults to follow the resolved `enable_tracing` (script off, service/agent on) and can be overridden explicitly or via `OBSERVABILITY_ENABLE_METRICS`. No new dependency: `opentelemetry-exporter-otlp` already ships the HTTP metric exporter.

`_configure_meter` mirrors `_configure_tracer`: it installs a process-global `MeterProvider` (OTLP → `/v1/metrics`) **unless** a real SDK `MeterProvider` is already set, in which case it reuses it and logs `meter_provider_already_installed`. Only a provider this client installed is recorded on `self._meter_provider`, and only that one is `shutdown()`-flushed in `close()`. Missing OTel deps log `metrics_dependency_missing` and continue (no raise), like tracing.

`_init_metric_instruments` creates the client's own self-metrics (`observability.loki.events_enqueued` / `events_dropped` / `delivery_failures` counters, an `observability.loki.queue_depth` observable gauge reading `_loki_queue` live, and `observability.http.{server,client}.duration` histograms). All instrument refs are `None` when metrics are off, and the recording call sites are guarded so they become silent no-ops.

Public API: `counter` / `histogram` / `up_down_counter` (+ `metric_meter` accessor for power users) get-or-create instruments cached in `self._user_instruments` keyed by `(kind, name)` — OTel warns on duplicate creation, so never create the same named instrument twice. They are silent no-ops when metrics are disabled (like `log_metrics`), so they're safe across `script`/`service` profiles. Module-level `counter` / `histogram` shortcuts route through the default client. Keep metric **attributes** low-cardinality (same rule as Loki labels): method/route yes, full URLs/request_ids/UUIDs no.

## Conventions to preserve

- Public API surface is the `__all__` list in [observability_client/__init__.py](observability_client/__init__.py). Don't break it without a deliberate version bump (currently 0.3.1 in [pyproject.toml](pyproject.toml)).
- `TelemetryClient = ObservabilityClient` is a documented migration alias — keep it.
- Loki **labels** stay low-cardinality (`job`, `service`, `env`, `host`, `level`); high-cardinality identifiers like `request_id`, query hashes, UUIDs go in the JSON **fields**, not in stream labels. See `_build_loki_body`.
- All public methods are documented in Google-style docstrings. Match that style when adding methods.
- Single-module design is intentional. Resist splitting `client.py` into submodules unless the user asks.
