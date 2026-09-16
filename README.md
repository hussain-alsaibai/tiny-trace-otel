# tiny-trace-otel

> A tiny, stdlib-only OpenTelemetry-compatible OTLP/HTTP tracing exporter for Python.

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#)
[![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-25%2F25%20passing-success.svg)](#testing)

The smallest way to ship OpenTelemetry-compatible spans from Python without pulling in the ~6MB `@opentelemetry/exporter-trace-otlp-http` package (which itself pulls in `@opentelemetry/core`, `@grpc/grpc-js`, `protobufjs`, and many transitive deps).

```python
from tiny_trace_otel import Tracer, BatchSpanProcessor, OTLPHttpExporter, set_processor

exporter = OTLPHttpExporter(
    endpoint="https://api.honeycomb.io/v1/traces",
    headers={"x-honeycomb-team": "YOUR_API_KEY"},
)

processor = BatchSpanProcessor(exporter, max_batch_size=64, export_interval_seconds=2.0)
set_processor(processor)

tracer = Tracer(
    name="myagent.tiny-trace-otel",
    resource_attrs={"service.name": "bounty-hunter", "service.version": "1.0.0"},
)

with tracer.start_span("scan-issues") as span:
    span.set_attribute("issues.scanned", 22)
    span.add_event("checkpoint", stage="initial-pass")
    results = find_targets()
    span.set_attribute("issues.found", len(results))
    for r in results[:3]:
        with tracer.start_span("triage", parent=span.context) as child:
            child.set_attribute("issue.id", r.id)
            analyze(r)
```

Ships traces to **Honeycomb, SigNoz, Grafana Cloud Tempo/Loki, Datadog (via otel-collector), New Relic, Jaeger, or any OTLP/HTTP consumer** — same JSON envelope as the official SDK.

---

## Why?

The official `@opentelemetry/exporter-trace-otlp-http` package pulls in ~6 MB of transitive deps just to POST a JSON envelope over HTTP. For an agent service that wants to ship spans to its own collector — or to a SaaS provider — that's overkill.

`tiny-trace-otel` is the smallest thing that ships:

- ✅ W3C Trace Context (`traceparent`, `tracestate`) headers
- ✅ OTLP/HTTP JSON wire format — drop-in compatible with OTel collectors
- ✅ Background-batch span processor
- ✅ Stdout exporter for local debugging
- ✅ `with span:` block syntax — auto-ends, auto-records exceptions
- ✅ Sync span API — pair with `asyncio.to_thread` for async code
- ✅ Zero dependencies — Python stdlib only
- ✅ MIT licensed — ~440 LOC, 25/25 tests passing

---

## Installation

```bash
# pip (when published)
pip install tiny-trace-otel

# Or just copy the single file
curl -O https://raw.githubusercontent.com/hussain-alsaibai/tiny-trace-otel/main/tiny_trace_otel.py
```

Requires **Python 3.8+**. No external packages.

---

## Quick start

```python
from tiny_trace_otel import (
    Tracer, BatchSpanProcessor, OTLPHttpExporter,
    StdoutExporter, set_processor, SpanKind,
)

# Stdout exporter for local debug
exporter = StdoutExporter()
proc = BatchSpanProcessor(exporter)
set_processor(proc)

# Tracer with resource attrs
tracer = Tracer(
    name="my-service",
    resource_attrs={"service.name": "demo", "service.version": "0.1.0"},
)

root = tracer.start_span("checkout", kind=SpanKind.SERVER)
root.set_attribute("http.method", "POST")

with tracer.start_span("charge-card", parent=root.context) as child:
    child.add_event("checkout.authorized")
    child.set_status("OK")

root.end()
proc.force_flush()
```

### Trace context propagation across services

```python
from tiny_trace_otel import parse_traceparent

# Receive incoming traceparent from upstream service
incoming = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
parent_ctx = parse_traceparent(incoming)

child = tracer.start_span("downstream-call", parent=parent_ctx)

# When sending to ANOTHER service, generate traceparent:
from tiny_trace_otel import format_traceparent
outgoing = format_traceparent(child.context.trace_id, child.context.span_id)
# outgoing = "00-0af7651916cd43dd8448eb211c80319c-<span_id>-01"
```

---

## Features

- **W3C Trace Context** — `traceparent` and `tracestate` parse/format helpers.
- **OTLP/HTTP JSON** — wire-compatible with official OTel collectors, Honeycomb, Tempo, SigNoz, Datadog, Jaeger.
- **`Tracer.start_span(name, kind, attributes, parent)`** — parent either explicit, tracer default, or fresh trace.
- **Span lifecycle** — `set_attribute`, `set_attributes`, `add_event`, `set_status`, `record_exception`, `end`.
- **`with span:`** — context-manager auto-ends and auto-records exceptions as events.
- **Span kinds** — `INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER`.
- **Status codes** — `UNSET`, `OK`, `ERROR`.
- **Resource attributes** — `process.service.name` etc. applied to every span.
- **Background batch processor** — `max_queue_size`, `max_batch_size`, `export_interval_seconds`, `force_flush()`, `shutdown()`.
- **Drop counters** — when the queue overflows, `dropped_count` tracks losses.
- **Stdout exporter** — for debugging; one JSON line per span.
- **OTLP/HTTP exporter** — JSON envelope, `urllib` POST, custom headers, timeout.
- **`global tracer`** — `get_tracer()`, `set_global_tracer()` for app-wide access.
- **Zero deps** — stdlib only.
- **MIT licensed**, 25/25 tests passing.

---

## API at a glance

### `Tracer(name, resource_attrs=None, parent_context=None)`

| Method | Returns | Description |
|---|---|---|
| `start_span(name, kind="INTERNAL", attributes=None, parent=None)` | `Span` | Begin a new span; parent wins over default. |
| `parent_context` (attr) | `SpanContext \| None` | Default parent if `start_span(parent=...)` not given. |

### `Span` (constructor takes `SpanContext` + name; usually produced by `Tracer`)

| Member | Type | Description |
|---|---|---|
| `context` | `SpanContext` | `trace_id`, `span_id`, `parent_span_id`, `trace_flags`, `trace_state`, `is_remote`. |
| `set_attribute(k, v)` | — | Coerces value via OTel attribute rules (bool, int, float, list, dict, str). |
| `set_attributes({...})` | — | Bulk. |
| `add_event(name, **attrs)` | — | Timestamped span event. |
| `set_status("OK" \| "ERROR" \| "UNSET", message=None)` | — | |
| `record_exception(exc)` | — | Adds `exception` event and sets ERROR status. |
| `end(status=None, message=None)` | — | Idempotent; enqueues to active processor. |
| `with s:` | — | Auto-ends; auto-records exception. |
| `to_otel_dict()` | `dict` | OTLP JSON wire-format shape. |

### `OTLPHttpExporter(endpoint, headers, timeout_seconds=5.0)`

| Method | Returns | Description |
|---|---|---|
| `export(spans)` | `(http_status, error_or_None)` | Post OTLP envelope; 200–299 = success. |

### `BatchSpanProcessor(exporter, max_queue_size=1024, max_batch_size=64, export_interval_seconds=2.0, export_timeout_seconds=30.0)`

| Method | Returns | Description |
|---|---|---|
| `on_end(span)` | — | Enqueue (drops if full). |
| `force_flush()` | — | Drain and export one batch synchronously. |
| `shutdown(timeout_seconds=None)` | — | Stop worker, drain remaining. |
| `dropped_count` | `int` | Spans dropped due to queue overflow. |

### Helpers

- `parse_traceparent(value)` → `SpanContext`
- `format_traceparent(trace_id, span_id, flags="01")` → `str`
- `set_processor(processor)` / `reset_processor(token)` — install/uninstall the active processor.
- `get_tracer(name="tiny-trace-otel")` — singleton tracer accessor.
- `TraceError` — exception for malformed context.
- `SpanKind`, `StatusCode` — string enums.

---

## Use cases

### 1. Local CLI tracing (stdout exporter)

```python
import sys
from tiny_trace_otel import Tracer, StdoutExporter, BatchSpanProcessor, set_processor, SpanKind

set_processor(BatchSpanProcessor(StdoutExporter()))
tracer = Tracer(name="cli-tool", resource_attrs={"service.name": "cli"})
# every span now dumps to stdout as JSON lines.
```

### 2. SaaS collector (Honeycomb)

```python
import os
exporter = OTLPHttpExporter(
    endpoint="https://api.honeycomb.io/v1/traces",
    headers={"x-honeycomb-team": os.environ["HONEYCOMB_API_KEY"], "x-honeycomb-dataset": "myagent"},
    timeout_seconds=10.0,
)
set_processor(BatchSpanProcessor(exporter, max_batch_size=128, export_interval_seconds=1.0))
```

### 3. Self-hosted OTel collector

```python
exporter = OTLPHttpExporter(
    endpoint="http://otel-collector:4318/v1/traces",
    headers={},
)
```

### 4. Cross-service trace propagation

```python
import urllib.request
from tiny_trace_otel import parse_traceparent, format_traceparent

# On receiving HTTP request
parent_ctx = parse_traceparent(request.headers["traceparent"])
span = tracer.start_span("handle-request", parent=parent_ctx)

# When calling downstream:
outgoing_traceparent = format_traceparent(span.context.trace_id, span.context.span_id)
req = urllib.request.Request(url, headers={"traceparent": outgoing_traceparent})
urllib.request.urlopen(req)
```

---

## Comparison vs alternatives

| Library | External deps | Stdlib only | OTLP/HTTP | gRPC OTLP | Tests | Lines |
|---|---|---|---|---|---|---|
| **tiny-trace-otel** | **0** | ✅ | ✅ | ❌ (intentional) | 25 | ~440 |
| `@opentelemetry/exporter-trace-otlp-http` | many | ❌ | ✅ | ✅ | n/a | 5000+ |
| `@opentelemetry/api` only (manual export) | 1 | ❌ | ❌ | ❌ | n/a | n/a |
| Custom JSON logger | 0 | ✅ | ❌ | ❌ | n/a | you build it |

**TL;DR** — if you ship Python, want OTLP-compatible spans, and don't want a 6MB dep tree, use `tiny-trace-otel`. If you need gRPC OTLP streaming for >10k spans/sec, use the official SDK.

---

## See also (OpenClaw ecosystem)

- [`tiny-otel`](https://github.com/hussain-alsaibai/tiny-otel) (Node.js) — sibling tracing lib for JS agents.
- [`tiny-cost-tracker`](https://github.com/hussain-alsaibai/tiny-cost-tracker) — pairs with span attributes for cost attribution.
- [`tiny-mcp-server`](https://github.com/hussain-alsaibai/tiny-mcp-server) — wraps MCP traffic in OTLP spans.
- [`tiny-budget-ledger`](https://github.com/hussain-alsaibai/tiny-budget-ledger) — gate traces by budget.

---

## Testing

```bash
python3 -m unittest test_tiny_trace_otel.py -v
```

Current: **25 / 25 passing.** Covers: trace context encode/decode, attribute coercion, span lifecycle, exception recording, parent/child relationships, OTLP envelope shape, stdout exporter, batch processor overflow, HTTP exporter success path.

---

## License

MIT — see [LICENSE](LICENSE).
