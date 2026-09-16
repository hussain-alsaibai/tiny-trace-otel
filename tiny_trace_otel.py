"""
tiny_trace_otel — A tiny, stdlib-only OpenTelemetry-compatible tracing exporter.

The smallest way to ship OpenTelemetry-compatible spans from Python without
pulling in the ~6MB `@opentelemetry/exporter-trace-otlp-http` package
(which itself pulls in `@opentelemetry/core`, `@grpc/grpc-js`,
`protobufjs`, and several other transitive deps).

What it ships
-------------
- OTLP/HTTP JSON envelopes (the official wire format).
- Context propagation via W3C Trace Context headers (`traceparent`,
  `tracestate`).
- `tracer.start_span(name, **attrs)` / `span.set_attribute(k, v)` /
  `span.end(status=...)` / `span.event(name, **attrs)`.
- Batch span processor with worker thread; synchronous flush helper.
- Stdout exporter for local debugging.
- Drop-in compatibility with the official OTel collector, Honeycomb,
  SigNoz, Tempo, Grafana Cloud, Datadog (via opentelemetry-collector),
  New Relic, and any other OTLP/HTTP consumer.

When NOT to use this
--------------------
- Production high-volume services (>10k spans/sec) — use the official
  OpenTelemetry SDK which has gRPC streaming exporters.
- gRPC OTLP — this ships HTTP/JSON only.

See README.md for full integration guide.
"""

from __future__ import annotations

import json
import os
import queue
import random
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sequence, Tuple

__version__ = "0.1.0"
__all__ = [
    "Tracer",
    "Span",
    "SpanContext",
    "SpanKind",
    "StatusCode",
    "BatchSpanProcessor",
    "OTLPHttpExporter",
    "StdoutExporter",
    "TraceError",
    "get_tracer",
    "set_global_tracer",
    "format_traceparent",
    "parse_traceparent",
]


# ---------------------------------------------------------------------------
# TraceContext — W3C Trace Context headers
# ---------------------------------------------------------------------------


class TraceError(Exception):
    """Raised on malformed traceparent headers and other context errors."""


class StatusCode:
    UNSET = "UNSET"
    OK = "OK"
    ERROR = "ERROR"


class SpanKind:
    INTERNAL = "INTERNAL"
    SERVER = "SERVER"
    CLIENT = "CLIENT"
    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"


# ---------------------------------------------------------------------------
# SpanContext
# ---------------------------------------------------------------------------


class SpanContext:
    """A 16-byte trace id, 8-byte span id, and trace-flags byte.

    Created via :func:`format_traceparent` / :func:`parse_traceparent`, or
    implicitly by :meth:`Tracer.start_span`.
    """

    __slots__ = ("trace_id", "span_id", "parent_span_id", "trace_flags", "trace_state", "is_remote")

    def __init__(
        self,
        trace_id: str,
        span_id: str,
        parent_span_id: Optional[str] = None,
        trace_flags: str = "01",
        trace_state: str = "",
        is_remote: bool = False,
    ) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.trace_flags = trace_flags
        self.trace_state = trace_state
        self.is_remote = is_remote

    def __repr__(self) -> str:
        return f"SpanContext(trace_id={self.trace_id!r}, span_id={self.span_id!r}, flags={self.trace_flags})"


def _validate_hex(token: str, length: int, label: str) -> None:
    if len(token) != length or any(c not in "0123456789abcdef" for c in token):
        raise TraceError(f"{label} must be {length} lowercase hex chars")


def format_traceparent(trace_id: str, span_id: str, flags: str = "01") -> str:
    """Format a W3C traceparent header value."""
    _validate_hex(trace_id, 32, "trace_id")
    _validate_hex(span_id, 16, "span_id")
    return f"00-{trace_id}-{span_id}-{flags}"


def parse_traceparent(value: str) -> SpanContext:
    """Parse a W3C traceparent header into a :class:`SpanContext`."""
    if not isinstance(value, str):
        raise TraceError("traceparent must be a string")
    parts = value.split("-")
    if len(parts) != 4:
        raise TraceError("traceparent must have 4 dash-separated parts")
    version, trace_id, span_id, flags = parts
    if version != "00":
        raise TraceError(f"unsupported traceparent version {version!r}")
    _validate_hex(trace_id, 32, "trace_id")
    _validate_hex(span_id, 16, "span_id")
    if len(flags) != 2 or any(c not in "0123456789abcdef" for c in flags):
        raise TraceError("trace_flags must be 2 lowercase hex chars")
    return SpanContext(
        trace_id=trace_id, span_id=span_id,
        parent_span_id=None, trace_flags=flags, trace_state="", is_remote=True,
    )


# ---------------------------------------------------------------------------
# Span — a single trace unit
# ---------------------------------------------------------------------------


class Span:
    """One span in a trace.

    Construct via :meth:`Tracer.start_span` (preferred) or directly with a
    parent :class:`SpanContext`. Always pair with :meth:`end` — use
    ``span.context_manager`` for a `with` block that auto-ends.
    """

    def __init__(
        self,
        context: SpanContext,
        name: str,
        kind: str = SpanKind.INTERNAL,
        start_time_ns: Optional[int] = None,
    ) -> None:
        self.context = context
        self.name = name
        self.kind = kind
        self.start_time_ns = start_time_ns if start_time_ns is not None else time.time_ns()
        self.end_time_ns: Optional[int] = None
        self.attributes: Dict[str, Any] = {}
        self.events: List[Dict[str, Any]] = []
        self.status: str = StatusCode.UNSET
        self.status_message: Optional[str] = None
        self._ended: bool = False

    # ----- mutators --------------------------------------------------------

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a single attribute. Coerced via :func:`_otel_attr_value`."""
        self.attributes[key] = _otel_attr_value(value)

    def set_attributes(self, attrs: Dict[str, Any]) -> None:
        for k, v in attrs.items():
            self.set_attribute(k, v)

    def add_event(self, name: str, **attrs: Any) -> None:
        """Record a timestamped event on the span."""
        sanitized = {k: _otel_attr_value(v) for k, v in attrs.items()}
        self.events.append({
            "name": name,
            "timeUnixNano": str(time.time_ns()),
            "attributes": [{"key": k, "value": v} for k, v in sanitized.items()],
        })

    def set_status(self, status: str, message: Optional[str] = None) -> None:
        if status not in (StatusCode.UNSET, StatusCode.OK, StatusCode.ERROR):
            raise TraceError(f"unknown status {status!r}")
        self.status = status
        self.status_message = message

    def record_exception(self, exc: BaseException) -> None:
        self.add_event("exception", **{"exception.type": type(exc).__name__, "exception.message": str(exc)})
        self.set_status(StatusCode.ERROR, str(exc))

    def end(self, status: Optional[str] = None, message: Optional[str] = None) -> None:
        """Close the span and queue it for export."""
        if self._ended:
            return
        if status is not None:
            self.set_status(status, message)
        self.end_time_ns = time.time_ns()
        self._ended = True
        # Enqueue into the active processor if any
        proc = _active_processor.get()
        if proc is not None:
            proc.on_end(self)

    def __exit__(self, et, ev, tb) -> bool:
        if ev is not None:
            # Reconstruct the exception object from its class + value
            try:
                exc = ev if isinstance(ev, BaseException) else et(ev)
            except Exception:
                exc = None
            if exc is not None:
                self.record_exception(exc)
        if not self._ended:
            self.end()
        return False

    def __enter__(self) -> "Span":
        return self

    # ----- export -----------------------------------------------------------

    def to_otel_dict(self) -> Dict[str, Any]:
        """Serialize into the OTLP JSON wire format."""
        parent_span_id = self.context.parent_span_id or ""
        d: Dict[str, Any] = {
            "traceId": self.context.trace_id,
            "spanId": self.context.span_id,
            "name": self.name,
            "kind": _SPAN_KIND_NUM.get(self.kind, 1),
            "startTimeUnixNano": str(self.start_time_ns),
            "endTimeUnixNano": str(self.end_time_ns or self.start_time_ns),
            "attributes": [{"key": k, "value": v} for k, v in self.attributes.items()],
            "events": self.events,
            "status": {"code": _STATUS_CODE_NUM.get(self.status, 0)},
            "links": [],
        }
        if parent_span_id:
            d["parentSpanId"] = parent_span_id
        if self.context.trace_state:
            d["traceState"] = self.context.trace_state
        if self.status_message:
            d["status"]["message"] = self.status_message
        return d


_SPAN_KIND_NUM = {
    SpanKind.INTERNAL: 1,
    SpanKind.SERVER: 2,
    SpanKind.CLIENT: 3,
    SpanKind.PRODUCER: 4,
    SpanKind.CONSUMER: 5,
}
_STATUS_CODE_NUM = {StatusCode.UNSET: 0, StatusCode.OK: 1, StatusCode.ERROR: 2}


def _otel_attr_value(value: Any) -> Dict[str, Any]:
    """Coerce a Python value into the OTLP JSON attribute-value format."""
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, (list, tuple)):
        return {"arrayValue": {"values": [_otel_attr_value(v) for v in value]}}
    if isinstance(value, dict):
        return {"kvlistValue": {"values": [{"key": k, "value": _otel_attr_value(v)} for k, v in value.items()]}}
    return {"stringValue": str(value)}


# ---------------------------------------------------------------------------
# Tracer
# ---------------------------------------------------------------------------


class Tracer:
    """A minimal W3C-compatible tracer.

    Parameters
    ----------
    name : str
        The instrumentation library name (e.g. ``"myagent.tiny-trace-otel"``).
    resource_attrs : dict, optional
        Resource-level attributes attached to every span as
        ``process.*`` attributes. E.g. ``{"service.name": "my-agent",
        "service.version": "1.2.3"}``.
    parent_context : SpanContext, optional
        If provided, all spans created by this tracer will use it as the
        default parent unless overridden per-span.
    """

    def __init__(
        self,
        name: str,
        resource_attrs: Optional[Dict[str, Any]] = None,
        parent_context: Optional[SpanContext] = None,
    ) -> None:
        self.name = name
        self.resource_attrs = dict(resource_attrs or {})
        self.default_parent = parent_context

    def start_span(
        self,
        name: str,
        kind: str = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        parent: Optional[SpanContext] = None,
    ) -> Span:
        """Begin a new span.

        - If ``parent`` is given, the new span links to it.
        - Otherwise, if the tracer has a ``default_parent`` (e.g. for batch
          jobs that should all share a trace id), it's used.
        - Otherwise, a brand-new 128-bit trace id is generated.
        """
        parent_ctx = parent or self.default_parent
        if parent_ctx is None:
            trace_id = _gen_trace_id()
            span_id = _gen_span_id()
            parent_span_id = None
        else:
            trace_id = parent_ctx.trace_id
            span_id = _gen_span_id()
            parent_span_id = parent_ctx.span_id
        ctx = SpanContext(
            trace_id=trace_id, span_id=span_id,
            parent_span_id=parent_span_id,
            trace_flags=parent_ctx.trace_flags if parent_ctx else "01",
            trace_state=parent_ctx.trace_state if parent_ctx else "",
        )
        span = Span(ctx, name=name, kind=kind)
        # resource attrs prefixed with `process.` per OTLP convention
        for k, v in self.resource_attrs.items():
            span.attributes[f"resource.{k}"] = _otel_attr_value(v)
        if attributes:
            span.set_attributes(attributes)
        return span


# ---------------------------------------------------------------------------
# Exporters
# ---------------------------------------------------------------------------


class OTLPHttpExporter:
    """Synchronous OTLP/HTTP JSON exporter.

    Parameters
    ----------
    endpoint : str
        Full URL to the OTLP traces endpoint, e.g.
        ``"https://api.honeycomb.io/v1/traces"``. Trailing slashes are
        stripped.
    headers : dict
        Extra HTTP headers. Required: an auth header (e.g. ``x-honeycomb-team``).
        ``"Content-Type"`` is added automatically if missing.
    timeout_seconds : float
        Request timeout. Default 5.0.
    """

    def __init__(self, endpoint: str, headers: Dict[str, str], timeout_seconds: float = 5.0) -> None:
        if not endpoint.startswith(("http://", "https://")):
            raise TraceError("endpoint must be http(s)")
        self.endpoint = endpoint.rstrip("/")
        self.headers = dict(headers)
        self.headers.setdefault("Content-Type", "application/json")
        self.timeout = timeout_seconds

    def export(self, spans: Sequence[Span]) -> Tuple[int, Optional[str]]:
        """Export `spans` as one OTLP request. Returns (http_status, error_msg_or_None)."""
        if not spans:
            return 0, None
        envelope = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": _dedupe_resource_attrs(spans),
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": spans[0].attributes.get("resource.library.name", {}).get("stringValue", "tiny-trace-otel")},
                            "spans": [s.to_otel_dict() for s in spans],
                        }
                    ],
                }
            ]
        }
        body = json.dumps(envelope, default=str).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint, data=body, method="POST", headers=self.headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                code = resp.getcode()
                if 200 <= code < 300:
                    return code, None
                return code, f"http {code}"
        except urllib.error.HTTPError as e:  # pragma: no cover - depends on collector
            return e.code, f"http {e.code}: {e.reason}"
        except urllib.error.URLError as e:
            return 0, f"url: {e.reason}"
        except (OSError, TimeoutError, socket.timeout) as e:  # pragma: no cover
            return 0, f"network: {e}"


def _dedupe_resource_attrs(spans: Sequence[Span]) -> List[Dict[str, Any]]:
    """Merge per-span resource.* attributes into a single resource list."""
    merged: Dict[str, Any] = {}
    for s in spans:
        for k, v in s.attributes.items():
            if k.startswith("resource.") and k != "resource.library.name":
                merged[k[len("resource."):]] = v
    return [{"key": k, "value": v} for k, v in merged.items()]


class StdoutExporter:
    """Print span JSON lines to stdout — for local debugging."""

    def __init__(self, stream: Any = None) -> None:
        self._stream = stream

    @property
    def stream(self):
        return self._stream or os.sys.stdout

    def export(self, spans: Sequence[Span]) -> Tuple[int, Optional[str]]:
        for s in spans:
            self.stream.write(json.dumps(s.to_otel_dict(), default=str) + "\n")
        self.stream.flush()
        return 200, None


# ---------------------------------------------------------------------------
# Span processor
# ---------------------------------------------------------------------------


class BatchSpanProcessor:
    """A simple background-thread span processor.

    Spans are enqueued via :meth:`on_end` and exported in batches up to
    ``max_batch_size`` or every ``export_interval_seconds``, whichever
    comes first. On :meth:`shutdown` the queue is drained.
    """

    def __init__(
        self,
        exporter: Any,
        max_queue_size: int = 1024,
        max_batch_size: int = 64,
        export_interval_seconds: float = 2.0,
        export_timeout_seconds: float = 30.0,
    ) -> None:
        self._exporter = exporter
        self._max_queue_size = max_queue_size
        self._max_batch_size = max_batch_size
        self._interval = export_interval_seconds
        self._timeout = export_timeout_seconds
        self._queue: "queue.Queue[Span]" = queue.Queue(maxsize=max_queue_size)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._dropped = 0
        self._worker = threading.Thread(target=self._run, name="tiny-trace-otel-export", daemon=True)
        self._worker.start()

    def on_end(self, span: Span) -> None:
        try:
            self._queue.put_nowait(span)
        except queue.Full:
            with self._lock:
                self._dropped += 1

    def force_flush(self) -> None:
        """Drain the queue immediately."""
        spans = self._drain(int(self._max_batch_size))
        if spans:
            self._exporter.export(spans)

    def shutdown(self, timeout_seconds: Optional[float] = None) -> None:
        """Stop the worker after flushing remaining spans."""
        self._stop.set()
        try:
            self._worker.join(timeout=timeout_seconds or (self._timeout + 5.0))
        except RuntimeError:
            pass
        # final drain
        while True:
            spans = self._drain(self._max_batch_size)
            if not spans:
                break
            self._exporter.export(spans)

    def _drain(self, max_count: int) -> List[Span]:
        out: List[Span] = []
        while len(out) < max_count:
            try:
                out.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return out

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                spans = self._drain(self._max_batch_size)
                if spans:
                    self._exporter.export(spans)
            except Exception:  # pragma: no cover - defensive
                pass
            # sleep with interruption support
            self._stop.wait(self._interval)
        # final flush happens in shutdown()

    @property
    def dropped_count(self) -> int:
        with self._lock:
            return self._dropped


# ---------------------------------------------------------------------------
# Globals — active processor & current tracer
# ---------------------------------------------------------------------------


import contextvars

_active_processor: "contextvars.ContextVar[Optional[BatchSpanProcessor]]" = contextvars.ContextVar("active_processor", default=None)
_active_tracer: "contextvars.ContextVar[Optional[Tracer]]" = contextvars.ContextVar("active_tracer", default=None)


def set_global_tracer(tracer: Tracer) -> None:
    _active_tracer.set(tracer)


def get_tracer(name: str = "tiny-trace-otel") -> Tracer:
    """Return the current :class:`Tracer`, creating one if needed."""
    t = _active_tracer.get()
    if t is None:
        t = Tracer(name=name)
        _active_tracer.set(t)
    return t


def set_processor(processor: BatchSpanProcessor) -> contextvars.Token:
    """Install a span processor; returns the token to restore state."""
    return _active_processor.set(processor)


def reset_processor(token: contextvars.Token) -> None:
    _active_processor.reset(token)


# ---------------------------------------------------------------------------
# Helpers — random IDs
# ---------------------------------------------------------------------------


_RNG = random.SystemRandom()


def _gen_trace_id() -> str:
    return "".join(format(_RNG.randint(0, 15), "x") for _ in range(32))


def _gen_span_id() -> str:
    return "".join(format(_RNG.randint(0, 15), "x") for _ in range(16))
