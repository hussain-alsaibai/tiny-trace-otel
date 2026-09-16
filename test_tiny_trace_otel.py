"""Tests for tiny_trace_otel.

Run with::

    python -m unittest test_tiny_trace_otel.py -v

These tests focus on:
- W3C trace context encode/decode round-trips
- Span lifecycle (start, attributes, events, status, end)
- Tracer parent/child relationships
- OTLP JSON envelope correctness
- StdoutExporter captures output
- BatchSpanProcessor queues and drains
- Error paths (malformed traceparent, unknown status)
"""

from __future__ import annotations

import json
import io
import time
import unittest
from unittest.mock import patch

from tiny_trace_otel import (
    Tracer,
    Span,
    SpanContext,
    SpanKind,
    StatusCode,
    BatchSpanProcessor,
    OTLPHttpExporter,
    StdoutExporter,
    TraceError,
    format_traceparent,
    parse_traceparent,
    _otel_attr_value,
)


class TestTraceContext(unittest.TestCase):
    def test_traceparent_roundtrip(self) -> None:
        tp = "00-00000000000000001111111111111111-2222222222222222-01"
        ctx = parse_traceparent(tp)
        self.assertEqual(ctx.trace_id, "00000000000000001111111111111111")
        self.assertEqual(ctx.span_id, "2222222222222222")
        self.assertEqual(ctx.trace_flags, "01")
        self.assertTrue(ctx.is_remote)
        self.assertEqual(format_traceparent(ctx.trace_id, ctx.span_id, ctx.trace_flags), tp)

    def test_traceparent_rejects_bad_version(self) -> None:
        with self.assertRaises(TraceError):
            parse_traceparent("01-00000000000000001111111111111111-2222222222222222-01")

    def test_traceparent_rejects_bad_hex(self) -> None:
        with self.assertRaises(TraceError):
            parse_traceparent("00-01234567890-2222222222222222-01")
        with self.assertRaises(TraceError):
            parse_traceparent("00-00000000000000001111111111111111-222-01")
        with self.assertRaises(TraceError):
            parse_traceparent("00-00000000000000001111111111111111-2222222222222222-0g")

    def test_traceparent_rejects_wrong_format(self) -> None:
        for bad in ["", "00-00-00-0g", "garbage", None]:
            with self.assertRaises(TraceError):
                parse_traceparent(bad)  # type: ignore[arg-type]


class TestAttrCoercion(unittest.TestCase):
    def test_bool(self) -> None:
        self.assertEqual(_otel_attr_value(True), {"boolValue": True})
        self.assertEqual(_otel_attr_value(False), {"boolValue": False})

    def test_int(self) -> None:
        self.assertEqual(_otel_attr_value(42), {"intValue": "42"})

    def test_float(self) -> None:
        v = _otel_attr_value(3.14)
        self.assertIn("doubleValue", v)
        self.assertAlmostEqual(v["doubleValue"], 3.14, places=5)

    def test_list(self) -> None:
        v = _otel_attr_value([1, "a", True])
        self.assertEqual(v["arrayValue"]["values"], [
            {"intValue": "1"}, {"stringValue": "a"}, {"boolValue": True}
        ])

    def test_dict(self) -> None:
        v = _otel_attr_value({"a": 1})
        self.assertEqual(v["kvlistValue"]["values"], [{"key": "a", "value": {"intValue": "1"}}])


class TestSpan(unittest.TestCase):
    def test_basic_lifecycle(self) -> None:
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        s = Span(ctx, name="op", kind=SpanKind.INTERNAL)
        s.set_attribute("foo", "bar")
        s.add_event("checkpoint", step=1)
        s.end(StatusCode.OK)
        d = s.to_otel_dict()
        self.assertEqual(d["name"], "op")
        self.assertEqual(d["traceId"], "0" * 32)
        self.assertEqual(d["spanId"], "0" * 16)
        self.assertEqual(d["status"]["code"], 1)  # OK
        self.assertIn({"key": "foo", "value": {"stringValue": "bar"}}, d["attributes"])
        self.assertEqual(len(d["events"]), 1)
        self.assertEqual(d["events"][0]["name"], "checkpoint")

    def test_end_is_idempotent(self) -> None:
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        s = Span(ctx, name="op")
        s.end()
        first_end_ns = s.end_time_ns
        s.end()
        self.assertEqual(s.end_time_ns, first_end_ns)

    def test_record_exception(self) -> None:
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        s = Span(ctx, name="op")
        try:
            raise RuntimeError("boom")
        except RuntimeError as exc:
            s.record_exception(exc)
        s.end()
        self.assertEqual(s.status, StatusCode.ERROR)
        self.assertEqual(s.status_message, "boom")
        names = [e["name"] for e in s.events]
        self.assertIn("exception", names)

    def test_unknown_status_raises(self) -> None:
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        s = Span(ctx, name="op")
        with self.assertRaises(TraceError):
            s.set_status("BOGUS")

    def test_context_manager(self) -> None:
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        with Span(ctx, name="cm") as s:
            s.set_attribute("inside", True)
        self.assertTrue(s._ended)
        self.assertEqual(s.attributes.get("inside"), {"boolValue": True})

    def test_context_manager_records_exception(self) -> None:
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        with self.assertRaises(RuntimeError):
            with Span(ctx, name="cm") as s:
                raise RuntimeError("inside")
        self.assertEqual(s.status, StatusCode.ERROR)


class TestTracer(unittest.TestCase):
    def test_starts_new_trace_by_default(self) -> None:
        t = Tracer(name="x")
        s = t.start_span("root")
        s.end()
        self.assertEqual(len(s.context.trace_id), 32)
        self.assertEqual(len(s.context.span_id), 16)
        self.assertIsNone(s.context.parent_span_id)

    def test_child_inherits_trace_id(self) -> None:
        t = Tracer(name="x")
        root = t.start_span("root")
        child = t.start_span("child", parent=root.context)
        root.end()
        child.end()
        self.assertEqual(child.context.trace_id, root.context.trace_id)
        self.assertNotEqual(child.context.span_id, root.context.span_id)
        self.assertEqual(child.context.parent_span_id, root.context.span_id)

    def test_default_parent(self) -> None:
        t = Tracer(name="x", parent_context=SpanContext(
            trace_id="0" * 32, span_id="0" * 16, trace_flags="01"))
        s = t.start_span("child")
        s.end()
        self.assertEqual(s.context.trace_id, "0" * 32)
        self.assertEqual(s.context.parent_span_id, "0" * 16)

    def test_resource_attrs_attached(self) -> None:
        t = Tracer(name="svc", resource_attrs={"service.name": "agent"})
        s = t.start_span("root")
        s.end()
        # resource attrs are prefixed with resource.
        self.assertIn("resource.service.name", s.attributes)


class TestStdoutExporter(unittest.TestCase):
    def test_writes_lines(self) -> None:
        buf = io.StringIO()
        e = StdoutExporter(stream=buf)
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        s = Span(ctx, name="hello"); s.end()
        code, err = e.export([s])
        self.assertEqual(code, 200)
        self.assertIsNone(err)
        lines = buf.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        d = json.loads(lines[0])
        self.assertEqual(d["name"], "hello")


class TestBatchSpanProcessor(unittest.TestCase):
    def test_queues_and_drains(self) -> None:
        class FakeExporter:
            def __init__(self) -> None:
                self.received: list = []
            def export(self, spans):
                self.received.extend(spans)
                return 200, None
        exporter = FakeExporter()
        proc = BatchSpanProcessor(exporter, max_queue_size=10, max_batch_size=3,
                                   export_interval_seconds=0.01)
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        for _ in range(5):
            s = Span(ctx, name="x")
            s.end()
            proc.on_end(s)
        proc.force_flush()
        proc.shutdown(timeout_seconds=1.0)
        # We exported exactly the max_batch_size (3) via force_flush, the rest on shutdown.
        self.assertEqual(len(exporter.received), 5)

    def test_drop_when_overflow(self) -> None:
        class FakeExporter:
            def export(self, spans): return 200, None
        proc = BatchSpanProcessor(FakeExporter(), max_queue_size=2, max_batch_size=1,
                                   export_interval_seconds=1.0)
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        # We don't even let it drain, just push more than the queue holds.
        for _ in range(5):
            s = Span(ctx, name="x"); s.end()
            proc.on_end(s)
        self.assertGreater(proc.dropped_count, 0)
        proc.shutdown(timeout_seconds=0.1)


class TestOTLPHttpExporter(unittest.TestCase):
    def test_endpoint_validation(self) -> None:
        with self.assertRaises(TraceError):
            OTLPHttpExporter(endpoint="ftp://example", headers={})

    @patch("urllib.request.urlopen")
    def test_export_success(self, mock_urlopen) -> None:
        class FakeResp:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def getcode(self): return 200
        mock_urlopen.return_value = FakeResp()
        e = OTLPHttpExporter(endpoint="https://example/v1/traces", headers={"x-key": "abc"})
        ctx = SpanContext(trace_id="0" * 32, span_id="0" * 16)
        s = Span(ctx, name="hello"); s.end()
        code, err = e.export([s])
        self.assertEqual(code, 200)
        self.assertIsNone(err)
        # verify body shape was sent
        sent = mock_urlopen.call_args.args[0]
        self.assertEqual(sent.method, "POST")
        body = json.loads(sent.data.decode("utf-8"))
        self.assertIn("resourceSpans", body)

    def test_empty_export(self) -> None:
        e = OTLPHttpExporter(endpoint="https://example/v1/traces", headers={"x-key": "abc"})
        code, err = e.export([])
        self.assertEqual(code, 0)
        self.assertIsNone(err)


if __name__ == "__main__":
    unittest.main(verbosity=2)
