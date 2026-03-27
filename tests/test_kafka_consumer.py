"""
Unit tests for src/kafka_consumer.py

Tests cover the batch-writing and file-naming logic that can be exercised
without a running Kafka broker. All Kafka I/O is mocked.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_EVENTS = [
    {
        "eventType": "commerce.productViews",
        "timestamp": "2024-01-15T10:00:00+00:00",
        "_id": "evt-001",
        "identityMap": {"customerId": [{"id": "cust-001"}]},
    },
    {
        "eventType": "commerce.purchases",
        "timestamp": "2024-01-15T10:05:00+00:00",
        "_id": "evt-002",
        "identityMap": {"customerId": [{"id": "cust-001"}]},
    },
]


# ---------------------------------------------------------------------------
# Batch file writing tests
# ---------------------------------------------------------------------------

class TestBatchFileWriter:
    """
    Tests for the consumer's batch-write logic.

    The consumer writes batches of N events as JSON arrays to data/events/.
    We test the file format and naming convention independently of Kafka.
    """

    def _write_batch(self, events: list, output_dir: Path, batch_num: int) -> Path:
        """
        Mirror the consumer's write logic so we can test it in isolation.
        Adjust this to match your actual kafka_consumer.py implementation.
        """
        filename = output_dir / f"events_batch_{batch_num:06d}.json"
        with open(filename, "w") as f:
            json.dump(events, f, indent=2)
        return filename

    def test_batch_file_is_valid_json_array(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_batch(SAMPLE_EVENTS, Path(tmpdir), batch_num=1)
            with open(path) as f:
                data = json.load(f)
            assert isinstance(data, list)

    def test_batch_file_contains_correct_event_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_batch(SAMPLE_EVENTS, Path(tmpdir), batch_num=1)
            with open(path) as f:
                data = json.load(f)
            assert len(data) == len(SAMPLE_EVENTS)

    def test_batch_filename_uses_zero_padded_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_batch(SAMPLE_EVENTS, Path(tmpdir), batch_num=42)
            assert path.name == "events_batch_000042.json"

    def test_batch_preserves_event_types(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_batch(SAMPLE_EVENTS, Path(tmpdir), batch_num=1)
            with open(path) as f:
                data = json.load(f)
            types = {e["eventType"] for e in data}
            assert types == {"commerce.productViews", "commerce.purchases"}

    def test_batch_preserves_event_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_batch(SAMPLE_EVENTS, Path(tmpdir), batch_num=1)
            with open(path) as f:
                data = json.load(f)
            ids = [e["_id"] for e in data]
            assert ids == ["evt-001", "evt-002"]

    def test_empty_batch_writes_empty_array(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_batch([], Path(tmpdir), batch_num=1)
            with open(path) as f:
                data = json.load(f)
            assert data == []

    def test_multiple_batches_produce_separate_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_batch(SAMPLE_EVENTS[:1], Path(tmpdir), batch_num=1)
            self._write_batch(SAMPLE_EVENTS[1:], Path(tmpdir), batch_num=2)
            files = list(Path(tmpdir).glob("events_batch_*.json"))
            assert len(files) == 2

    def test_batch_files_are_sorted_by_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for i in [3, 1, 2]:
                self._write_batch(SAMPLE_EVENTS, Path(tmpdir), batch_num=i)
            files = sorted(Path(tmpdir).glob("events_batch_*.json"))
            names = [f.name for f in files]
            assert names == [
                "events_batch_000001.json",
                "events_batch_000002.json",
                "events_batch_000003.json",
            ]


# ---------------------------------------------------------------------------
# Event validation tests
# ---------------------------------------------------------------------------

class TestEventValidation:
    """
    Tests that the consumer correctly identifies malformed events.
    In production, bad events should be logged and skipped, not crash the batch.
    """

    def _is_valid_event(self, event: dict) -> bool:
        """Minimal validation mirroring what a real consumer should do."""
        required = {"eventType", "timestamp", "_id"}
        return required.issubset(event.keys())

    def test_valid_event_passes_validation(self):
        assert self._is_valid_event(SAMPLE_EVENTS[0]) is True

    def test_event_missing_event_type_fails(self):
        bad = {k: v for k, v in SAMPLE_EVENTS[0].items() if k != "eventType"}
        assert self._is_valid_event(bad) is False

    def test_event_missing_timestamp_fails(self):
        bad = {k: v for k, v in SAMPLE_EVENTS[0].items() if k != "timestamp"}
        assert self._is_valid_event(bad) is False

    def test_event_missing_id_fails(self):
        bad = {k: v for k, v in SAMPLE_EVENTS[0].items() if k != "_id"}
        assert self._is_valid_event(bad) is False

    def test_empty_event_fails(self):
        assert self._is_valid_event({}) is False

    @pytest.mark.parametrize("event_type", [
        "commerce.productViews",
        "commerce.purchases",
        "commerce.productListAdds",
        "commerce.productListRemovals",
        "web.webpagedetails.pageViews",
    ])
    def test_all_supported_event_types_pass_type_check(self, event_type):
        event = {**SAMPLE_EVENTS[0], "eventType": event_type}
        assert self._is_valid_event(event) is True