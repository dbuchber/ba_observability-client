"""Tests for notification authoring and relay delivery.

The module under test holds no credentials by design, so these tests care about
two things: that a message reaches whichever route is registered, and that a
failure on any route is swallowed rather than raised into the caller's job.
"""

from __future__ import annotations

import json
import time
from typing import Any, Iterator

import pytest

from observability_client import notify

_NOTIFY_ENV_VARS = ("NOTIFY_ENABLED", "NOTIFY_MIN_UNITS")


@pytest.fixture(autouse=True)
def _reset_notify_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Clear env and the module's process-global routes around every test."""
    for name in _NOTIFY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    notify.set_relay(None)
    notify.set_direct_sender(None)
    yield
    notify.set_relay(None)
    notify.set_direct_sender(None)


class _FakeRelay:
    """Minimal stand-in for the observability client's log_event surface."""

    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.fail = fail

    def log_event(self, event: str, **fields: Any) -> None:
        if self.fail:
            raise RuntimeError("loki unreachable")
        self.events.append((event, fields))


class TestEnabled:
    def test_no_route_registered_is_disabled(self) -> None:
        assert notify.enabled() is False

    def test_relay_alone_enables(self) -> None:
        notify.set_relay(_FakeRelay())

        assert notify.enabled() is True

    def test_direct_sender_alone_enables(self) -> None:
        notify.set_direct_sender(lambda _text: True)

        assert notify.enabled() is True

    def test_env_switch_overrides_registered_routes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notify.set_relay(_FakeRelay())
        monkeypatch.setenv("NOTIFY_ENABLED", "0")

        assert notify.enabled() is False

    def test_enabled_for_applies_threshold(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notify.set_relay(_FakeRelay())
        monkeypatch.setenv("NOTIFY_MIN_UNITS", "50")

        assert notify.enabled_for(50) is False
        assert notify.enabled_for(51) is True

    def test_enabled_for_default_notifies_for_any_nonzero_job(self) -> None:
        notify.set_relay(_FakeRelay())

        assert notify.enabled_for(1) is True
        assert notify.enabled_for(0) is False


class TestSendRouting:
    def test_relay_receives_manual_notify_with_text(self) -> None:
        relay = _FakeRelay()
        notify.set_relay(relay)

        assert notify.send("hello") is True
        event, fields = relay.events[0]
        assert event == "manual_notify"
        assert fields["notify_text"] == "hello"
        assert fields["push"] is True

    def test_direct_flag_prefers_the_injected_sender(self) -> None:
        relay = _FakeRelay()
        seen: list[str] = []
        notify.set_relay(relay)
        notify.set_direct_sender(lambda text: bool(seen.append(text)) or True)

        assert notify.send("dying", direct=True) is True
        assert seen == ["dying"]
        assert relay.events == []

    def test_direct_flag_falls_back_to_relay_without_a_sender(self) -> None:
        relay = _FakeRelay()
        notify.set_relay(relay)

        assert notify.send("dying", direct=True) is True
        assert relay.events[0][1]["notify_text"] == "dying"

    def test_relay_absent_uses_direct_sender_even_without_the_flag(self) -> None:
        seen: list[str] = []
        notify.set_direct_sender(lambda text: bool(seen.append(text)) or True)

        assert notify.send("progress") is True
        assert seen == ["progress"]

    def test_no_route_returns_false(self) -> None:
        assert notify.send("nowhere") is False

    def test_env_switch_suppresses_delivery(self, monkeypatch: pytest.MonkeyPatch) -> None:
        relay = _FakeRelay()
        notify.set_relay(relay)
        monkeypatch.setenv("NOTIFY_ENABLED", "off")

        assert notify.send("muted") is False
        assert relay.events == []


class TestSendNeverRaises:
    def test_relay_failure_is_swallowed(self) -> None:
        notify.set_relay(_FakeRelay(fail=True))

        assert notify.send("boom") is False

    def test_direct_failure_is_swallowed_and_does_not_reach_the_relay(self) -> None:
        relay = _FakeRelay()
        notify.set_relay(relay)

        def _explode(_text: str) -> bool:
            raise RuntimeError("no network")

        notify.set_direct_sender(_explode)

        # One route per call: escalating a direct failure to the relay would
        # deliver a message the caller asked to keep off that path.
        assert notify.send("dying", direct=True) is False
        assert relay.events == []

    def test_relay_failure_does_not_escalate_to_the_direct_sender(self) -> None:
        seen: list[str] = []
        notify.set_relay(_FakeRelay(fail=True))
        notify.set_direct_sender(lambda text: bool(seen.append(text)) or True)

        # A consumer whose credentials are present in the environment must not
        # get an outbound request it never asked for — this is what keeps a
        # stubbed test suite hermetic when the relay is broken.
        assert notify.send("progress") is False
        assert seen == []


class TestCompose:
    def test_layout_is_title_fields_then_timestamp(self) -> None:
        text = notify.compose("● TITLE", [("Stage", "filter"), ("Lines", "42")])

        lines = text.split("\n")
        assert lines[0] == "● TITLE"
        assert lines[1] == ""
        assert lines[2:4] == ["Stage: filter", "Lines: 42"]
        assert lines[-1].startswith("Timestamp: ")

    def test_empty_values_are_dropped(self) -> None:
        text = notify.compose("T", [("Kept", "yes"), ("Dropped", "")])

        assert "Kept: yes" in text
        assert "Dropped" not in text

    def test_blocks_are_appended_and_empty_ones_skipped(self) -> None:
        text = notify.compose("T", [("Stage", "filter")], blocks=["first\nsecond", "", "third"])

        # Each rendered block is preceded by exactly one blank line; the empty
        # one in between must not add a second.
        assert "Stage: filter\n\nfirst\nsecond\n\nthird\n\nTimestamp: " in text

    def test_stamp_makes_two_identical_messages_distinct_over_time(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stamps = iter(["01.08. 01:00:00", "01.08. 01:00:01"])
        monkeypatch.setattr(notify, "stamp", lambda: next(stamps))

        assert notify.compose("T", []) != notify.compose("T", [])


class TestDurations:
    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (0, "0s"),
            (-5, "0s"),
            (45, "45s"),
            (120, "2 min"),
            (5400, "1h 30min"),
            (7200, "2h"),
        ],
    )
    def test_human_duration(self, seconds: float, expected: str) -> None:
        assert notify.human_duration(seconds) == expected

    def test_clock_time_returns_local_wall_clock(self) -> None:
        assert notify.clock_time() == time.strftime("%H:%M")

    def test_clock_time_offsets_forward(self) -> None:
        expected = time.strftime("%H:%M", time.localtime(time.time() + 3600))

        assert notify.clock_time(3600) == expected

    def test_clock_time_clamps_negative_offsets(self) -> None:
        assert notify.clock_time(-3600) == time.strftime("%H:%M")


class TestConfigBlock:
    GROUPS = (
        ("Arm", ("arm",)),
        ("Retrieval", ("top_k", "collection")),
    )
    LABELS = {"arm": "", "top_k": "k"}

    def test_empty_params_render_nothing(self) -> None:
        assert notify.config_block(None) == ""
        assert notify.config_block({}) == ""

    def test_groups_render_in_declared_order(self) -> None:
        text = notify.config_block(
            {"collection": "entities", "arm": "full", "top_k": 5},
            groups=self.GROUPS,
            labels=self.LABELS,
        )

        assert text.split("\n") == [
            "Konfiguration:",
            "Arm: full",
            "Retrieval: k=5 · collection=entities",
        ]

    def test_ungrouped_keys_land_under_the_other_label(self) -> None:
        text = notify.config_block({"unknown": "x"}, groups=self.GROUPS)

        assert "Weitere: unknown=x" in text

    def test_skip_keys_and_suffixes_are_omitted(self) -> None:
        text = notify.config_block(
            {"arm": "full", "noisy": "x", "some_path": "/tmp/a"},
            groups=self.GROUPS,
            labels=self.LABELS,
            skip_keys={"noisy"},
            skip_suffixes=("_path",),
        )

        assert text == "Konfiguration:\nArm: full"

    def test_everything_skipped_renders_nothing(self) -> None:
        assert notify.config_block({"a_path": "/tmp"}, skip_suffixes=("_path",)) == ""

    def test_none_and_empty_values_are_dropped(self) -> None:
        assert notify.config_block({"a": None, "b": ""}) == ""

    def test_booleans_read_as_words(self) -> None:
        text = notify.config_block({"flag_on": True, "flag_off": False})

        assert "flag_on=an" in text
        assert "flag_off=aus" in text

    def test_stringified_booleans_read_the_same(self) -> None:
        text = notify.config_block({"flag": "True"})

        assert "flag=an" in text

    def test_long_values_are_truncated(self) -> None:
        text = notify.config_block({"key": "x" * 80})

        assert "…" in text
        assert "x" * 80 not in text

    def test_run_ids_are_shortened(self) -> None:
        text = notify.config_block({"source_run_id": "0123456789abcdef"})

        assert "source_run_id=01234567…" in text

    def test_limit_counts_the_dropped_remainder(self) -> None:
        params = {f"k{i}": i for i in range(5)}

        text = notify.config_block(params, limit=2)

        assert "(+3 weitere)" in text

    def test_limit_of_zero_reports_only_the_count(self) -> None:
        text = notify.config_block({"a": 1, "b": 2}, limit=0)

        assert text == "Konfiguration:\nWeitere: (+2 Parameter)"

    def test_headings_and_labels_are_overridable(self) -> None:
        text = notify.config_block({"a": 1}, heading="Config:", other_label="Other")

        assert text == "Config:\nOther: a=1"


class TestRelayIntegrationWithRealClient:
    def test_message_reaches_loki_as_manual_notify(
        self, make_client: Any, loki_capture: Any
    ) -> None:
        client = make_client()
        notify.set_relay(client)

        assert notify.send(notify.compose("● TEST", [("Stage", "filter")])) is True

        # The relay uses the asynchronous push path, so wait for the worker.
        assert loki_capture["delivered"].wait(2.0)
        body, _endpoint = loki_capture["bodies"][0]
        payload = json.loads(body["streams"][0]["values"][0][1])
        assert payload["message"] == "manual_notify"
        assert payload["notify_text"].startswith("● TEST")
        assert "Stage: filter" in payload["notify_text"]
