"""Compose lifecycle notifications and hand them to the Loki relay.

**This module writes the message text; Grafana delivers it.** Each message is
pushed to Loki as a ``manual_notify`` event carrying the finished text in
``notify_text``. A Grafana alert rule turns every distinct text into an alert
instance and a notification template renders ``{{ .Labels.notify_text }}``
verbatim, so the message that arrives on a phone is the one composed here —
Grafana is a transport, not the author.

That split is the reason this module can live in a shared library at all: it
holds **no credentials**. No bot token, no chat id, no call to a chat provider's
API. A repo that vendors this package gains notifications without gaining a
secret to manage, and muting or re-routing stays a Grafana config change instead
of a code change.

The relay rule selects ``{job="host-python"}`` and groups by
``(notify_text, service)``, so every consumer of this client lands in it as long
as it does not override ``LOKI_JOB`` — the ``service_name`` passed to
:class:`~observability_client.ObservabilityClient` is what keeps senders apart.
Two consequences worth knowing:

* Grouping by text means two byte-identical messages collapse into one alert
  instance and the second is never delivered. :func:`stamp` exists to prevent
  that, and :func:`compose` appends it to every message.
* A consumer that *does* need a credentialed fallback — for the one message that
  must survive a broken observability chain, typically "the job died" — injects
  its own transport with :func:`set_direct_sender`. The credential stays in that
  consumer; this module only calls the hook.

Notifications are a **side-channel, never a dependency**: every public function
here swallows its own errors. A job that has been running for three hours must
not die because a message could not be delivered.

Messages go out as **plain text on purpose**: model, service and file names are
full of underscores, which Telegram's Markdown parser rejects as unclosed
entities and then drops the message entirely.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Container, Mapping, Sequence
from typing import Any

_relay_obs: Any = None
_direct_sender: Callable[[str], bool] | None = None
_state_lock = threading.Lock()


def _truthy(value: str | None, *, default: bool) -> bool:
    """Interpret an env-var string as a flag.

    Args:
        value (str | None): Raw environment value, possibly unset or empty.
        default (bool): Result for an unset or empty value.

    Returns:
        bool: False only for an explicit falsy spelling.
    """
    if value is None or value == "":
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _int_env(name: str, default: int) -> int:
    """Read an integer environment variable, falling back on anything unparsable.

    Args:
        name (str): Environment variable name.
        default (int): Value used when unset, empty or not an integer.

    Returns:
        int: Resolved integer.
    """
    try:
        return int(os.environ.get(name, "") or default)
    except (TypeError, ValueError):
        return default


def set_relay(obs: Any) -> None:
    """Register the observability client that carries messages to Loki.

    Called once per process, typically where the client is built. Without a
    relay :func:`send` has nowhere to go and returns False unless a direct
    sender was injected.

    Args:
        obs (Any): Object exposing ``log_event(event, **fields)`` — an
            :class:`~observability_client.ObservabilityClient` or a test double.
    """
    global _relay_obs

    with _state_lock:
        _relay_obs = obs


def set_direct_sender(sender: Callable[[str], bool] | None) -> None:
    """Register a credentialed transport for messages that bypass the relay.

    This is the seam that keeps secrets out of this package. A consumer that
    holds a bot token registers a callable here; ``send(..., direct=True)`` then
    uses it for the one message whose delivery must not depend on Alloy, Loki
    and Grafana all being healthy.

    Args:
        sender (Callable[[str], bool] | None): Transport returning success, or
            None to unregister.
    """
    global _direct_sender

    with _state_lock:
        _direct_sender = sender


def enabled() -> bool:
    """Report whether a message could be delivered at all.

    Returns:
        bool: True when notifications are not switched off via
            ``NOTIFY_ENABLED`` and at least one route is registered.
    """
    if not _truthy(os.environ.get("NOTIFY_ENABLED"), default=True):
        return False
    return _relay_obs is not None or _direct_sender is not None


def enabled_for(units: int, *, min_units_env: str = "NOTIFY_MIN_UNITS", default_min: int = 0) -> bool:
    """Report whether a job of ``units`` items is worth notifying about.

    Small jobs are usually smoke tests, and nobody wants a phone buzzing for
    those. The threshold is env-configurable so the same code can be quiet in
    development and loud in production.

    Args:
        units (int): Size of the job — records, entities, lines, whatever the
            caller counts.
        min_units_env (str): Environment variable holding the threshold.
        default_min (int): Threshold when the variable is unset. The default of
            0 notifies for everything, which is the right default for a library
            that cannot know what "small" means for its caller.

    Returns:
        bool: True when notifications are possible and the job is big enough.
    """
    if not enabled():
        return False
    return units > _int_env(min_units_env, default_min)


def send(text: str, *, direct: bool = False) -> bool:
    """Deliver ``text``. Returns success, never raises.

    Exactly one route is attempted per call, and which one is decided before
    anything is sent — deliberately, with no cross-route retry. A relay failure
    must not silently escalate to the credentialed transport: for a consumer that
    holds credentials, that would turn a stubbed-out test into a real outbound
    request, and a muted channel into a delivered message.

    Args:
        text (str): Finished message, as produced by :func:`compose`.
        direct (bool): Use the injected direct sender. Falls back to the relay
            when no direct sender is registered, so a keyless consumer can mark
            a message urgent without knowing which routes exist.

    Returns:
        bool: True when the chosen route accepted the message.
    """
    if not _truthy(os.environ.get("NOTIFY_ENABLED"), default=True):
        return False
    if direct and _direct_sender is not None:
        return _send_direct(text)
    if _relay_obs is not None:
        return _send_via_relay(text)
    if _direct_sender is not None:
        return _send_direct(text)
    return False


#!node _send_via_relay
def _send_via_relay(text: str) -> bool:
    """Push the finished message to Loki for the Grafana relay rule to pick up."""
    try:
        _relay_obs.log_event("manual_notify", notify_text=text, push=True)
        return True
    except Exception:  # noqa: BLE001 — a lost notification never fails a job
        return False


def _send_direct(text: str) -> bool:
    """Hand the message to the consumer-supplied transport."""
    try:
        return bool(_direct_sender(text)) if _direct_sender else False
    except Exception:  # noqa: BLE001 — same reason as the relay path
        return False


def stamp() -> str:
    """Return the local wall-clock stamp carried by every message.

    Two purposes. It says *when* something happened without opening Grafana —
    and, more load-bearing, it makes each message text unique. The relay turns
    every distinct text into its own alert instance, so two byte-identical
    messages would otherwise collapse into one and the second would never be
    delivered.

    Returns:
        str: ``DD.MM. HH:MM:SS`` in local time.
    """
    return time.strftime("%d.%m. %H:%M:%S")


def compose(
    title: str,
    fields: Sequence[tuple[str, str]],
    *,
    blocks: Sequence[str] | None = None,
) -> str:
    """Lay out one notification: title, labelled fields, timestamp last.

    Every message shares this shape so a glance at the phone finds the same
    thing in the same place::

        ● FORTSCHRITT

        Stage: filter
        Fortschritt: 50 % (4.2M/8.4M Zeilen)
        ETA: ~12 min

        Timestamp: 01.08. 01:30:43

    Fields with an empty value are dropped, so callers can pass optional ones
    unconditionally. No padding or alignment: these render in a proportional
    font, where column padding only looks broken.

    Args:
        title (str): Headline line, conventionally an icon plus caps.
        fields (Sequence[tuple[str, str]]): ``(label, value)`` pairs; empty
            values are skipped.
        blocks (Sequence[str] | None): Multi-line sections appended after the
            fields, each separated by a blank line. Empty blocks are skipped.

    Returns:
        str: Finished plain-text message.
    """
    parts = [title, ""]
    parts.extend(f"{label}: {value}" for label, value in fields if value)
    for block in blocks or []:
        if block:
            parts.extend(["", block])
    parts.extend(["", f"Timestamp: {stamp()}"])
    return "\n".join(parts)


def human_duration(seconds: float) -> str:
    """Format a duration the way it should read on a phone.

    Args:
        seconds (float): Duration; negative values are clamped to zero.

    Returns:
        str: ``45s``, ``12 min``, ``3h 07min`` or ``3h``.
    """
    seconds = max(0.0, float(seconds))
    if seconds < 90:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 90:
        return f"{minutes:.0f} min"
    hours = int(minutes // 60)
    rest = int(minutes % 60)
    return f"{hours}h {rest:02d}min" if rest else f"{hours}h"


def clock_time(seconds_ahead: float = 0.0) -> str:
    """Return local wall-clock time ``seconds_ahead`` from now.

    A relative ETA ("~17 min") forces arithmetic against the message's own
    timestamp, and the answer is stale by the time the phone is unlocked. The
    absolute time it points at does not decay, so both are shown together.

    Args:
        seconds_ahead (float): Offset from now; negative values are clamped.

    Returns:
        str: ``HH:MM`` in local time.
    """
    return time.strftime("%H:%M", time.localtime(time.time() + max(0.0, seconds_ahead)))


def _config_value(key: str, value: Any, *, max_chars: int = 48) -> str:
    """Render one parameter value for a phone-sized message."""
    if isinstance(value, bool):
        return "an" if value else "aus"
    text = str(value)
    if text in ("True", "False"):
        return "an" if text == "True" else "aus"
    # A run/job id is identity, not information — the message header carries it.
    if key.endswith("_run_id") and len(text) > 12:
        return text[:8] + "…"
    if len(text) > max_chars:
        text = text[: max_chars - 3] + "…"
    return text


def _config_entry(key: str, value: Any, labels: Mapping[str, str]) -> str:
    """``key=value``, or the bare value where the group label carries the key."""
    label = labels.get(key, key)
    rendered = _config_value(key, value)
    return rendered if label == "" else f"{label}={rendered}"


def config_block(
    params: Mapping[str, Any] | None,
    *,
    groups: Sequence[tuple[str, Sequence[str]]] = (),
    labels: Mapping[str, str] | None = None,
    skip_keys: Container[str] = frozenset(),
    skip_suffixes: tuple[str, ...] = (),
    limit: int = 24,
    heading: str = "Konfiguration:",
    other_label: str = "Weitere",
) -> str:
    """Render a flat parameter dict as a small grouped table.

    This is what distinguishes two otherwise identical-looking jobs — which
    ablation arm, which collection, which dump — so it belongs in the message
    that arrives before you walk away.

    One line per *group* rather than per key: a line per key pushes the timestamp
    off the notification preview, while a single wrapped paragraph hides the one
    field you are looking for among twenty others.

    Keys absent from ``groups`` land under ``other_label`` rather than being
    dropped — a new knob must never vanish silently from the message that says
    what is running.

    Args:
        params (Mapping[str, Any] | None): Parameters to render. None or empty
            yields an empty string.
        groups (Sequence[tuple[str, Sequence[str]]]): ``(group label, keys)`` in
            display order. The vocabulary is the caller's: this module has no
            opinion about what a parameter means.
        labels (Mapping[str, str] | None): Per-key display names. An empty
            string means "print the value alone", for groups whose label already
            supplies the context.
        skip_keys (Container[str]): Keys to omit entirely, e.g. ones the caller
            already surfaces as their own line.
        skip_suffixes (tuple[str, ...]): Omit keys ending in any of these, e.g.
            ``('_path', '_fingerprint')`` for host-specific or machine-only
            values.
        limit (int): Cap on ungrouped keys rendered; the rest are counted.
        heading (str): First line of the block.
        other_label (str): Group label for keys not covered by ``groups``.

    Returns:
        str: Rendered block, or an empty string when nothing is left to show.
    """
    if not params:
        return ""
    label_map = labels or {}
    usable = {
        key: value
        for key, value in params.items()
        if key not in skip_keys
        and not (skip_suffixes and key.endswith(skip_suffixes))
        and value is not None
        and value != ""
    }
    if not usable:
        return ""

    lines: list[str] = []
    shown = 0
    claimed: set[str] = set()
    for label, keys in groups:
        present = [k for k in keys if k in usable]
        if not present:
            continue
        claimed.update(present)
        lines.append(
            f"{label}: " + " · ".join(_config_entry(k, usable[k], label_map) for k in present)
        )
        shown += len(present)

    rest = [k for k in usable if k not in claimed]
    if rest:
        remaining = max(0, limit - shown)
        head, dropped = rest[:remaining], len(rest) - remaining
        if head:
            tail = f" · (+{dropped} weitere)" if dropped > 0 else ""
            tail_entries = " · ".join(_config_entry(k, usable[k], label_map) for k in head)
            lines.append(f"{other_label}: {tail_entries}{tail}")
        elif dropped > 0:
            lines.append(f"{other_label}: (+{dropped} Parameter)")
    return f"{heading}\n" + "\n".join(lines)
