"""Backward-compatible import for the reusable observability client.

This module is kept for compatibility with existing examples. New code should
import :class:`observability_client.ObservabilityClient` directly.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to Python path to enable imports from observability_client
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from observability_client import ObservabilityClient


class ObservabilityLogger(ObservabilityClient):
    """Backward-compatible alias for :class:`ObservabilityClient`."""

