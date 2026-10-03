"""Configuration for the registry service."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class WindowExceededPolicy(enum.Enum):
    """Behaviour when a rollback is deeper than the configured window.

    ERROR: reject the reorg, keep the current committed history, and mark
           the service degraded (explicit, nothing silently dropped).
    CLAMP: accept the reorg but only roll back up to the window depth;
           the oldest entries are treated as final. Deterministic at the
           window boundary by construction.
    """

    ERROR = "error"
    CLAMP = "clamp"


@dataclass(frozen=True)
class SourceConfig:
    name: str
    # Lower number wins field-level conflicts.
    priority: int
    path: str = ""
    timeout_s: float = 5.0
    max_retries: int = 1


@dataclass(frozen=True)
class RegistryConfig:
    sources: tuple[SourceConfig, ...] = ()
    # How many recent entries may be rolled back by a reorg.
    rollback_window: int = 64
    window_exceeded_policy: WindowExceededPolicy = WindowExceededPolicy.ERROR
    # Reads older than this many seconds since last successful refresh are
    # reported as stale (but still served, explicitly flagged).
    staleness_ttl_s: float = 30.0
    # Background refresh interval; 0 disables the background loop.
    refresh_interval_s: float = 0.0
    # Directory for persisted committed snapshots (restart recovery).
    state_dir: str = ""
