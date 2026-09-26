"""Ganglion: a small control plane + worker runtime that launches Agent Spine runs.

Ganglion owns job intake, worker registration, atomic claim, lease/epoch fencing,
environment preparation, launching, heartbeats, crash reconciliation, retries,
notifications and result forwarding. Agent Spine owns all software-engineering logic.
"""

__version__ = "2.0.0"
