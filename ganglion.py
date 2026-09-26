#!/usr/bin/env python3
"""Compatibility entry point for Ganglion 1.x deployments.

Existing launchd plists and scripts run ``ganglion.py --config ... run``; that keeps working
(``run`` = ``worker start``, and a 1.x config maps to single-machine mode). New installs use
``bin/ganglion`` or the ``ganglion`` console script. The ``ganglion/`` package takes import
precedence over this file, so ``import ganglion`` always means the package.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

from ganglion.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
