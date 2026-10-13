"""Luna: High-performance data engineering and analytics platform.

Designed for robust, out-of-core data processing and seamless transition from SAS.
"""

from __future__ import annotations

import logging
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("luna")
except PackageNotFoundError:
    __version__ = "0.1.1"

# Standard library NullHandler to avoid "No handler found" warnings (PEP 282)
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Locks wildcard imports: 'from luna import *' only exposes '__version__'
__all__ = ["__version__"]
