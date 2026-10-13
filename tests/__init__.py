"""Luna Connectors test suite."""

# Pre-import pandas if installed to ensure PyArrow extension type registrations
try:
    import pandas  # noqa: F401
except ImportError:
    pass
