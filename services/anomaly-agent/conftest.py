# conftest.py — pytest configuration for the anomaly-agent service.
# Placing conftest.py at the service root (services/anomaly-agent/) causes
# pytest to add this directory to sys.path automatically. This allows tests to
# write `from detection.statistical import ...` without installing the package —
# the detection/ directory is found relative to this conftest.
# This file intentionally has no content beyond this comment. pytest's own
# conftest discovery mechanism handles the sys.path insertion when it finds
# conftest.py in a directory that is NOT itself a Python package (no __init__.py).
