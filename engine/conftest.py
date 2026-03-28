"""Root conftest.py — prevent pytest from collecting src/ as test modules."""
collect_ignore_glob = ["src/**/*.py"]
