"""Top-level package for Airtable Analyzer."""

from importlib import metadata


def get_version() -> str:
    """Return the installed package version."""
    try:
        return metadata.version("airtable-analyzer")
    except metadata.PackageNotFoundError:
        return "0.1.0"


__all__ = ["get_version"]
