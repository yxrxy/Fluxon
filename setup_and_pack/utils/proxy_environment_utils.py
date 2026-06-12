from __future__ import annotations

import os
from typing import Dict

__all__ = [
    "detect_proxy_settings",
    "normalize_proxy_env",
]



def detect_proxy_settings() -> Dict[str, str]:
    """Detect proxy settings from environment variables."""
    proxy_vars = [
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "no_proxy",
        "NO_PROXY",
        "ftp_proxy",
        "FTP_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ]

    detected_proxy = {}

    # Read from environment variables.
    for var in proxy_vars:
        if var in os.environ and os.environ[var].strip():
            detected_proxy[var] = os.environ[var]
            print(f"🌐 Detected proxy env var: {var}={os.environ[var]}")

    return detected_proxy


def normalize_proxy_env() -> Dict[str, str]:
    """Ensure proxy env vars exist in both cases (some tools only recognize one)."""
    pairs = [
        ("http_proxy", "HTTP_PROXY"),
        ("https_proxy", "HTTPS_PROXY"),
        ("no_proxy", "NO_PROXY"),
        ("ftp_proxy", "FTP_PROXY"),
        ("all_proxy", "ALL_PROXY"),
    ]

    updated: Dict[str, str] = {}
    for lower, upper in pairs:
        lower_val = os.environ.get(lower)
        upper_val = os.environ.get(upper)

        if lower_val and not upper_val:
            os.environ[upper] = lower_val
            updated[upper] = lower_val
        elif upper_val and not lower_val:
            os.environ[lower] = upper_val
            updated[lower] = upper_val

    return updated
