from __future__ import annotations

import base64
import os
import re

__all__ = ["path_to_safe_container_name"]



def path_to_safe_container_name(prefix:str,filepath: str) -> str:
    # Get absolute path
    abs_path = os.path.abspath(filepath)

    # Base64 encode
    encoded = base64.b64encode(abs_path.encode()).decode()

    # Replace characters that are invalid in container names
    safe = encoded.replace("+", "a").replace("/", "b").replace("=", "c")

    # Keep only lowercase letters, numbers, and hyphens
    safe = re.sub(r'[^a-z0-9-]', '-', safe.lower())

    # Ensure the name starts and ends with an alphanumeric character
    safe = re.sub(r'^[^a-z0-9]+', '', safe)
    safe = re.sub(r'[^a-z0-9]+$', '', safe)

    return prefix+"-"+safe
