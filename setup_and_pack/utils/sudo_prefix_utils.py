from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import List

__all__ = [
    "sudo_prefix",
    "host_sudo_prefix",
]



def sudo_prefix() -> List[str]:
    """Choose whether to use sudo for docker-related shell commands.

    We intentionally prefer running docker as the current user whenever possible.

    Causal chain:
    - Running docker with sudo makes host artifacts produced by `docker save/cp`
      root-owned, which then breaks follow-up steps that read those files without sudo.
    - Many dev machines have non-root docker access (user in docker group), so sudo
      is unnecessary and harmful.

    Strategy:
    - If running as root: no sudo.
    - If NoNewPrivs=1: no sudo (sudo would fail anyway).
    - If `docker info` works without sudo: no sudo.
    - Otherwise, if sudo is available and non-interactive: use sudo -n -E.
    - Otherwise: no sudo (caller will see a docker permission error).
    """
    if os.geteuid() == 0:
        return []

    status_path = Path('/proc/self/status')
    if status_path.exists():
        for raw in status_path.read_text(encoding='utf-8').splitlines():
            if raw.startswith('NoNewPrivs:'):
                if raw.split(':', 1)[1].strip() == '1':
                    return []
                break

    try:
        if shutil.which('docker'):
            r = subprocess.run(['docker', 'info'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
            if r.returncode == 0:
                return []
    except Exception:
        pass

    try:
        if shutil.which('sudo'):
            r = subprocess.run(['sudo', '-n', 'true'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
            if r.returncode == 0:
                return ['sudo', '-n', '-E']
    except Exception:
        pass

    return []


def host_sudo_prefix() -> List[str]:
    """Choose whether to use sudo for host filesystem commands.

    Causal chain:
    - containerized build steps can write root-owned artifacts into host bind mounts;
    - later host-side pack steps still need to chmod/copy those paths deterministically;
    - docker access and filesystem ownership are independent concerns, so docker-specific
      sudo routing must not be reused for host chmod/copy operations.

    Strategy:
    - If running as root: no sudo.
    - Otherwise, if sudo is available and non-interactive: use sudo -n.
    - Otherwise: no sudo and let the caller surface the permission error clearly.
    """
    if os.geteuid() == 0:
        return []

    try:
        if shutil.which('sudo'):
            r = subprocess.run(['sudo', '-n', 'true'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
            if r.returncode == 0:
                return ['sudo', '-n']
    except Exception:
        pass

    return []
