from __future__ import annotations


def render_monitor_cli(*, config_path: str, workdir: str) -> str:
    from .tool import import_fluxon_pyo3_local

    fluxon_pyo3 = import_fluxon_pyo3_local()

    return fluxon_pyo3.monitor_render_cli(config_path, workdir)  # type: ignore[attr-defined]


def render_monitor_web(*, config_path: str, workdir: str) -> str:
    from .tool import import_fluxon_pyo3_local

    fluxon_pyo3 = import_fluxon_pyo3_local()

    return fluxon_pyo3.monitor_render_web(config_path, workdir)  # type: ignore[attr-defined]
