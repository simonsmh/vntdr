from __future__ import annotations

from pathlib import Path


def test_dockerfile_installs_project_after_copying_src() -> None:
    dockerfile = (Path(__file__).resolve().parents[2] / "Dockerfile").read_text()

    copy_lock_index = dockerfile.index("COPY pyproject.toml uv.lock README.md .python-version ./")
    deps_sync_index = dockerfile.index("uv sync --frozen --extra veighna --group dev --no-install-project")
    copy_src_index = dockerfile.index("COPY src ./src")
    project_sync_index = dockerfile.rindex("RUN uv sync --frozen --extra veighna --group dev")

    assert copy_lock_index < deps_sync_index < copy_src_index < project_sync_index
