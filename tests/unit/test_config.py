from __future__ import annotations

from pathlib import Path

import pytest

from vntdr.config import ConfigurationError, Settings


def test_settings_load_nested_models(monkeypatch: pytest.MonkeyPatch, env_map: dict[str, str]) -> None:
    for key, value in env_map.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("OKX_API_KEY", "key")
    monkeypatch.setenv("OKX_SECRET_KEY", "secret")
    monkeypatch.setenv("OKX_PASSPHRASE", "pass")
    monkeypatch.setenv("TG_BOT_TOKEN", "bot")
    monkeypatch.setenv("TG_CHAT_ID", "chat")

    settings = Settings.from_env()

    assert settings.okx.api_key.get_secret_value() == "key"
    assert settings.okx.trading_enabled is True
    assert settings.database.username == "tester"
    assert settings.redis.host == "localhost"
    assert settings.telegram.chat_id == "chat"
    assert settings.research.report_dir == Path(env_map["VNTDR_REPORT_DIR"])


def test_settings_validate_for_doctor_requires_database_password(
    monkeypatch: pytest.MonkeyPatch,
    env_map: dict[str, str],
) -> None:
    settings = Settings.from_mapping({key: value for key, value in env_map.items() if key != "PG_PASSWORD"})

    with pytest.raises(ConfigurationError):
        settings.validate_for("doctor")
