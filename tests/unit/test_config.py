from __future__ import annotations

from pathlib import Path

import pytest

from vntdr.config import ConfigurationError, Settings


def test_settings_load_nested_models(
    monkeypatch: pytest.MonkeyPatch,
    env_map: dict[str, str],
) -> None:
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
    settings = Settings.from_mapping(
        {key: value for key, value in env_map.items() if key != "PG_PASSWORD"}
    )

    with pytest.raises(ConfigurationError):
        settings.validate_for("doctor")


def test_config_service_reset_restores_shared_settings_in_place(
    tmp_path: Path,
    env_map: dict[str, str],
) -> None:
    from vntdr.services.config_service import ConfigService

    settings = Settings.from_mapping(env_map)
    original_settings = settings
    config = ConfigService(settings, config_file=tmp_path / "config_override.json")

    assert config.set("research.default_symbol", "BTC-USDT-SWAP")
    assert config.set("research.default_interval", "1h")
    assert config.reset("research.default_symbol")

    assert config.settings is original_settings
    assert config.get("research.default_symbol") == "XAU-USDT-SWAP"
    assert config.get("research.default_interval") == "1h"

    config.reset_all()
    assert config.settings is original_settings
    assert config.get("research.default_interval") == "4H"
