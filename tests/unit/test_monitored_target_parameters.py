from types import SimpleNamespace

from vntdr.cli import CommandContext
from vntdr.config import Settings
from vntdr.services.config_service import ConfigService
from vntdr.webapp import _get_target_parameters


def test_target_parameters_override_only_the_target_with_parameters(
    tmp_path, env_map: dict[str, str]
) -> None:
    settings = Settings.from_mapping(
        {
            **env_map,
            "VNTDR_DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research.sqlite3'}",
        }
    )
    config = ConfigService(settings, config_file=tmp_path / "config_override.json")
    config.set("research.strategy_parameters", {"cm_macd_ult_mtf": {"fast_length": 4}})

    edited_target = {
        "strategy_name": "cm_macd_ult_mtf",
        "parameters": {"fast_length": 6, "slow_length": 21},
    }
    untouched_target = {"strategy_name": "cm_macd_ult_mtf"}

    assert _get_target_parameters(edited_target, config) == {
        "fast_length": 6,
        "slow_length": 21,
    }
    assert _get_target_parameters(untouched_target, config) == {"fast_length": 4}


def test_command_context_forwards_target_parameters_to_monitoring(
    env_map: dict[str, str],
) -> None:
    settings = Settings.from_mapping(env_map)
    context = CommandContext.__new__(CommandContext)
    context.settings = settings
    context.refresh_runtime_config = lambda: None
    captured: dict[str, object] = {}
    context.monitoring_service = SimpleNamespace(
        monitor_once=lambda **kwargs: captured.update(kwargs) or "result"
    )

    result = context.monitor_once(
        strategy_name="cm_macd_ult_mtf",
        symbol="XAU-USDT-SWAP",
        interval="4h",
        method="ga",
        volume=1.0,
        parameters={"fast_length": 6},
    )

    assert result == "result"
    assert captured["parameters"] == {"fast_length": 6}
