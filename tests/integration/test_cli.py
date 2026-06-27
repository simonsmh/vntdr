from __future__ import annotations

import sys
import threading
from dataclasses import dataclass
from types import SimpleNamespace

from typer.testing import CliRunner

from vntdr.cli import CommandContext, app
from vntdr.config import Settings
from vntdr.models import HealthCheckResult, MonitorResult, SyncResult
from vntdr.services.config_service import ConfigService

runner = CliRunner()


class FakeSignalStore:
    def get(self, key: str) -> int | None:
        return None
    
    def set(self, key: str, value: int) -> None:
        pass


class FakeMonitoringService:
    def __init__(self, monitor_result: MonitorResult) -> None:
        self.monitor_result = monitor_result
        self.signal_store = FakeSignalStore()
    
    def reconcile_positions(self, symbol: str) -> int | None:
        return 0
    
    def monitor_once(self, **_: object) -> MonitorResult:
        return self.monitor_result


@dataclass
class FakeContext:
    health_result: HealthCheckResult
    sync_result: SyncResult
    monitor_result: MonitorResult | None = None

    def __post_init__(self) -> None:
        if self.monitor_result is not None:
            self.monitoring_service = FakeMonitoringService(self.monitor_result)

    def doctor(self) -> HealthCheckResult:
        return self.health_result

    def sync_history(self, **_: object) -> SyncResult:
        return self.sync_result

    def monitor_once(self, **_: object) -> MonitorResult:
        if self.monitor_result is None:
            raise AssertionError("monitor_result must be configured for this test")
        return self.monitor_result


def test_doctor_returns_non_zero_when_dependency_is_unhealthy(monkeypatch) -> None:
    fake_context = FakeContext(
        health_result=HealthCheckResult(
            ok=False,
            checks={"database": False, "redis": True, "veighna": True},
        ),
        sync_result=SyncResult(job_id=1, inserted_count=0, cleaned_count=0, duplicates_removed=0),
    )
    monkeypatch.setattr("vntdr.cli.create_command_context", lambda settings: fake_context)

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 1
    assert "database" in result.stdout


def test_sync_history_command_prints_insert_summary(monkeypatch, env_map: dict[str, str]) -> None:
    fake_context = FakeContext(
        health_result=HealthCheckResult(
            ok=True,
            checks={"database": True, "redis": True, "veighna": False},
        ),
        sync_result=SyncResult(job_id=7, inserted_count=10, cleaned_count=10, duplicates_removed=0),
    )
    monkeypatch.setattr("vntdr.cli.create_command_context", lambda settings: fake_context)

    result = runner.invoke(
        app,
        [
            "sync-history",
            "--symbol",
            "BTC-USDT-SWAP",
            "--interval",
            "1m",
            "--start",
            "2026-01-01T00:00:00+00:00",
            "--end",
            "2026-01-01T00:09:00+00:00",
        ],
        env=env_map,
    )

    assert result.exit_code == 0
    assert "inserted=10" in result.stdout


def test_live_once_reports_signal_and_actions(monkeypatch, env_map: dict[str, str]) -> None:
    fake_context = FakeContext(
        health_result=HealthCheckResult(
            ok=True,
            checks={"database": True, "redis": True, "veighna": True},
        ),
        sync_result=SyncResult(job_id=7, inserted_count=10, cleaned_count=10, duplicates_removed=0),
        monitor_result=MonitorResult(
            symbol="XAUUSDT",
            interval="4h",
            strategy_name="cm_macd_ult_mtf",
            signal=-1,
            previous_signal=1,
            best_parameters={"fast_length": 3},
            actions=["sell_long", "sell_short"],
            notification_sent=True,
        ),
    )
    monkeypatch.setattr("vntdr.cli.create_command_context", lambda settings: fake_context)

    result = runner.invoke(
        app,
        ["live", "--once", "--symbol", "XAUUSDT", "--interval", "4h", "--strategy", "cm_macd_ult_mtf"],
        env={**env_map, "TG_BOT_TOKEN": "bot", "TG_CHAT_ID": "chat"},
    )

    assert result.exit_code == 0
    assert "signal=-1" in result.stdout
    assert "sell_short" in result.stdout


def test_gradio_command_uses_env_port(monkeypatch) -> None:
    called: list[int] = []
    monkeypatch.setitem(
        sys.modules,
        "vntdr.webapp",
        SimpleNamespace(main=lambda port: called.append(port)),
    )

    result = runner.invoke(app, ["gradio"], env={"GRADIO_PORT": "8787"})

    assert result.exit_code == 0
    assert called == [8787]


def test_gradio_command_option_overrides_env_port(monkeypatch) -> None:
    called: list[int] = []
    monkeypatch.setitem(
        sys.modules,
        "vntdr.webapp",
        SimpleNamespace(main=lambda port: called.append(port)),
    )

    result = runner.invoke(app, ["gradio", "--port", "9090"], env={"GRADIO_PORT": "8787"})

    assert result.exit_code == 0
    assert called == [9090]


def test_command_context_hot_reloads_okx_runtime_clients(tmp_path, env_map: dict[str, str]) -> None:
    settings = Settings.from_mapping(
        {
            **env_map,
            "OKX_API_KEY": "old-key",
            "OKX_SECRET_KEY": "old-secret",
            "OKX_PASSPHRASE": "old-passphrase",
            "OKX_DEMO_TRADING": "true",
        }
    )
    context = CommandContext.__new__(CommandContext)
    context.settings = settings
    context.monitoring_service = SimpleNamespace(order_executor="old-order")
    context.history_service = SimpleNamespace(history_client="old-history")
    context._runtime_config_lock = threading.Lock()
    context._okx_runtime_signature = context._okx_runtime_config_signature(settings)

    built_orders: list[str] = []
    built_history: list[str] = []

    def build_order(updated_settings: Settings) -> str:
        value = updated_settings.okx.api_key.get_secret_value()
        built_orders.append(value)
        return f"order:{value}"

    def build_history(updated_settings: Settings) -> str:
        value = f"history:{updated_settings.okx.demo_trading}"
        built_history.append(value)
        return value

    context._build_order_executor = build_order
    context._build_history_client = build_history
    config_service = ConfigService(settings, config_file=tmp_path / "config_override.json")
    config_service.set("okx.api_key", "new-key")

    context.refresh_runtime_config(config_service)
    context.refresh_runtime_config(config_service)

    assert context.monitoring_service.order_executor == "order:new-key"
    assert context.history_service.history_client == "history:True"
    assert built_orders == ["new-key"]
    assert built_history == ["history:True"]
