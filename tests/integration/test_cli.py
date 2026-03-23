from __future__ import annotations

from dataclasses import dataclass

from typer.testing import CliRunner

from vntdr.cli import app
from vntdr.models import HealthCheckResult, MonitorResult, SyncResult

runner = CliRunner()


@dataclass
class FakeContext:
    health_result: HealthCheckResult
    sync_result: SyncResult
    monitor_result: MonitorResult | None = None

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
