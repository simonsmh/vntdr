from __future__ import annotations

from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values
from pydantic import BaseModel, Field, SecretStr


class ConfigurationError(ValueError):
    """Raised when the environment is missing required configuration."""


class OkxSettings(BaseModel):
    api_key: SecretStr | None = None
    secret_key: SecretStr | None = None
    passphrase: SecretStr | None = None
    rest_base_url: str = "https://www.okx.com"
    demo_trading: bool = False


class DatabaseSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    username: str | None = None
    password: SecretStr | None = None
    database_name: str | None = None
    url: str | None = None

    @property
    def dsn(self) -> str:
        if self.url:
            return self.url
        if not all([self.username, self.password, self.database_name]):
            raise ConfigurationError("Database credentials are incomplete.")
        password = self.password.get_secret_value()
        return (
            f"postgresql+psycopg://{self.username}:{password}@{self.host}:{self.port}/"
            f"{self.database_name}"
        )


class RedisSettings(BaseModel):
    host: str = "localhost"
    port: int = 6379
    db: int = 0

    @property
    def url(self) -> str:
        return f"redis://{self.host}:{self.port}/{self.db}"


class TelegramSettings(BaseModel):
    bot_token: SecretStr | None = None
    chat_id: str | None = None


class ResearchSettings(BaseModel):
    report_dir: Path = Path("reports")
    sync_retry_count: int = 3
    sync_batch_limit: int = 100
    default_warmup_days: int = 10
    default_strategy: str = "cm_macd_ult_mtf"
    default_symbol: str = "XAUUSDT"
    default_interval: str = "4h"
    monitor_lookback_bars: int = 120
    default_order_size: float = 1.0


class RiskSettings(BaseModel):
    max_strategy_capital: float = Field(default=0.30, ge=0.0, le=1.0)
    max_total_exposure: float = Field(default=0.60, ge=0.0, le=1.0)
    max_drawdown: float = Field(default=0.02, ge=0.0, le=1.0)


class Settings(BaseModel):
    okx: OkxSettings
    database: DatabaseSettings
    redis: RedisSettings
    telegram: TelegramSettings
    research: ResearchSettings
    risk: RiskSettings

    @classmethod
    def from_env(cls) -> "Settings":
        from os import environ

        env_mapping = {}
        env_file = Path(".env")
        if env_file.exists():
            env_mapping.update({key: value for key, value in dotenv_values(env_file).items() if value is not None})
        env_mapping.update(environ)
        return cls.from_mapping(env_mapping)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, str]) -> "Settings":
        database_url = mapping.get("VNTDR_DATABASE_URL")
        return cls(
            okx=OkxSettings(
                api_key=_secret(mapping.get("OKX_API_KEY")),
                secret_key=_secret(mapping.get("OKX_SECRET_KEY")),
                passphrase=_secret(mapping.get("OKX_PASSPHRASE")),
                rest_base_url=mapping.get("OKX_REST_BASE_URL", "https://www.okx.com"),
                demo_trading=_to_bool(mapping.get("OKX_DEMO_TRADING", "false")),
            ),
            database=DatabaseSettings(
                host=mapping.get("PG_HOST", "localhost"),
                port=int(mapping.get("PG_PORT", "5432")),
                username=mapping.get("PG_USER"),
                password=_secret(mapping.get("PG_PASSWORD")),
                database_name=mapping.get("PG_DB_NAME"),
                url=database_url,
            ),
            redis=RedisSettings(
                host=mapping.get("REDIS_HOST", "localhost"),
                port=int(mapping.get("REDIS_PORT", "6379")),
                db=int(mapping.get("REDIS_DB", "0")),
            ),
            telegram=TelegramSettings(
                bot_token=_secret(mapping.get("TG_BOT_TOKEN")),
                chat_id=mapping.get("TG_CHAT_ID"),
            ),
            research=ResearchSettings(
                report_dir=Path(mapping.get("VNTDR_REPORT_DIR", "reports")),
                sync_retry_count=int(mapping.get("VNTDR_SYNC_RETRY_COUNT", "3")),
                sync_batch_limit=int(mapping.get("VNTDR_SYNC_BATCH_LIMIT", "100")),
                default_warmup_days=int(mapping.get("VNTDR_DEFAULT_WARMUP_DAYS", "10")),
                default_strategy=mapping.get("VNTDR_DEFAULT_STRATEGY", "cm_macd_ult_mtf"),
                default_symbol=mapping.get("VNTDR_DEFAULT_SYMBOL", "XAUUSDT"),
                default_interval=mapping.get("VNTDR_DEFAULT_INTERVAL", "4h"),
                monitor_lookback_bars=int(mapping.get("VNTDR_MONITOR_LOOKBACK_BARS", "120")),
                default_order_size=float(mapping.get("VNTDR_DEFAULT_ORDER_SIZE", "1.0")),
            ),
            risk=RiskSettings(
                max_strategy_capital=float(mapping.get("VNTDR_MAX_STRATEGY_CAPITAL", "0.30")),
                max_total_exposure=float(mapping.get("VNTDR_MAX_TOTAL_EXPOSURE", "0.60")),
                max_drawdown=float(mapping.get("VNTDR_MAX_DRAWDOWN", "0.02")),
            ),
        )

    def validate_for(self, command_name: str) -> None:
        validators = {
            "doctor": self._validate_database,
            "sync-history": self._validate_database,
            "backtest": self._validate_database,
            "optimize": self._validate_database,
            "walk-forward": self._validate_database,
            "live": self._validate_live,
        }
        validator = validators.get(command_name)
        if validator:
            validator()
        self.research.report_dir.mkdir(parents=True, exist_ok=True)

    def _validate_database(self) -> None:
        if not self.database.url and (
            not self.database.username or not self.database.password or not self.database.database_name
        ):
            raise ConfigurationError("PG_USER, PG_PASSWORD and PG_DB_NAME are required.")

    def _validate_live(self) -> None:
        self._validate_database()
        if not self.telegram.bot_token or not self.telegram.chat_id:
            raise ConfigurationError("Telegram credentials are required for live mode.")


def _secret(value: str | None) -> SecretStr | None:
    return SecretStr(value) if value else None


def _to_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}
