from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vntdr.config import Settings


class ConfigService:
    """动态配置管理服务，支持运行时修改配置并持久化"""

    # 配置项中文名称映射
    CONFIG_LABELS = {
        "research.default_strategy": "📊 默认策略",
        "research.enabled_strategies": "🧩 启用的策略",
        "research.default_symbol": "💰 默认交易对",
        "research.default_interval": "⏱️ 默认周期",
        "research.default_order_size": "📦 默认下单量",
        "research.default_rank_lookback_hours": "⏰ 回测默认回看小时数",
        "research.maker_fee_rate": "💵 Maker 手续费率",
        "research.taker_fee_rate": "💵 Taker 手续费率",
        "research.use_maker_fee": "⚡ 使用 Maker 费率",
        "research.slippage_bps": "📉 滑点（基点）",
        "research.spread_bps": "↔️ 买卖价差（基点）",
        "research.funding_rate_per_bar": "💱 每根K线资金费率",
        "research.optimize_target": "🎯 寻优打分排序指标",
        "research.execution_mode": "🚦 执行模式 (仅通知/模拟/实盘)",
        "risk.max_strategy_capital": "🛡️ 单策略最大资金",
        "risk.max_total_exposure": "🛡️ 最大总敞口",
        "risk.max_drawdown": "📉 最大回撤限制",
        "risk.max_order_size": "📦 最大下单量",
        "risk.allow_opening_trades": "✅ 允许开仓",
        "okx.api_key": "🔑 OKX API Key",
        "okx.secret_key": "🔑 OKX Secret Key",
        "okx.passphrase": "🔑 OKX Passphrase",
        "okx.demo_trading": "⚡ OKX 模拟交易",
    }

    def __init__(self, settings: Settings, config_file: Path | None = None):
        self.settings = settings
        # Keep the startup/environment values as the reset baseline.  The
        # Settings object is shared by the CLI/web worker, so later restores
        # must update it in place rather than replacing the object.
        self._base_settings = settings.model_copy(deep=True)
        self.config_file = config_file or Path.home() / ".vntdr" / "config_override.json"
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        self._overrides: dict[str, Any] = {}
        self._load_overrides()

    def _load_overrides(self) -> None:
        """加载覆盖的配置"""
        self._overrides = {}
        if self.config_file.exists():
            try:
                loaded = json.loads(self.config_file.read_text(encoding="utf-8"))
            except (OSError, TypeError, ValueError):
                loaded = {}
            if isinstance(loaded, dict):
                self._overrides = loaded
        self._restore_base_settings()

    def _restore_base_settings(self) -> None:
        """Restore startup values and reapply the current overrides in place."""
        restored = self._base_settings.model_copy(deep=True)
        for field_name in type(self.settings).model_fields:
            setattr(self.settings, field_name, getattr(restored, field_name))
        self._apply_overrides()

    def _save_overrides(self) -> None:
        """保存覆盖的配置"""
        with open(self.config_file, "w", encoding="utf-8") as f:
            json.dump(self._overrides, f, indent=2, ensure_ascii=False)

    def _apply_overrides(self) -> None:
        """应用覆盖配置到 settings 对象"""
        for key, value in self._overrides.items():
            self._set_setting(key, value, persist=False)

    def _is_secret_field(self, model: Any, field_name: str) -> bool:
        from pydantic import SecretStr

        # ``model_fields`` is a class attribute in Pydantic 2. Accessing it
        # through an instance emits a deprecation warning in 2.11 and will be
        # removed in Pydantic 3.
        model_fields = getattr(type(model), "model_fields", None)
        if model_fields is None:
            return False
        field_info = model_fields.get(field_name)
        if not field_info:
            return False
        ann = field_info.annotation
        if ann is SecretStr:
            return True
        if hasattr(ann, "__args__"):
            return any(arg is SecretStr for arg in ann.__args__)
        return False

    def _set_setting(self, key: str, value: Any, persist: bool = True) -> None:
        """设置单个配置项"""
        from pydantic import SecretStr
        # 解析嵌套 of key，例如 "research.maker_fee_rate"
        parts = key.split(".")
        if len(parts) == 1:
            # 顶级配置
            if hasattr(self.settings, parts[0]):
                if self._is_secret_field(self.settings, parts[0]) and not isinstance(
                    value, SecretStr
                ):
                    value = SecretStr(value) if value else None
                setattr(self.settings, parts[0], value)
        elif len(parts) == 2:
            # 嵌套配置，如 research.maker_fee_rate
            section = getattr(self.settings, parts[0], None)
            if section is not None and hasattr(section, parts[1]):
                if self._is_secret_field(section, parts[1]) and not isinstance(value, SecretStr):
                    value = SecretStr(value) if value else None
                setattr(section, parts[1], value)

        if persist:
            persist_value = value.get_secret_value() if isinstance(value, SecretStr) else value
            self._overrides[key] = persist_value
            self._save_overrides()

    def get(self, key: str) -> Any:
        """获取配置值"""
        parts = key.split(".")
        if len(parts) == 1:
            return getattr(self.settings, parts[0], None)
        elif len(parts) == 2:
            section = getattr(self.settings, parts[0], None)
            if section is not None:
                return getattr(section, parts[1], None)
        return None

    def set(self, key: str, value: Any) -> bool:
        """设置配置值"""
        # 验证配置键是否存在
        parts = key.split(".")
        if len(parts) == 1:
            if not hasattr(self.settings, parts[0]):
                return False
        elif len(parts) == 2:
            section = getattr(self.settings, parts[0], None)
            if section is None or not hasattr(section, parts[1]):
                return False
        else:
            return False

        # 类型转换
        current_value = self.get(key)
        if current_value is not None:
            try:
                if isinstance(current_value, bool):
                    value = str(value).lower() in {"1", "true", "yes", "on"}
                elif isinstance(current_value, int):
                    value = int(float(value))
                elif isinstance(current_value, float):
                    value = float(value)
            except (ValueError, TypeError):
                return False

        self._set_setting(key, value)
        return True

    def list_all(self) -> dict[str, Any]:
        """列出所有可配置项"""
        result = {}

        # OKX 配置
        for key in ["api_key", "secret_key", "passphrase", "demo_trading"]:
            result[f"okx.{key}"] = getattr(self.settings.okx, key)

        # Research 配置
        for key in [
            "default_strategy",
            "enabled_strategies",
            "default_symbol",
            "default_interval",
            "default_order_size",
            "default_rank_lookback_hours",
            "maker_fee_rate",
            "taker_fee_rate",
            "use_maker_fee",
            "slippage_bps",
            "spread_bps",
            "funding_rate_per_bar",
            "optimize_target",
            "execution_mode",
        ]:
            result[f"research.{key}"] = getattr(self.settings.research, key)

        # Risk 配置
        for key in [
            "max_strategy_capital",
            "max_total_exposure",
            "max_drawdown",
            "max_order_size",
            "allow_opening_trades",
        ]:
            result[f"risk.{key}"] = getattr(self.settings.risk, key)

        return result

    def reset(self, key: str) -> bool:
        """重置单个配置项为默认值"""
        if key in self._overrides:
            del self._overrides[key]
            self._save_overrides()
            self._restore_base_settings()
            return True
        return False

    def reset_all(self) -> None:
        """重置所有配置为默认值"""
        self._overrides = {}
        self._save_overrides()
        self._restore_base_settings()
