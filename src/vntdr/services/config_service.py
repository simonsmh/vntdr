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
        "research.default_symbol": "💰 默认交易对",
        "research.default_interval": "⏱️ 默认周期",
        "research.default_order_size": "📦 默认下单量",
        "research.default_rank_lookback_hours": "⏰ 回测默认回看小时数",
        "research.maker_fee_rate": "💵 Maker 手续费率",
        "research.taker_fee_rate": "💵 Taker 手续费率",
        "research.use_maker_fee": "⚡ 使用 Maker 费率",
        "risk.max_strategy_capital": "🛡️ 单策略最大资金",
        "risk.max_total_exposure": "🛡️ 最大总敞口",
        "risk.max_drawdown": "📉 最大回撤限制",
        "risk.max_order_size": "📦 最大下单量",
        "risk.allow_opening_trades": "✅ 允许开仓",
    }

    def __init__(self, settings: Settings, config_file: Path | None = None):
        self.settings = settings
        self.config_file = config_file or Path.home() / ".vntdr" / "config_override.json"
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        self._overrides: dict[str, Any] = {}
        self._load_overrides()

    def _load_overrides(self) -> None:
        """加载覆盖的配置"""
        if self.config_file.exists():
            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    self._overrides = json.load(f)
                self._apply_overrides()
            except Exception:
                self._overrides = {}

    def _save_overrides(self) -> None:
        """保存覆盖的配置"""
        with open(self.config_file, "w", encoding="utf-8") as f:
            json.dump(self._overrides, f, indent=2, ensure_ascii=False)

    def _apply_overrides(self) -> None:
        """应用覆盖配置到 settings 对象"""
        for key, value in self._overrides.items():
            self._set_setting(key, value, persist=False)

    def _set_setting(self, key: str, value: Any, persist: bool = True) -> None:
        """设置单个配置项"""
        # 解析嵌套的 key，例如 "research.maker_fee_rate"
        parts = key.split(".")
        if len(parts) == 1:
            # 顶级配置
            if hasattr(self.settings, parts[0]):
                setattr(self.settings, parts[0], value)
        elif len(parts) == 2:
            # 嵌套配置，如 research.maker_fee_rate
            section = getattr(self.settings, parts[0], None)
            if section is not None and hasattr(section, parts[1]):
                setattr(section, parts[1], value)

        if persist:
            self._overrides[key] = value
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

        # Research 配置
        for key in [
            "default_strategy",
            "default_symbol",
            "default_interval",
            "default_order_size",
            "default_rank_lookback_hours",
            "maker_fee_rate",
            "taker_fee_rate",
            "use_maker_fee",
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
            # 重新加载 settings 来恢复默认值？或者需要更复杂的逻辑
            # 简单处理：删除覆盖后，下次重启会恢复默认
            return True
        return False

    def reset_all(self) -> None:
        """重置所有配置为默认值"""
        self._overrides = {}
        self._save_overrides()
