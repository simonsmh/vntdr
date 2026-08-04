# `tests/unit` 单元测试 Wiki

单元测试不应依赖真实网络、真实账户或共享生产数据库；优先使用临时 SQLite、fake Provider、mock API 和显式构造的 Settings。

## 文件到业务风险映射

| 测试文件 | 验证范围 |
|---|---|
| `test_models.py`、`test_config.py`、`test_cleaning.py` | Pydantic 时间/区间/研究配置、环境配置覆盖、K 线排序去重/缺口和周末日历 |
| `test_data_context_and_factors.py`、`test_data_quality_and_packs.py`、`test_exchange_isolation.py` | 辅助周期只读已关闭 K、因子 `available_at`、缺口/陈旧门禁、资产包和交易所隔离 |
| `test_cm_macd_strategy.py`、`test_indicator_strategies.py`、`test_multi_factor_strategy.py`、`test_trade_mode.py` | 策略信号、指标缓存、KDJ/RSI/成交量、多因子门槛/滞后/预注册空间和方向过滤 |
| `test_event_driven_backtest.py`、`test_backtest_costs.py`、`test_metrics.py`、`test_optimization_methods.py`、`test_factor_ablation.py`、`test_research_validation.py`、`test_multi_timeframe_research.py` | 下一根开盘成交、每 bar 成本、指标年化/回撤、搜索算法、固定消融、验证门禁和多周期防泄漏 |
| `test_external_factors.py`、`test_factor_sync.py`、`test_tradingview_history.py`、`test_okx_history_client.py`、`test_akshare_fund_flow.py`、`test_etf_flow_ingestion.py`、`test_etf_factor_model.py` | 外部源归一化、延迟可用时间、代理 symbol 隔离、OKX 分页/大小写、AkShare 重试与当前市值 ETF universe 筛选、ETF 任务级重试/审计、ETF 多因子标签/走查/评分 |
| `test_okx_order_executor.py`、`test_risk_manager.py`、`test_position_sizing.py`、`test_portfolio.py`、`test_portfolio_runtime.py`、`test_notify_only.py` | 订单翻译/重试、回撤/白名单/数量门禁、ATR sizing、组合敞口及通知模式安全默认 |
| `test_strategy_versioning.py`、`test_strategy_runtime.py`、`test_governance.py`、`test_shadow_runs.py` | 版本快照、闭 K 生效时间、激活/回滚审批、影子权益/回撤 |
| `test_monitoring_instance.py`、`test_monitoring.py`、`test_monitored_target_parameters.py` | 实例版本解析、辅助数据传递、监控目标参数覆盖和状态处理 |
| `test_repository_case_insensitivity.py` | Repository 读取周期大小写兼容 |
| `test_telegram_research_service.py`、`test_telegram_bot_formatting.py` | 周期排名、格式化、chat 权限、Redis key 和 Telegram 降级 |
| `test_webapp_helpers.py`、`test_responsive_webapp.py`、`test_dockerfile.py` | 参数空间/平台/健康表格、Gradio 回调防旧请求、Docker 构建约束 |

修改行为时，应优先在对应文件补回归测试，并同步父目录、模块文档和根 Wiki；删除测试必须说明覆盖迁移原因。
