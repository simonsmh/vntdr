# `tests/integration` 集成测试 Wiki

集成测试验证模块组装和持久化边界；涉及外部服务的测试必须显式区分可离线运行和需要凭据/网络的情况。

| 文件 | 验证流程 |
|---|---|
| `test_alembic_migration.py` | Alembic upgrade 后策略平台表可用，检查迁移链和 metadata/数据库结构 |
| `test_cli.py` | Typer 命令退出码/输出、研究配置、doctor、同步摘要、live once、Gradio 端口和 OKX runtime 热加载 |
| `test_history_sync.py` | HistorySyncService 的重试、清洗、Repository 幂等入库和重复同步无重复事实 |
| `test_monitoring.py` | 监控长转空/空转、单闭 K 幂等、最小持仓后反转和通知动作 |
| `test_research_workflows.py` | backtest/optimize/walk-forward 报告、折收益拼接以及 XAU 4h CM MACD 候选 |
| `test_telegram_bot_commands.py` | Telegram 应用实际命令注册、状态面板、callback 刷新、陈旧状态过滤、watch job 和格式化 |
| `test_okx_real_api.py` | 可选的 OKX/Telegram 边界验证；运行前必须确认凭据、demo 环境和网络策略，不能在 CI 中隐式调用生产账户 |

ETF 动态市值池的离线单元覆盖位于 `tests/unit/test_akshare_fund_flow.py` 和
`tests/unit/test_etf_flow_ingestion.py`；集成层应继续验证其与 CLI/Compose 配置的边界，而不把当前市值快照当成历史回测事实。

集成失败排查顺序：先检查环境变量和数据库/Redis 可用性，再检查迁移 revision、时间窗口/样本是否足够，最后检查外部 API。修改 CLI 入口、Compose 启动顺序、迁移、监控状态机或 Telegram handler 时必须同步本文件、对应服务文档和根 Wiki。
