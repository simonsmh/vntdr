# `tests` 测试体系 Wiki

测试是项目业务契约的可执行索引。全局 pytest 配置在 `pyproject.toml`（`testpaths=tests`、默认 `-q`），fixture 在 `conftest.py`；目录细节见 [`unit/AGENTS.md`](unit/AGENTS.md) 和 [`integration/AGENTS.md`](integration/AGENTS.md)。

## 公共 fixture 与隔离

- `env_map` 提供本地测试用 PostgreSQL/Redis/报告目录环境值；测试不得读取生产 `.env` 或真实密钥。
- `sample_bar_payloads` 提供 BTC 1m 样本，`sample_xau_bar_payloads` 提供 XAU 4h 样本；研究测试通常把 `Database` 指向临时 SQLite 文件，并显式设置 `VNTDR_DATABASE_URL`/`VNTDR_REPORT_DIR`。
- 外部接口通过 fake client、monkeypatch 或注入的 `connection_factory` 隔离；标记 `integration` 的测试可触及更宽的应用边界，`test_okx_real_api.py` 需要外部环境/凭据时应显式处理。

## 测试分层与风险地图

| 层级 | 目标 | 典型风险 |
|---|---|---|
| `unit/` | 单个模型、策略、服务、适配器、Repository 和 UI helper 的确定性行为 | 时序、防泄漏、成本/风控、错误码、序列化和状态机 |
| `integration/` | CLI、数据库迁移、行情同步、监控、研究工作流和 Telegram 应用边界 | 组装顺序、持久化幂等、闭 K 处理、报告产物、命令退出码 |

推荐执行：

```bash
uv run pytest
uv run pytest tests/unit
uv run pytest tests/integration
uv run pytest tests/unit/test_event_driven_backtest.py -q
```

新增或修改代码必须按最近模块的 `AGENTS.md` 更新测试映射；涉及交易、时间可用性、迁移、Redis 状态、Telegram 权限或部署配置时不能只补 happy path。
