# vntdr 项目 Wiki 与协作约束

本文档是仓库级项目说明和 Agent 工作约束。代码优先：当文档与实现冲突时，先以当前代码、迁移脚本和测试为准，再修正文档。各功能目录的细节存放在对应目录下的 `AGENTS.md`，进入子目录时应同时阅读本文件和最近的目录文档。

## 1. 项目目标与当前边界

vntdr 是一个以研究为先的量化交易平台，围绕 VeighNa、OKX、PostgreSQL、Redis 和 Gradio 构建，目标业务是：

1. 获取并治理市场/外部因子数据；
2. 使用可复现的策略进行回测、参数寻优、走查和消融；
3. 通过版本化策略、影子运行和审批门禁后，持续监控已启用实例；
4. 在收盘 K 线确认买卖方向后，通过 Telegram 通知，并由风控生成订单指令；
5. 在交易执行能力正式放开后，才允许通过 OKX 适配器进行多空开平仓和持仓对账。

保留的项目验收目标是：以 `src/vntdr/strategies/cm_macd_ult_mtf.py` 为主策略，在 24 小时验证窗口内围绕 `XAU-USDT-SWAP`（默认 `4H`）完成可复现的参数研究，持续监控已关闭 K 线的多空买卖点并通过 Telegram 通知。真实买入/卖出和多空持仓只有在执行链路经过单独安全评审、OKX demo 验证和发布审批后，才可纳入验收；当前发布版本不满足这一项。

当前 CM MACD 代码默认参数为 `fast=6`、`slow=21`、`signal=3`、`trend=7`、`trade_mode=both`。参数搜索空间以策略文件当前实现为准（fast `[2,4,6,8,10,12]`、slow `[10,15,20,25,30]`、signal/trend `[3,5,7,9]`）；旧文档中更窄的搜索空间不能当作现行契约。该策略名称中的 MTF 是设计意图，当前实现不是完整的多时间框架引擎。

当前仓库的实际安全边界必须优先于“真实交易”目标：

- `Settings.research.execution_mode` 支持 `notify_only`、`paper`、`live`，默认是 `notify_only`；
- `vntdr live` 在 `MonitoringService.monitor_once()` 中会生成信号、通知和经风控过滤后的动作，但当前发布实现明确抑制订单执行，日志会记录 `orders intentionally suppressed`；因此不能把当前系统描述成已经自动实盘下单；
- OKX API key/secret/passphrase 不完整时，`CommandContext` 使用 `SimulatedOrderExecutor`；完整凭据才会构造 `OkxOrderExecutor`，且 Compose 默认 `OKX_DEMO_TRADING=true`；
- `OkxOrderExecutor` 具备订单翻译、重试、持仓和权益查询能力，但不等于 live 主流程已授权真实下单；任何放开执行的变更都必须单独评审、补测试并同步本目录及 `src/vntdr/adapters/AGENTS.md`；
- 仓库没有 nginx 配置。域名、TLS 和反向代理属于外部部署层，不能作为仓库内已交付能力记录。

## 2. 运行架构

```text
                 ┌──────────────────────┐
                 │  Gradio webapp :7860 │
                 │  研究/配置/看板       │
                 └──────────┬───────────┘
                            │ PostgreSQL + 共享 config_override.json
┌──────────────┐    ┌────────▼───────────┐    ┌─────────────────────┐
│ OKX/TV/外部源 │───▶│  services + storage │◀───│ Redis signal/status │
└──────────────┘    │  数据/研究/运行时   │    └──────────┬──────────┘
                    └────────┬───────────┘               │
                             ▼                           │
                    ┌────────────────────┐               │
                    │ quant_core: vntdr live│────────────┘
                    │ 同步/收盘信号/通知    │
                    └────────┬───────────┘
                             ▼
                    Telegram + OKX adapter
                    （当前 live 仅通知，不执行订单）

                    etf_ingest: APScheduler
                    AkShare → ETF 两张表 → Gradio 只读展示
                                      └─ sklearn 多因子研究（只读）
```

Compose 中的服务：

| 服务 | 入口 | 职责 | 主要持久化 |
|---|---|---|---|
| `db` | PostgreSQL | 业务、行情、研究、策略治理和 ETF 审计数据 | `pg_data` |
| `cache` | Redis | 信号、已处理 K 线、冷却时间、live status、Telegram 临时状态 | `redis_data` |
| `quant_core` | `vntdr live` | 增量同步、监控、风险门禁、通知；启动时可迁移数据库 | `.vntrader`、`reports`、`config_data` |
| `webapp` | `vntdr gradio` | 研究工作流、监控管理、策略平台、设置和 ETF 面板 | 共享 `config_data` |
| `etf_ingest` | `vntdr etf-flow-scheduler` | 工作日 ETF 资金流采集与失败重试 | PostgreSQL |

## 3. 目录与文档地图

| 路径 | 模块职责 | 详细文档 |
|---|---|---|
| `src/vntdr/` | 公共模型、配置、清洗、CLI、Gradio 组装 | [`src/vntdr/AGENTS.md`](src/vntdr/AGENTS.md) |
| `src/vntdr/adapters/` | OKX 订单/状态、Telegram 通知与机器人 | [`src/vntdr/adapters/AGENTS.md`](src/vntdr/adapters/AGENTS.md) |
| `src/vntdr/factors/` | OHLCV 因子协议、因子实现和资产包 | [`src/vntdr/factors/AGENTS.md`](src/vntdr/factors/AGENTS.md) |
| `src/vntdr/services/` | 数据同步、研究、策略生命周期、组合、风控、监控和外部数据 | [`src/vntdr/services/AGENTS.md`](src/vntdr/services/AGENTS.md) |
| `src/vntdr/storage/` | SQLAlchemy ORM、数据库会话和仓储边界 | [`src/vntdr/storage/AGENTS.md`](src/vntdr/storage/AGENTS.md) |
| `src/vntdr/strategies/` | 策略插件、指标、注册表和内置策略 | [`src/vntdr/strategies/AGENTS.md`](src/vntdr/strategies/AGENTS.md) |
| `migrations/` | Alembic 环境和数据库版本演进 | [`migrations/AGENTS.md`](migrations/AGENTS.md)、[`migrations/versions/AGENTS.md`](migrations/versions/AGENTS.md) |
| `tests/` | 单元、集成和外部边界验证 | [`tests/AGENTS.md`](tests/AGENTS.md) |
| `.github/` | GHCR Docker 构建发布工作流 | [`.github/AGENTS.md`](.github/AGENTS.md) |

根目录关键文件：

| 文件 | 真实职责 |
|---|---|
| `README.md` | 使用导览、ETF 资金流、TradingView 研究边界、影子审批和当前通知模式说明 |
| `pyproject.toml` / `uv.lock` | Python 版本、依赖、`vntdr` CLI 入口、pytest/ruff 配置和锁定依赖 |
| `docker-compose.yml` | `db`、`cache`、`quant_core`、`etf_ingest`、`webapp` 五服务及卷、健康检查、网络 |
| `Dockerfile` | Python 3.12 镜像、frozen `uv` 安装、VeighNa extra、源码和迁移打包 |
| `docker-entrypoint.sh` | 在 `VNTDR_RUN_MIGRATIONS=true` 时先执行 Alembic，再 `exec` 容器命令 |
| `stack.env.example` | Compose 环境变量模板；只可作为模板，真实 `stack.env` 不得提交 |
| `.github/workflows/publish-ghcr.yml` | GHCR 多架构镜像构建/发布；当前不是 pytest/ruff 门禁流水线 |

## 4. 端到端业务流程

### 4.1 配置、启动与依赖检查

1. `Settings.from_env()` 从 `.env` 与进程环境读取 OKX、PostgreSQL、Redis、Telegram、研究和风控设置，并用 `SecretStr` 保存密钥。
2. `ConfigService` 以 `~/.vntdr/config_override.json` 为覆盖层，把 Gradio 设置页和已注册控制路径的修改应用到共享 `Settings` 对象；`vntdr live` 每轮重新加载，`CommandContext.refresh_runtime_config()` 在 OKX 连接配置变化时重建客户端。
3. `CommandContext` 创建 `Database`、各 Repository、`HistorySyncService`、`ResearchService`、`MonitoringService`、策略运行时/治理、组合运行时和 Telegram 研究服务。
4. `doctor` 检查数据库、Redis 和 VeighNa 依赖。Compose 的 `quant_core` 可由入口脚本在 `VNTDR_RUN_MIGRATIONS=true` 时先执行 `alembic upgrade head`。

### 4.2 行情与因子数据链路

```text
OKX public candles ─┐
TradingView proxy ───┼─▶ HistoryClient/Provider
                   │    └─▶ clean_bars
                   │         ├─ UTC/Interval 规范化
                   │         ├─ 去重
                   │         └─ 可选补齐缺口（合成 K 线）
                   └──────▶ MarketDataRepository → bars

FRED/CFTC/OKX derivatives ─▶ FactorObservation
                           └▶ available_at 点时约束
                           └▶ StrategyRepository → factor_observations

AkShare ETF flow ─▶ normalize/summary ─▶ EtfMoneyFlowRepository
                                      ├─ etf_money_flow_daily
                                      └─ etf_flow_ingestion_runs
```

- OKX 行情走公共市场接口，demo header 不应污染公共行情；小时周期转换为 OKX 要求的大写形式。
- TradingView 适配器是非官方 WebSocket 研究代理，输出符号必须以 `TV:` 开头、交易所固定为 `TRADINGVIEW`，不能与可执行 OKX 行情混合。
- 所有决策时间都应使用已关闭 K 线；`MarketDataContext` 同时按 `observed_at` 和 `available_at` 过滤因子，避免未发布数据泄漏。
- ETF 采集默认在每批开始前刷新当前总市值≥100亿元的动态 ETF 池（显式 `VNTDR_ETF_WATCHLIST` 可覆盖）；`available_at` 按上海时区交易日 16:10 建模，单只 ETF 失败不会阻断其他标的；批次被标记为 `retryable` 时由 APScheduler 任务级指数退避重试。
- ETF 日数据同时保存可空的开高低、收盘、成交量、成交额和换手率；生产 Provider 在 AkShare 日线接口断连时使用公共 Sina 日线 fallback。Gradio 默认按最近 60 个交易日叠加 K 线、资金流入/流出；未筛选时展示动态池每只 ETF 最近候选，选择单只 ETF 后下拉框自动刷新 K 线、资金流和该标的候选列表，并用收盘后可得的 3 日流入、MA5 和量比生成买入以及已有多头持仓的卖出/减仓观察候选、参考价和观察区间，候选不是下单信号；旧资金流行若未补齐 OHLCV 仍可显示资金流图。采集任务可以写入更长的自然日窗口，面板只按交易日裁剪。
- ETF 多因子研究使用 `scikit-learn` 的标准化 Logistic 回归，对资金流、价格动量、均线偏离、波动率、量比和横截面排名做扩展窗口走查；标签从 T 日收盘后的 T+1 开盘进入，默认持有 3 个交易日，并扣除往返成本。模型评分、样本外事件收益和特征系数只用于研究，不生成订单。

### 4.3 研究、验证与版本审批

1. `ResearchService.backtest()` 从数据库读取区间行情，按策略信号逐 K 线执行，使用手续费、滑点、价差和资金费成本模型，输出收益、夏普、回撤、交易数、胜率、盈亏比等指标，并写入 `reports/*.md/json` 与 `research_runs`。
2. `optimize()` 支持 grid、heuristic 和 genetic search；`optimize_target` 可按 Sharpe 或 total return 排序。优化结果是研究候选，不是自动激活授权。
3. `walk_forward()` 在训练窗口寻优，将训练历史保留为指标 warm-up，只统计测试窗口的样本外表现，按折写入 `walk_forward_folds` 并拼接样本外权益。
4. `factor_ablation()` 在相同 K 线和固定变体上比较，不重新拟合参数；`validate-strategy` 组合回测和走查，要求最少交易数、折数、正样本外收益和回撤门槛。
5. `strategy-create` 创建 `StrategyInstance` 与不可变 `StrategyVersion`；`shadow-start/record-equity/finish` 记录通知模式的权益观察；`strategy-approve` 还会核验回测/走查运行 ID、版本/实例绑定、影子状态和回撤，再写入 `strategy_activations`；`strategy-rollback` 写入带 `rollback_of` 的新激活记录。

### 4.4 监控、通知、风控与订单边界

每个监控目标/策略实例的一次处理顺序为：

1. 增量同步行情，读取 Redis 中的上次信号、已处理闭 K 时间、持仓起始时间和冷却截止时间；
2. 只保留已经收盘的 K 线；若 Redis 没有状态，尝试通过订单适配器对账现有多/空仓；
3. 使用固定参数、默认参数或在已关闭 K 线上运行参数寻优；调用策略得到 `1=多`、`-1=空`、`0=空仓`；
4. 执行数据健康门禁（最少 K 线、缺口、陈旧数据），坏数据不能增加敞口，但允许退出既有仓位；
5. 应用最小持仓 K 数和 cooldown，防止同一方向噪音反复开平；
6. `RiskManager` 校验允许交易对、最大下单量、最大回撤和是否允许开仓；由信号变化生成开平多/空 `OrderInstruction`；
7. 非首次状态变化发送 Telegram HTML 通知，并把状态/心跳写入 Redis；首次 bootstrap 不通知、不生成动作；
8. 当前版本不调用 `order_executor.execute()`，动作只用于可观测性，故不能宣称已自动买入/卖出。若将来开放下单，必须保留闭 K、风控、对账和 demo/live 明确隔离。

组合模式由 `PortfolioRuntimeService` 遍历启用实例，收集各实例决策，再由 `PortfolioAllocator` 执行策略/资产类别/品种/波动率/相关性簇/总敞口上限，输出组合目标权重；该输出目前仍受通知模式边界约束。

### 4.5 用户入口

- Gradio「策略研究工作流」：行情同步、回测、参数寻优、样本外走查、K 线/信号图和交易记录；「监控部署与管理」维护 `monitored_targets`。
- Gradio「实盘监控看板」：读取 Redis live status、OKX 账户权益/持仓和最近日志；「ETF 资金流」只读数据库或显式触发一次有界采集；「策略平台」展示实例、版本、影子运行和数据健康；「系统设置」修改共享覆盖配置。
- Telegram 当前实际注册的命令主要为 `/start`、`/status`；代码仍保留 `/rank`、`/run`、`/auto`、`/stop` 和旧 `/config` 处理函数，但简化版 `build_application()` 未注册这些旧命令，变更时必须以注册表和集成测试为准。
- CLI 入口为 `vntdr`，研究/同步/治理命令见 [`src/vntdr/AGENTS.md`](src/vntdr/AGENTS.md)。

## 5. 运行方式与入口

### 本地开发

使用 Python 3.12 和 `uv` 安装锁定依赖；需要 VeighNa 相关命令时使用项目定义的 `veighna` extra。常用入口如下：

```bash
uv sync --extra veighna --group dev
uv run vntdr doctor
uv run vntdr gradio --port 7860
uv run vntdr live
uv run vntdr telegram-bot
```

研究/数据/治理入口包括 `sync-history`、`sync-tradingview`、`sync-okx-derivatives`、`akshare-csi300-flow`、`etf-universe-scan`、`etf-flow-ingest`、`etf-flow-scheduler`、`etf-factor-research`、`backtest`、`optimize`、`walk-forward`、`ablate-strategy`、`validate-strategy`、`research-runs`、`strategy-create`、`strategy-approve`、`strategy-rollback` 以及 `shadow-*`；完整参数以 `uv run vntdr --help` 和当前 `src/vntdr/cli.py` 为准。

### Docker Compose

```bash
cp stack.env.example stack.env    # 仅创建本地文件；填值后不得提交
docker compose --env-file stack.env up -d
docker compose --env-file stack.env logs -f quant_core
docker compose --env-file stack.env exec quant_core vntdr doctor
```

`quant_core` 执行 `vntdr live`，`webapp` 执行 Gradio，`etf_ingest` 执行 ETF 调度器；数据库迁移由 `quant_core` 的 `VNTDR_RUN_MIGRATIONS=true` 触发。入口会兼容旧版已由 `create_schema()` 建成完整表但没有 Alembic 状态的数据库：仅在验证迁移表完整时 stamp 到 `20260730_03` 再升级（当前 head 为 `20260803_04`），其他迁移失败仍阻止启动。Compose 本身不提供 nginx、TLS、域名或认证层。`vntdr live` 若没有配置 `monitored_targets`（或一次性目标参数），会启动但没有监控目标；不要把“进程存活”当作“正在交易”。

## 6. 配置、数据和环境隔离

- 环境变量模板见 `stack.env.example`；禁止把真实 OKX、Telegram、数据库密码写入 Git、文档、测试输出或报告。
- 关键配置分组为：`OKX_API_KEY`/`OKX_SECRET_KEY`/`OKX_PASSPHRASE` 与 `OKX_DEMO_TRADING`；`VNTDR_DATABASE_URL` 或 `PG_*`；`REDIS_*`；`TG_BOT_TOKEN`/`TG_CHAT_ID`；`VNTDR_DEFAULT_SYMBOL`、`VNTDR_DEFAULT_INTERVAL`、`VNTDR_DEFAULT_STRATEGY`、`VNTDR_EXECUTION_MODE` 及研究/风控参数。`research.monitored_targets` 是配置覆盖文件中的 JSON 列表，不是 `stack.env.example` 中的环境变量。变量名可写入文档，凭据值只能通过受控运行环境注入。
- PostgreSQL 是持久化业务数据和研究证据的主库；Redis 是运行时状态/缓存，不是研究事实源；`reports/` 保存 Markdown/JSON 研究产物；`config_data` 在 `webapp` 与 `quant_core` 间共享覆盖文件。
- `bars` 同时使用 `symbol + exchange + interval + datetime` 语义；代理数据必须按交易所过滤，不能因为符号相似而混用。
- 生产数据库演进使用 Alembic；`Database.create_schema()` 适合测试/本地初始化，不能替代版本迁移审计。
- Docker 镜像使用 `uv.lock` frozen 安装，构建包含 VeighNa extra 和 dev 依赖；`plotly` 需要保持 `>=6`。

## 7. 测试与验收

```bash
uv run pytest
uv run pytest tests/unit
uv run pytest tests/integration
uv run vntdr doctor
```

验收应覆盖：数据清洗/点时可用性、研究成本与样本外走查、策略版本/影子门禁、Redis 信号幂等、Telegram 通知、风控拦截、OKX 适配器错误重试、Compose/迁移和 Web UI 辅助函数。真实交易验收必须使用 OKX demo，且先确认当前 live 路径没有被意外改成可下单。

## 8. 强制文档同步规则

1. 任何代码、配置、数据库迁移、部署、CI、测试或运行时行为变更，都必须在同一提交同步检查并更新受影响目录的 `AGENTS.md`。
2. 若变更影响模块边界、数据流、业务流程、交易安全、配置契约或部署拓扑，必须同时更新本根目录 `AGENTS.md`；仅局部实现变化至少更新最近的模块文档。
3. 新增模块/子目录必须新增对应 `AGENTS.md`，并在本文件的目录地图登记；删除或重命名模块时同步删除/迁移文档链接。
4. 文档必须使用中文、引用实际路径和真实入口；不得复制密钥、token、密码或生产数据。文档变更不等于代码变更，不要为了“满足文档同步”而修改无关实现。
5. 提交前检查 `git diff --stat`、`git diff --check`、文档链接路径、受影响测试和本文档中的当前状态。代码变更若没有同步文档说明，应视为未完成。

## 9. 当前已知限制

- 默认 `notify_only` 且 live 监控抑制订单执行；真实多空下单尚未作为当前发布能力验收。
- 回测质量依赖数据库历史长度和数据完整性；短窗口寻优/高夏普不能替代足够长历史和样本外验证。
- 监控数据健康检查与 Gradio 展示阈值不同：监控默认允许最少 1 根 K 线，而 UI 的数据健康表默认要求 50 根；修改阈值时必须同时评估两条路径。
- `RiskSettings` 中的部分组合资金/敞口字段尚未完整传入 `PortfolioAllocator`；不能把配置字段的存在误写成已经生效的组合风控。
- `OkxOrderExecutor` 具备重试和账户查询，但当前没有完整的幂等键/成交确认闭环；即使未来解除通知模式，也必须补齐订单状态审计和故障恢复。
- 外部 AkShare、TradingView、OKX 公共接口均可能限流或变更字段/协议，适配器只应把研究结果写入隔离边界。
- ETF 多因子模型目前只有动态市值池的短历史窗口；样本外事件数量和历史长度不足时只能作为研究候选，不能据此宣称盈利能力或自动启用。
- `publish-ghcr.yml` 目前只负责镜像发布，不会自动运行完整测试；发布前仍需本地或单独 CI 运行测试分层。
- 旧文档关于 `SimulatedOrderExecutor` 不接受 `symbol` 参数的描述已过时；当前接口支持可选 `symbol`，启动对账不应再以该 TypeError 作为已知问题。
- Docker Compose 没有仓库内 nginx/TLS 配置；外部反代、证书和域名需要独立运维文档。
