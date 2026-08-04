# `src/vntdr` 直属模块 Wiki

本文只描述 `src/vntdr` 目录的直属模块：`__init__.py`、`cleaning.py`、`cli.py`、`config.py`、`models.py`、`webapp.py`。`adapters/`、`services/`、`strategies/`、`storage/` 下的文件只在解释直属入口的实际调用边界时引用；它们各自的文档仍由各自目录负责。

## 阅读边界与事实口径

- 包的可执行入口由 `pyproject.toml` 的 `vntdr = "vntdr.cli:run"` 定义；CLI 命令以当前 `cli.py` 注册的 Typer 命令为准。
- 研究报告、监控状态、配置覆盖和订单适配器是不同层次：有订单适配器不等于当前运行时会下单。
- 本目录没有在模块层面直接实现策略公式、数据库 ORM、OKX 下单协议或 Telegram Bot 命令；这些行为由导入的子模块承担。
- 配置值、令牌、口令和密钥不应写入本 Wiki、日志、提交或截图；本文只记录变量名、字段名和安全要求。

## 直属入口总览

```text
pyproject: vntdr = vntdr.cli:run
                 │
                 ▼
        cli.run() → Typer app
          │                 │
          │                 └─ gradio → webapp.main()
          │                                  │
          │                                  ├─ ConfigService / config_override.json
          │                                  ├─ HistorySyncService → OKX/TradingView → clean_bars
          │                                  ├─ ResearchService → strategies + factors → ResearchReport
          │                                  ├─ ETF services → AkShare → ETF repositories
          │                                  └─ Redis/OKX 只读状态 → 监控看板
          │
          ├─ doctor / sync-* / ETF-* / research-*
          │       └─ Settings → Database → repositories ↔ PostgreSQL
          │
          ├─ live / portfolio-run
          │       └─ MonitoringService
          │            ├─ MarketDataRepository ← PostgreSQL
          │            ├─ ResearchService → strategies/registry.py → strategies/*
          │            ├─ RiskManager → 过滤开仓指令
          │            ├─ RedisSignalStore ↔ Redis
          │            └─ TelegramNotifier → Telegram
          │
          └─ strategy/shadow/validation 命令
                  └─ StrategyRepository → StrategyVersion/Instance/Activation/ShadowRun

研究数据落库：
  外部行情 → HistoryClient → HistorySyncService → clean_bars
           → BarRecord/CleanBarsResult → MarketDataRepository → PostgreSQL

研究执行：
  ResearchJobConfig → ResearchService
       → MarketDataRepository + auxiliary bars/factors
       → strategy module → BacktestOutcome
       → ResearchReport → reports/*.md/*.json + ResearchRunRepository

当前监控执行：
  完成 K 线 → latest_signal → 数据质量/持仓/冷却/风控
       → MonitorResult + Redis 状态 + Telegram 通知
       → 订单指令只被记录/展示，MonitoringService 当前不会调用 order_executor.execute()
```

## 直属文件说明

### `__init__.py`

职责很小：导入包时调用 `logging.basicConfig`，设置 INFO 级别及统一的时间、logger 名称、级别和消息格式，并导出模块级 `logger`。没有从包根导出业务类，也没有注册 CLI、数据库或策略；需要使用具体能力时应直接导入对应直属模块。

依赖只有标准库 `logging`。由于它在导入时配置基础日志，嵌入已有应用或测试进程时应注意宿主进程是否已经配置 logging；不要把业务初始化放进这里。

### `cleaning.py`

这是行情 payload 到规范化 K 线的纯数据清洗边界，不访问网络、数据库或交易接口。

#### 常量与函数

- `INTERVAL_TO_DELTA` 只为 `1m`、`3m`、`5m`、`15m`、`30m`、`1h`、`4h`、`1d` 提供时间间隔。这里是清洗/监控实际支持的集合，不等同于 `models.Interval` 的语法集合。
- `clean_bars(raw_bars, interval, fill_missing=False, calendar="continuous") -> CleanBarsResult`：
  - 输入是字典列表；每项的 `datetime` 通过 `dateutil.parser.isoparse` 解析，再由 `BarRecord` 校验并统一为 UTC。
  - 以 `BarRecord.key = (symbol, exchange, interval, datetime)` 去重；同一 key 重复时后出现的记录覆盖前一条，`duplicates_removed` 加一。
  - 按 `datetime` 升序排序。
  - `calendar="continuous"` 将每个时间间隔都视为应有 K 线；`calendar="weekday"` 跳过周末应闭市的缺口。非法日历值会抛 `ValueError`。
  - `fill_missing=True` 时，缺口中每根合成 K 线以此前收盘价填充 OHLC，成交量为 `0.0`，`is_synthetic=True`；同时返回检测和填充数量。
  - 空输入直接返回空 `CleanBarsResult`；非空输入使用不在 `INTERVAL_TO_DELTA` 中的周期会抛 `ValueError("Unsupported interval: ...")`。
- `_is_open_gap(previous, current, delta, calendar)` 是缺口判定辅助函数：连续市场总判定为开放缺口；工作日市场对日内周期只判断同一日期，对日及以上周期逐个检查中间的工作日。

输出只包含 `CleanBarsResult`，不会自动持久化。`services/history.py` 的 `HistorySyncService.sync` 负责调用它，再把结果交给 `MarketDataRepository`。

### `config.py`

该文件把环境/`.env` 的字符串映射为 Pydantic 配置对象，并提供少量派生属性和按命令的最低配置检查。它不读取 `~/.vntdr/config_override.json`；运行时覆盖由 `services.config_service.ConfigService` 另行加载。

#### 配置模型

| 类型 | 关键字段与行为 |
|---|---|
| `ConfigurationError` | 配置缺失或不完整时使用的 `ValueError` 子类。 |
| `OkxSettings` | OKX REST 地址、Demo 标志、保证金模式、订单类型、重试次数/等待时间及 `api_key`、`secret_key`、`passphrase`。密钥字段使用 `SecretStr`；`trading_enabled` 只有三项 OKX 凭据同时存在时才为真。 |
| `DatabaseSettings` | `host/port/username/password/database_name/url`；`dsn` 优先返回 `url`，否则要求用户名、密码、数据库名完整，并组装 `postgresql+psycopg` DSN。 |
| `RedisSettings` | `host/port/db`，`url` 组装为 Redis URL。 |
| `TelegramSettings` | `bot_token` 和 `chat_id`；缺失时通知能力由上层决定是否降级。 |
| `ResearchSettings` | 报告目录、同步重试/批量参数、默认策略/标的/周期、监控回看 K 线数、订单量、回测回看小时数、Maker/Taker 费率、滑点/价差/资金费率、寻优目标、`execution_mode`、策略级参数、启用策略和 `monitored_targets`。执行模式的模型值限定为 `notify_only`、`paper`、`live`。 |
| `RiskSettings` | 单策略资金、总敞口、最大回撤、允许标的、最大下单量和是否允许开仓；数值范围由 `Field` 做基础约束。 |
| `Settings` | 聚合以上六个分区：`okx`、`database`、`redis`、`telegram`、`research`、`risk`。 |

#### 加载、转换和校验

- `Settings.from_env()` 先读取当前工作目录的 `.env`，再用 `os.environ` 覆盖同名项；最后调用 `from_mapping`。
- `Settings.from_mapping(mapping)` 负责字符串到整数、浮点数、布尔值、`Path` 和 `SecretStr` 的转换。布尔值 `_to_bool` 识别 `1/true/yes/on`（不区分大小写）。
- 使用的非敏感环境变量包括：`PG_HOST`、`PG_PORT`、`PG_USER`、`PG_DB_NAME`、`REDIS_HOST`、`REDIS_PORT`、`REDIS_DB`、`VNTDR_REPORT_DIR`、`VNTDR_SYNC_RETRY_COUNT`、`VNTDR_SYNC_BATCH_LIMIT`、`VNTDR_DEFAULT_WARMUP_DAYS`、`VNTDR_DEFAULT_STRATEGY`、`VNTDR_DEFAULT_SYMBOL`、`VNTDR_DEFAULT_INTERVAL`、`VNTDR_MONITOR_LOOKBACK_BARS`、`VNTDR_DEFAULT_ORDER_SIZE`、`VNTDR_DEFAULT_RANK_LOOKBACK_HOURS`、`VNTDR_MAKER_FEE_RATE`、`VNTDR_TAKER_FEE_RATE`、`VNTDR_USE_MAKER_FEE`、`VNTDR_SLIPPAGE_BPS`、`VNTDR_SPREAD_BPS`、`VNTDR_FUNDING_RATE_PER_BAR`、`VNTDR_OPTIMIZE_TARGET`、`VNTDR_EXECUTION_MODE`、`VNTDR_MAX_STRATEGY_CAPITAL`、`VNTDR_MAX_TOTAL_EXPOSURE`、`VNTDR_MAX_DRAWDOWN`、`VNTDR_ALLOWED_SYMBOLS`、`VNTDR_MAX_ORDER_SIZE`、`VNTDR_ALLOW_OPENING_TRADES`、`OKX_REST_BASE_URL`、`OKX_DEMO_TRADING`、`OKX_MARGIN_MODE`、`OKX_ORDER_TYPE`、`OKX_ORDER_RETRY_COUNT`、`OKX_ORDER_RETRY_WAIT`。
- 敏感或应按敏感处理的变量名为：`OKX_API_KEY`、`OKX_SECRET_KEY`、`OKX_PASSPHRASE`、`PG_PASSWORD`、`VNTDR_DATABASE_URL`、`TG_BOT_TOKEN`、`TG_CHAT_ID`、`TRADINGVIEW_AUTH_TOKEN`。它们只能通过受控 secret store、进程环境或权限受限的本地文件注入；不要写入 Wiki、提交、普通日志或聊天记录。`VNTDR_DATABASE_URL` 可能内含数据库密码，应按密钥处理。
- `Settings.validate_for(command_name)`：`doctor`、行情同步、研究和 ETF 入库/调度命令检查数据库配置；`live` 还检查数据库并在 Telegram 凭据缺失时只记录 warning。所有命令都会确保 `research.report_dir` 存在。
- `validate_for` 不会强制要求 OKX 凭据，也不会验证 Demo/实盘选择；没有完整 OKX 凭据时 `cli.CommandContext` 会选择模拟订单执行器。不要把“配置通过”理解为“已授权真实交易”。

#### 运行时覆盖

`ConfigService` 的实际规则如下，供 CLI、Gradio 和 Telegram 共用：

- 默认文件是 `Path.home() / ".vntdr" / "config_override.json"`；Docker 中通常由共享 volume 提供。
- `_load_overrides()` 读取 JSON；文件不可读、JSON 无效或顶层不是对象时按空覆盖处理，然后在同一个 `Settings` 对象上恢复启动基线并重新应用覆盖。
- `set(key, value)` 支持一层或两层字段名，例如 `research.*`、`risk.*`、`okx.*`；按当前值类型做基础转换后写回 JSON。未知键或转换失败返回 `False`。它不会重新运行完整的 Pydantic 模型校验，因此新增设置键时必须补测试和验证。
- `reset` 删除一个覆盖；`reset_all` 清空全部覆盖并恢复启动/环境值。
- `cli.live` 每轮读取覆盖；`CommandContext.refresh_runtime_config()` 检测 OKX 配置签名变化后重建订单和历史客户端。Gradio/Telegram 的设置操作直接修改共享 `Settings` 并持久化；配置文件权限和备份必须按含密钥文件处理。

### `models.py`

这是跨层 DTO/领域数据模型集合；没有数据库写入和网络请求。所有模型均为 Pydantic `BaseModel`，时间字段通常通过 `_ensure_utc` 转为带 UTC 时区的 datetime。

#### 市场数据模型

- `_ensure_utc(value)`：无时区时间按 UTC 解释；有时区时间转换到 UTC。
- `BarRecord`：`symbol/exchange/interval/datetime/open/high/low/close/volume/is_synthetic`；默认交易所为 `OKX`、成交量为零、非合成。`key` 是去重用四元组。
- `Instrument`：独立于交易所符号语法的标的描述，含 `asset_class`（crypto/commodity/equity/fx/index）、`calendar`（continuous/weekday）和可选计价币；symbol/exchange 会去空格并转大写。
- `Interval`：规范化周期；别名 `d/day/h/hour/min` 会被转换，格式必须匹配数字加 `m/h/d/w`，`seconds` 返回秒数。
- `DataQualityReport`：检查周期、时间、K 线数、缺口、过期、可用性和原因。
- `CleanBarsResult`、`SyncResult`：分别承接清洗统计和同步任务/入库统计。

#### 策略、版本和风控模型

- `StrategyVersion`：由治理流程按不可变版本快照使用的策略代码/参数记录，含 UUID、父版本、代码版本和因子配置；模型本身未设置 `frozen`，`clone()` 产生带 `parent_id` 的新版本。
- `StrategyInstance`：策略实例名、`Instrument`、主周期、辅助周期、执行模式和启用标志。
- `StrategyActivation`：实例与版本的生效关系、时间、审批人和回滚来源。
- `ValidationGate`：回测、走查、影子三个布尔门；`approved` 只有三者全真才为真。
- `ShadowRun`：通知-only 观察期的起止/权益/回撤/观测数/状态（active/passed/failed）。
- `FactorObservation`：因子观测值、观测时间、可用时间、周期和 metadata；禁止 `available_at < observed_at`，用于避免前视。
- `StrategyDecision`：实例对标的的 `signal`（-1 到 1）、confidence、时间和原因。
- `PortfolioDecision`、`PortfolioRunResult`：目标权重、总/净敞口、缩放原因、逐实例决策和实例错误。
- `PositionSizingDecision`：单位数、名义金额、风险预算、止损距离和是否封顶。

#### 交易、监控和研究模型

- `OrderInstruction`：symbol、动作、数量和原因；它只是订单意图 DTO，不代表已经发单。
- `TradeRecord`：事件回测中含成本的完整多/空交易，记录进出场时间/价格、毛/净收益、持仓根数、交易费和资金费。
- `MonitorResult`：标的、周期、策略、当前/上一信号、使用参数、动作、通知是否发送、错误和可选版本 ID。
- `HealthCheckResult.lines()`：将命名检查转换为 CLI 输出行；`ok` 是所有检查结果的合取。
- `FoldResult`：走查折号、训练/测试时间、折内指标和参数。
- `ResearchJobConfig`：研究输入；含策略、标的、交易所、主/辅助周期、起止时间、模式（backtest/optimize/walk-forward）、方法、固定参数、参数空间、训练/测试窗口和寻优目标。校验 `start < end`；寻优模式需要非空参数空间；走查需要正的训练/测试窗口。`report_slug` 将 `walk-forward` 映射为 `walk_forward`。
- `ResearchReport`：策略、数据集、模式、指标、最优参数、折结果和 Top 结果；`to_markdown()` 和 `to_json()` 只负责序列化。
- `ResearchValidationResult`：回测报告、走查报告、通过标志、原因和门槛；`AblationResult` 承接同一数据上的显式变体结果。
- `aggregate_metrics(metric_rows)`：空输入返回零指标；否则对首行键集合逐键求均值，不做加权或重新计算组合收益。

模型层的校验错误通常是 `ValueError`/Pydantic `ValidationError`。它们不负责把信号转换成 OKX 动作，也不负责检查数据库中是否存在对应实例或版本。

### `cli.py`

`cli.py` 是所有命令行入口和运行时装配层。模块级 `app = typer.Typer(...)` 注册命令，`run()` 只调用 `app()`。

#### `CommandContext`

`CommandContext(settings)` 完成一次进程内装配：

1. 用 `settings.database.dsn` 创建 `Database` 并 `create_schema()`；创建 `MarketDataRepository`、`ResearchRunRepository`、`StrategyRepository`。
2. 创建 `HistorySyncService` + `OkxHistoryClient`，公共行情客户端使用普通 OKX 公共市场接口；Demo 标志用于客户端配置，不代表公共行情一定需要密钥。
3. 创建 `ResearchService`，并把策略仓库作为因子仓库传入。
4. 创建 Redis 客户端、`TelegramNotifier`、订单执行器、`RedisSignalStore`、`RiskManager` 和 `MonitoringService`。
5. 创建 `StrategyRuntimeService`、`StrategyGovernanceService`、`PortfolioRuntimeService` 和 `TelegramResearchService`。

`_build_order_executor()` 在三个 OKX 凭据不完整时返回 `SimulatedOrderExecutor`，完整时返回带 Demo、保证金、订单类型和重试设置的 `OkxOrderExecutor`。`refresh_runtime_config()` 在锁内重新载入覆盖，只在 OKX 配置签名变化时重建 OKX 历史/订单客户端。

上下文方法 `doctor`、`sync_history`、`backtest`、`optimize`、`walk_forward`、`validate_candidate`、`factor_ablation`、`monitor_once`、`monitor_once_async`、`run_portfolio_once` 是直属 CLI 与服务层之间的薄包装。策略实例/版本方法负责先加载策略以拒绝拼写错误，再持久化实例和版本；审批/回滚交给 `StrategyGovernanceService`。

`doctor()` 分别 ping 数据库、Redis，并导入 `vnpy`、`vnpy_ctastrategy`、`vnpy_okx`、`vnpy_postgresql`、`vnpy_riskmanager`；结果封装为 `HealthCheckResult`。

关键辅助函数：`_resolve_gradio_port` 解析显式端口/`GRADIO_PORT`；`_build_research_config` 将 CLI 文本参数组装成 `ResearchJobConfig`；`_build_etf_flow_ingestion_service` 组装 AkShare、ETF 仓库和可选市值 universe resolver；`sync_target_market_data` 为 live 目标做增量同步，失败时记录 warning 并继续使用本地数据。

#### 命令索引

所有命令都先通过 `Settings.from_env()`；需要数据库的命令还会调用 `validate_for`。日期参数由 `datetime.fromisoformat` 或 `date.fromisoformat` 解析，研究参数最终进入 `ResearchJobConfig`。

| 命令 | 作用、输入和输出边界 |
|---|---|
| `strategy-create` | 用 `--name/--strategy/--symbol/--interval` 创建策略实例和待审批版本；可给 `--exchange`、`--asset-class`、重复的 `--aux-interval`、`--execution-mode`。默认意图是通知-only。输出实例和版本 UUID。 |
| `strategy-approve` | 校验两个已完成且同一数据集的研究运行，以及同实例/版本的 `passed` 影子运行；回测至少 10 笔交易、走查至少 3 折、样本外收益为正且回撤不越过 10% 门槛后才激活。需要 `--backtest-run-id`、`--walk-forward-run-id` 和 `--shadow-run-id`。 |
| `strategy-rollback` | 将实例重新激活到已有版本，并记录 `rollback_of`；要求当前存在不同的生效版本。 |
| `shadow-start` | 为实例/版本创建可审计通知-only 影子运行。 |
| `shadow-record-equity` | 给 active 影子运行追加标记权益观测，输出观测数和最大回撤。 |
| `shadow-finish` | 以 `passed` 或 `failed` 结束影子运行；通过至少需要 28 天、至少一条观测且回撤不超过 10%。 |
| `portfolio-run` | 运行所有启用且有生效版本的实例，汇总目标权重、总/净敞口、缩放原因和实例错误；当前输出是通知-only 目标组合。 |
| `doctor` | 检查数据库、Redis 和 VeighNa 可选依赖；失败以非零退出码结束。初始化异常会被转成失败检查。 |
| `sync-history` | 从 OKX 公共 K 线同步 `--symbol/--interval/--start/--end`，可选 `--fill-missing` 和 `--calendar continuous|weekday`；经清洗后写 PostgreSQL，并输出同步任务、入库、清洗和去重统计。 |
| `sync-tradingview` | 通过非官方 TradingView WebSocket 研究代理同步；`--tv-symbol` 为来源标识，`--output-symbol` 必须以 `TV:` 开头，默认按工作日市场清洗；会读取 `TRADINGVIEW_AUTH_TOKEN`，输出与 OKX 隔离。 |
| `sync-okx-derivatives` | 同步 OKX 公开 funding/open-interest 因子观测到策略仓库，供研究使用，不是下单入口。 |
| `akshare-csi300-flow` | 采集单只 A 股或沪深 300 成分的资金流，输出日频 CSV、股票汇总 CSV 和 JSON 摘要到报告目录；远端错误退出码为 2。 |
| `etf-universe-scan` | 调用 AkShare 当前 ETF 总市值快照，按 `--min-market-cap` 和可选 `--max-symbols` 筛选，写 `etf_universe_market_cap.csv`。这是 CLI 已注册的“当前快照”命令，不是历史市值或回测事实。 |
| `etf-flow-ingest` | 对 ETF 资金流做一次有界采集并幂等写入 PostgreSQL；未传参数且未设置 `VNTDR_ETF_WATCHLIST` 时默认动态筛选总市值≥100亿元的 ETF，输出 JSON 摘要，`retryable` 时非零退出。 |
| `etf-flow-scheduler` | 用 APScheduler 按时区、小时和分钟常驻采集；默认每批先刷新总市值≥100亿元的动态 ETF 池，`--run-once` 只跑一次。任务级重试与单请求重试是两层机制。 |
| `etf-factor-research` | 读取 ETF 日频事实表，使用资金流/价格/量价因子进行 sklearn 扩展窗口走查；输出最新评分、折指标、事件收益和特征系数 CSV/JSON，不写订单。 |
| `backtest` | 按策略、标的、交易所、周期、时间窗和可选辅助周期运行单次研究，固定使用策略默认参数/覆盖参数，输出 `ResearchReport.to_markdown()`。 |
| `optimize` | 使用策略默认参数空间和 `--method`（例如 grid/ga/heuristic）寻优，按配置/任务的目标排序，输出报告并持久化研究运行。 |
| `walk-forward` | 需要 `--train-window`、`--test-window`；每折只用训练数据选择参数，在测试段执行，持久化折结果和样本外报告。 |
| `ablate-strategy` | 用重复的 `--variant NAME={...}` 在同一数据上显式覆盖参数，不重新寻优；输出各变体收益、回撤和交易数。 |
| `research-runs` | 按标的可选过滤，列出持久化研究证据 ID、状态、模式、数据集、收益和回撤，供审批命令引用。 |
| `validate-strategy` | 用固定参数组合同时运行回测和走查；走查目标固定为样本外收益，并应用交易数、折数、收益和回撤门槛；失败以非零退出。 |
| `live` | 启动实时监控；先做依赖检查和持仓信号对账，再对每个监控目标增量同步、只用已完成 K 线监控、更新 Redis 状态并发 Telegram 通知。`--once` 执行一轮后退出；常驻模式带指数退避，且可在后台启动 Telegram Bot。 |
| `gradio` | 读取 `--port`，否则使用 `GRADIO_PORT`，再调用 `webapp.main`；默认端口由 CLI 常量决定。 |
| `telegram-bot` | 前台启动 `TelegramCommandBot`，复用同一 `CommandContext` 的研究服务、监控回调、ConfigService 和 Redis。 |

#### `live` 的真实运行边界

- 目标优先来自 `research.monitored_targets`；没有目标时，只有显式传入 `--strategy/--symbol/--interval` 才生成一个临时目标。
- 启动时为每个目标检查 Redis signal cache；缺缓存时尝试从 OKX 持仓推断多/空/空仓信号。对账失败会记录并从空状态开始监控，不能把它当成持仓已确认。
- 每轮每个目标在独立线程中先 `sync_target_market_data`，再调用 `CommandContext.monitor_once`；已处理的闭合 K 线不会重复触发状态变更。
- `MonitoringService` 会生成 `OrderInstruction` 并经过 `RiskManager` 过滤，但当前代码在执行阶段明确记录“orders intentionally suppressed”，没有调用 `order_executor.execute()`。因此当前 `live`、`portfolio-run` 和 Gradio 监控面板都不能据此声称已真实买入/卖出；OKX 执行器主要用于持仓/权益查询和未来适配。
- Telegram 未配置时 live 只禁用通知并给 warning，不会自动补齐凭据。监控重复异常达到阈值后会发送错误告警，并按上限退避。

#### CLI 错误处理

- 配置、Pydantic、日期和服务层错误通常向 Typer 冒泡；参数格式错误使用 `typer.BadParameter`。
- `doctor` 将上下文初始化异常转为失败健康检查；AkShare 采集/调度命令对数据源异常返回可识别的错误退出码。
- `HistorySyncService` 会先创建同步任务，失败时将任务标记为 `failed` 后重新抛出；研究服务则把成功报告写文件和研究运行仓库。
- live 常驻循环隔离单目标异常；主循环失败时指数退避，成功后重置计数。维护命令时要保留这些“记录错误后继续/重试”的语义，避免吞掉研究证据或交易状态错误。

### `webapp.py`

这是 Gradio 研究/监控工作站，不是独立的研究引擎。它把用户输入解析成 `ResearchJobConfig`，调用服务层，再把报告、图表和数据库/Redis 状态转换为 Gradio 组件输出。

#### 初始化和通用辅助函数

- 依赖 `gradio`、`pandas`、`plotly`、`dateutil`、`zoneinfo`，以及 ConfigService、历史/研究/ETF/数据质量服务、数据库仓库和策略 registry/指标函数。
- `_get_config_service()` 和 `_get_services()` 是模块级懒加载。后者创建数据库 schema、`MarketDataRepository`、`ResearchRunRepository`、`StrategyRepository`、`ResearchService` 和 `HistorySyncService`，并缓存三者；设置页/override 更新不会自动替换全部缓存服务。
- `_parse_datetime` 支持 datetime、ISO 文本和 dateutil 文本；结束时间若只给日期会扩展到当天 23:59:59，最后由 Pydantic 模型统一 UTC。
- `_parse_params` 读取每行 `key=value`，按布尔、整数、浮点、字符串顺序转换。
- `_parse_space_value` 支持逗号列表、`~`/`-`/`to` 范围和 `:step`、`/step`、`step N` 步长；`_parameter_space_from_text` 将多行文本转成优化空间。
- `_start_preview`/`_is_current_preview` 用锁和单调版本号防止较旧的异步图表响应覆盖用户最新选择。
- `STRATEGY_PARAMS` 保留 UI 的参数标签、默认值、推荐空间和边界；随后用 `strategy_configs()` 为 registry 中发现的策略补齐 metadata。`_enabled_strategy_names` 过滤配置中启用的策略，空配置回退到发现到的策略。
- `_auto_fit_parameter_space` 将推荐空间限制在策略边界内，不会自动把整个边界笛卡尔积都加入搜索。
- `_metrics_df`、`_params_df`、`_params_line` 只做表格/中文标签转换；`_ema` 和 `_build_kline_macd_chart` 只做展示计算。
- `_get_target_parameters` 解析目标级参数并兼容旧的策略级覆盖；`_get_targets_df_and_choices` 生成监控目标表和选择器；`_platform_instances_df`、`_shadow_runs_df`、`_data_health_df` 分别读取版本平台、影子运行和数据健康只读视图；ETF 相关的 `_load_etf_panel`/`_ingest_etf_panel` 负责数据库读取和一次有界采集。

#### Gradio 页面

`main(port)` 先读取 override，解析 `GRADIO_PORT`/默认端口和默认研究参数，再构造 `gr.Blocks`，最后以 `server_name="0.0.0.0"`、指定端口启动。

1. **「策略研究工作流」**
   - 选择策略、标的、周期、开始/结束日期和行情来源；OKX 走 `HistorySyncService`，TradingView 需要 `TV:` 输出标的并走研究隔离客户端。
   - 「同步行情数据」只同步并报告 K 线数，不直接运行策略。
   - 「运行策略回测」展示 K 线/信号/策略指标图、指标、使用参数和交易记录。
   - 「参数寻优」支持 Grid、GA、Heuristic，选择 Sharpe 或收益率目标；`trade_mode` 被固定为本次运行参数，不进入搜索维度；结果保留 Top 结果并可填回回测。
   - 「样本外走查测试」接收训练/测试窗口、寻优方法和目标，展示每折参数/指标，并在 UI 中把各折测试段拼成样本外权益曲线和交易记录。
   - 「监控部署与管理」的新增、更新、删除都会写 `research.monitored_targets`；工作流管理支持把每个目标的策略参数保存到目标自身，旧目标没有自身参数时回退到策略级覆盖/内置默认。
2. **「实盘监控看板」**
   - `fetch_live_status` 从 Redis hash `vntdr:live_statuses` 读取每个目标状态，从 list `vntdr:live_logs` 读取最近日志；最新心跳小于 90 秒显示在线，否则显示无响应，没有数据显示离线。
   - 有 OKX 凭据时只查询账户权益和持仓并显示 Demo/Live 标签；没有凭据时显示未配置。该页面没有发单回调。
   - 页面下方可新增/移除监控目标；这组简化控件只写 symbol/interval/strategy/volume，不替工作流编辑器写入完整的目标参数。
3. **「ETF资金流」**
   - 观察池和回看天数由组件输入；观察池留空时读取动态市值池的全部已入库标的，刷新后加载代码和名称选项。「刷新数据库视图」只读 `EtfMoneyFlowRepository`，输出摘要、日频明细、趋势图、买入/卖出观察候选、多因子最新评分和最近采集运行。
   - 「立即采集并入库」只触发一次有界 `EtfFlowIngestionService.run`，失败显示可重试状态，不承担常驻调度。
4. **「策略平台」**
   - 展示实例、当前生效版本、执行模式；展示影子运行的观察天数、权益和回撤；展示启用实例的 `assess_bars` 数据健康结果。
   - 这个页面的 `_data_health_df` 明确以至少 50 根 K 线、缺口和过期状态生成“可用/阻止开仓”视图。
5. **「系统设置」**
   - 可编辑默认策略、启用策略、默认标的/周期/下单量、回看小时数、费率、寻优目标、风控资金/敞口/回撤/最大下单量/开仓开关和 OKX Demo/凭据字段。
   - 这些 UI 字段对应 `CFG_KEYS`：`research.default_strategy`、`research.enabled_strategies`、`research.default_symbol`、`research.default_interval`、`research.default_order_size`、`research.default_rank_lookback_hours`、`research.maker_fee_rate`、`research.taker_fee_rate`、`research.use_maker_fee`、`research.optimize_target`、`risk.max_strategy_capital`、`risk.max_total_exposure`、`risk.max_drawdown`、`risk.max_order_size`、`risk.allow_opening_trades`、`okx.api_key`、`okx.secret_key`、`okx.passphrase`、`okx.demo_trading`。
   - 保存、重新加载、重置分别调用 ConfigService 的 `set`/`_load_overrides`/`reset_all`，并刷新全局研究控件和监控目标选择器。密钥输入框是密码型组件，但加载逻辑会把 `SecretStr` 解包后回填 UI；部署时应限制 Gradio 访问权限，并确保 override 文件不被非授权用户读取。

#### 主要回调与 I/O

- `run_fetch_market_data`：校验时间范围；OKX 使用历史同步，TradingView 要求 `symbol.startswith("TV:")`，默认不补缺；异常直接转成状态文本。
- `run_backtest`：解析参数，创建 `ResearchJobConfig`，从 repository 加载 bars，调用 `ResearchService.backtest` 生成并持久化报告，再调用服务的详细回测内部方法生成信号、图表和交易记录。它不是订单回调。
- `preview_strategy_chart`：在策略/标的/周期/日期/方向/参数变化时只加载 bars 和执行展示回测，不写研究报告、不更新指标表；旧请求若版本过期则丢弃输出。
- `run_optimize`：解析搜索空间，删除 `trade_mode` 搜索维度，把方向作为固定参数传给 `ResearchService.optimize`；返回最优指标、参数、Top 结果和候选状态。
- `run_walk_forward`：调用服务走查，再按各折测试段重新生成 UI 所需的样本外连续权益/交易表；服务层的折和报告仍负责持久化。
- `fetch_live_status`：Redis/OKX 读取分别捕获异常并显示为离线、过期或查询失败，不把读状态失败伪装成成功。
- `manage_add_target`、`manage_update_target`、`manage_delete_target` 与 `add_monitored_target`、`remove_monitored_target`：检查重复/选择项后用 ConfigService 持久化目标，随后刷新表格和监控状态。
- ETF 回调 `_load_etf_panel` 和 `_ingest_etf_panel` 将数据库行转换为摘要、日频明细、任务审计、Plotly 图和买入/卖出观察候选表，并更新动态标的筛选器；观察池格式错误、外部采集异常都回显到页面。
- `app.load` 会依次加载设置、读取 live 状态、自动运行一次回测、加载 ETF 面板；因此首次打开页面会触发数据库读取和一次研究回测。

## 研究与交易边界

### 研究路径

- `backtest`、`optimize`、`walk-forward`、`ablate-strategy`、`validate-strategy` 只读取已落库行情/因子并写研究报告、研究运行和折结果。
- `ResearchService` 通过 strategy registry 动态加载策略；辅助周期和 `FactorObservation.available_at` 用于构造 `MarketDataContext`，因子仓库只返回在决策时间已可用的观测。
- 研究报告文件由 `research.report_dir` 控制，命名包含策略和模式；数据库的 `ResearchRunRepository` 保存证据 ID，审批命令据此核对数据集和状态。
- TradingView 适配器、AkShare 资金流和 ETF 市值快照都是研究/数据采集边界，不应直接被解释为 OKX 可成交价格或历史上的点时事实。

### 监控与交易路径

- 监控只处理已完成 K 线，Redis 保存当前 signal、已处理 K 线时间、开仓时间/冷却时间和 live status/log。
- 风控会校验允许标的、最大下单量、最大回撤和是否允许开仓；数据过期或缺口时不会增加暴露，已有仓位仍可能被允许关闭。
- `OrderInstruction` 和 `OkxOrderExecutor` 是适配层接口；当前 `MonitoringService` 不执行它们。真实 OKX 订单提交不属于当前直属入口已经完成的能力，启用密钥也不能绕过这一边界。
- Telegram 发送的是信号/动作/状态通知；Gradio 账户区是权益和持仓查询，不是交易终端。

## 对应测试文件索引

| 文件 | 覆盖重点 |
|---|---|
| `tests/unit/test_cleaning.py` | K 线排序、按 key 去重、连续日历补缺、工作日周末缺口不补。 |
| `tests/unit/test_config.py` | 环境映射、嵌套 Settings、数据库必需配置、ConfigService 单项/全部重置且保持同一 Settings 对象。 |
| `tests/unit/test_models.py` | `ResearchJobConfig` 日期/优化约束、`ResearchReport.to_markdown()`。 |
| `tests/integration/test_cli.py` | `doctor` 退出码、同步命令摘要、`live --once` 输出、Gradio 端口优先级、OKX 运行时客户端热加载。 |
| `tests/unit/test_webapp_helpers.py` | 策略平台/影子/数据健康 DataFrame、参数空间解析、自动范围拟合、策略元数据。 |
| `tests/unit/test_responsive_webapp.py` | `webapp.main()` 注册图表预览回调、KDJ 图表输出、过期 preview 结果丢弃。 |
| `tests/integration/test_history_sync.py` | 历史同步服务、清洗和存储边界。 |
| `tests/integration/test_research_workflows.py` | 回测、寻优、走查报告、报告文件和 XAU 研究样例。 |
| `tests/unit/test_event_driven_backtest.py`、`test_backtest_costs.py`、`test_optimization_methods.py` | 事件回测、费用/滑点/资金费率、搜索方法。 |
| `tests/unit/test_research_validation.py`、`test_factor_ablation.py`、`test_trade_mode.py` | 审批前研究门槛、显式消融和多空方向过滤。 |
| `tests/integration/test_monitoring.py`、`tests/unit/test_monitoring_instance.py`、`test_notify_only.py` | 监控信号、闭合 K 线幂等、持仓/冷却、通知-only 边界。 |
| `tests/unit/test_okx_order_executor.py`、`tests/integration/test_okx_real_api.py` | OKX 执行器的动作映射/重试和外部 API 边界；真实 API 测试应按环境谨慎运行。 |
| `tests/unit/test_monitored_target_parameters.py` | 目标级参数覆盖及 CLI → MonitoringService 传递。 |
| `tests/integration/test_telegram_bot_commands.py`、`tests/unit/test_telegram_bot_formatting.py`、`test_telegram_research_service.py` | Telegram 命令、状态格式和研究查询回调。 |
| `tests/unit/test_akshare_fund_flow.py` | AkShare 资金流和 ETF 总市值筛选。 |

快速定位直属模块的测试可运行：

```bash
uv run pytest \
  tests/unit/test_cleaning.py \
  tests/unit/test_config.py \
  tests/unit/test_models.py \
  tests/unit/test_webapp_helpers.py \
  tests/unit/test_responsive_webapp.py \
  tests/integration/test_cli.py
```

全量测试使用 `uv run pytest`。依赖外部数据库、Redis、OKX、TradingView 或 AkShare 的集成测试应先确认测试环境和凭据隔离；不要把生产密钥用于测试。

## 常用运行命令

```bash
# 查看当前注册命令与选项
uv run vntdr --help

# 检查数据库、Redis 和 VeighNa 依赖
uv run vntdr doctor

# 研究：日期用 ISO 8601 文本；先确认数据已同步
uv run vntdr backtest \
  --strategy cm_macd_ult_mtf \
  --symbol XAU-USDT-SWAP \
  --interval 4h \
  --from <START_ISO> --to <END_ISO>

uv run vntdr optimize \
  --strategy cm_macd_ult_mtf \
  --symbol XAU-USDT-SWAP \
  --interval 4h \
  --from <START_ISO> --to <END_ISO> --method heuristic

uv run vntdr walk-forward \
  --strategy cm_macd_ult_mtf \
  --symbol XAU-USDT-SWAP \
  --interval 4h \
  --from <START_ISO> --to <END_ISO> \
  --train-window <TRAIN_BARS> --test-window <TEST_BARS>

# 只运行一轮监控，不进入常驻循环
uv run vntdr live --once

# 启动 Gradio；--port 优先于 GRADIO_PORT
uv run vntdr gradio --port 7860
```

生产或共享部署在使用 ETF 表前应按项目迁移流程执行 `alembic upgrade head`；直属命令中的 `Database.create_schema()` 适合启动/测试兜底，不替代版本化迁移。容器入口对旧版“完整建表但无 Alembic 状态”的数据库只将其标记到 `20260730_03` 基线后继续升级，部分 schema 或已有 revision 的失败仍会阻止启动。运行 `live` 前必须确认数据库、Redis、Telegram 访问控制、OKX Demo/实盘开关和监控目标均已审阅；敏感变量只通过安全注入提供。

## 已知限制与维护提示

1. **周期集合不一致**：`Interval` 接受 `w` 等规范形式，但 `clean_bars`、历史同步和监控使用的 `INTERVAL_TO_DELTA` 只覆盖有限周期；新增周期必须同时更新清洗、完成 K 线判定、数据质量和测试。
2. **研究引擎与 UI 有私有耦合**：Gradio 直接调用 `ResearchService._load_bars`、`_execute_backtest`、`_metrics_from_returns`，并调用 `ConfigService._load_overrides`。重命名这些私有方法会在页面运行时才暴露问题，应先补回调测试。
3. **数据健康门槛需区分视图与运行时**：策略平台 `_data_health_df` 明确以 50 根 K 线生成展示门；`MonitoringService.monitor_once` 调用 `assess_bars` 时使用其默认最小根数，实际还会受缺口/过期、风控和策略逻辑影响。若要把 50 根变成强制开仓门，必须在服务层统一实现并补集成测试，不能只改 UI 文案。
4. **当前版本不下单**：不要因为配置了 OKX 凭据、看到 `OrderInstruction` 或看到账户持仓而宣称完成实盘买卖；下单调用缺失是明确的交易边界。若未来放开，必须重新审核执行模式、幂等、失败重试、持仓对账和测试。
5. **配置覆盖没有完整模型重校验**：ConfigService 的动态 `set` 主要做当前类型转换；新字段、Literal、范围约束和嵌套结构要在服务层显式验证，并测试坏值、重启恢复和并发读写。
6. **密钥暴露面**：Gradio 设置加载会把 `SecretStr` 解包给密码输入组件，override JSON 也可能保存明文密钥。必须限制 UI、volume、文件权限和日志访问，提交前清理本地凭据并轮换已泄露密钥。
7. **外部数据不是稳定合同**：OKX、TradingView 非官方协议、AkShare 网页数据都可能限流、字段变化或权限变化；保留同步任务失败状态、source/availability 时间和重试记录，不要把采集失败显示成成功。
8. **数据库迁移与建表并存**：CLI/Gradio 会调用 `create_schema()`，ETF 生产链路仍要求 Alembic 迁移；新增 ORM 表或字段时必须同时更新迁移和对应 repository 测试。
9. **Gradio 组件变量名复用**：研究工作流和实盘目标管理都使用 Python 变量名 `manage_status`，后者会覆盖前者的局部引用；修改事件绑定或状态输出时需核对实际组件，避免两个区域状态文本混用。
10. **实时状态依赖 Redis 约定**：看板依赖 `vntdr:live_statuses`、`vntdr:live_logs` 及心跳字段的既有格式；改变 `MonitoringService._save_live_status` 的键名、字段或时间单位时，必须同步更新 webapp、Telegram Bot 和测试。

## 文档维护规则

- 修改直属模块的公开函数、Pydantic 字段、命令名、Gradio 标签/回调、配置键或 Redis/数据库契约时，同步更新本文件和对应测试索引。
- 先用 `rg` 查找调用方和测试，再改文档；不要根据根目录历史进度或旧部署说明推断当前代码能力。
- 新增策略或外部数据源时，说明它属于研究、通知还是交易层，并写清 symbol/exchange/calendar 隔离；不要把研究代理行情写成可成交行情。
- 所有示例使用占位符，不写真实 API Key、Secret Key、Passphrase、数据库密码、Telegram token/chat id 或 TradingView token。
- 本文件是 `src/vntdr` 直属入口 Wiki；子目录文档和代码不应被本任务范围内的更新顺手改动。
