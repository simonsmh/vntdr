# `src/vntdr/storage` Wiki

本文档描述当前代码的真实持久化边界。实现以 `database.py`、`repositories.py`、`models.py`、Alembic 迁移和现有测试为准；新增功能应先更新实现与迁移，再更新本文档。

## 模块边界

`storage` 只负责 SQLAlchemy 连接/会话、ORM 表映射和仓储读写。`models.py` 中的 Pydantic 模型是服务层与仓储之间的领域数据契约，不是 ORM 模型；仓储会将 ORM 查询结果物化为 Pydantic 对象或普通字典后返回，不把需要会话/懒加载的 ORM 行越过仓储边界。

当前的持久化分工是：

- PostgreSQL（测试中也使用 SQLite）保存可重建的行情、同步/研究审计、策略治理、影子运行、因子观测和 ETF 资金流。
- Redis 保存实时进程间状态和短期缓存：信号、已处理 K 线、持仓计时/冷却时间、live 状态日志，以及 Telegram 的排名和 watch 配置。Redis 不是行情、研究报告或策略版本的权威来源。
- 文件保存配置覆盖和研究/采集产物。默认配置覆盖文件是 `Path.home() / ".vntdr" / "config_override.json"`；容器中 webapp 与 quant_core 通过共享的 `config_data` volume 看到同一文件。报告目录由 `VNTDR_REPORT_DIR` 指定，默认 `reports/`，研究报告的 Markdown/JSON 以及部分 CLI 的 CSV/JSON 摘要写在那里。

不要把订单、持仓、信号历史或研究成交明细想当然地加入数据库：当前没有对应 ORM 表。实时持仓来自 OKX 执行器，当前 `MonitoringService` 生成的下单指令仍被明确抑制在通知/影子路径，未调用 `order_executor.execute()`。

## 数据库连接与会话

### `Database`

`Database(url)` 在 `database.py` 中创建 SQLAlchemy Engine 和 `sessionmaker`：

- `Settings.database.dsn` 优先使用 `VNTDR_DATABASE_URL`；否则由 `PG_HOST`、`PG_PORT`、`PG_USER`、`PG_PASSWORD`、`PG_DB_NAME` 生成 `postgresql+psycopg://...` DSN。
- SQLite URL 会自动加入 `check_same_thread=False`，所以测试可以使用 `sqlite://` 或临时 `sqlite+pysqlite:///...`。生产目标是 PostgreSQL。
- `sessionmaker` 使用 `expire_on_commit=False`。`Database.session()` 是上下文管理器：正常退出提交；任意异常先 rollback，再原样抛出；最后关闭会话。
- `Database.create_schema()` 只是对 `Base.metadata` 调用 `create_all()`，适合临时 SQLite、测试和当前启动兜底，不是生产迁移机制。
- `Database.ping()` 执行 `SELECT 1`，异常不吞掉；CLI `doctor` 将它转换成健康检查结果。

仓储方法通常“一次调用一个会话/事务”。不要把 `Session`、ORM 行或需要懒加载的对象跨线程、跨调用返回；异步仓储接口使用 `ThreadPoolExecutor` 调用同步方法，每个工作线程都应自行创建会话。业务层若需要把多个仓储动作作为一个原子事务，当前接口没有提供事务组合边界，不能假设多个方法调用会自动回滚为一个整体。

## 领域模型与 ORM 映射

`models.py` 中与存储直接对应的模型及其边界如下：

- `BarRecord` 对应 `bars`。它的业务键属性 `key` 是 `(symbol, exchange, interval, datetime)`；`datetime` 会规范为 UTC。`Instrument` 会把 `symbol`/`exchange` 大写，`Interval` 会把周期规范为小写（如 `4H` → `4h`），但 `BarRecord` 本身不会替调用方规范 `exchange` 或 `interval`。
- `ResearchJobConfig` 是回测/寻优/走查输入；`ResearchReport` 和 `FoldResult` 是研究输出，其中报告摘要进入 `research_runs`，折结果进入 `walk_forward_folds`，同时报告文件写入文件系统。`SyncResult` 只是历史同步返回值。
- `StrategyVersion`、`StrategyInstance`、`StrategyActivation`、`ShadowRun`、`FactorObservation` 分别对应同名的治理/审计/因子表；`Instrument`、`Interval` 是其中的嵌套领域值，落库时拆成字符串和 JSON 列表。
- ETF 入库没有 `models.py` 专用 Pydantic 实体；`EtfMoneyFlowRepository` 接收 DataFrame 或字典序列，返回普通字典。其表模型是 `EtfMoneyFlowDailyORM` 和 `EtfFlowIngestionRunORM`。
- `StrategyDecision`、`PortfolioDecision`、`PortfolioRunResult`、`OrderInstruction`、`TradeRecord`、`MonitorResult`、`HealthCheckResult`、`ResearchValidationResult`、`PositionSizingDecision`、`DataQualityReport`、`CleanBarsResult`、`AblationResult` 等目前没有对应表；它们是运行时、风控、回测或接口返回对象。

### 当前 `Base.metadata` 的完整表清单

下面列出 `database.py` 实际声明的全部 ORM 表和字段类别。除特别说明外，`id` 是整数主键；UUID 领域 ID 以 `String(36)` 存储。JSON 字段必须传入可被当前数据库/驱动序列化的字典或列表。

- `bars` / `BarORM`：`id`；数据标识 `symbol`、`exchange`、`interval`、`datetime`；OHLCV `open`、`high`、`low`、`close`、`volume`；合成数据标记 `is_synthetic`。
- `sync_jobs` / `SyncJobORM`：`id`；同步范围 `symbol`、`interval`、`start_at`、`end_at`；状态 `status`；计数 `inserted_count`、`cleaned_count`、`duplicates_removed`；错误文本 `error`。
- `research_runs` / `ResearchRunORM`：`id`；研究标识 `mode`、`strategy_name`、`symbol`、`interval`；状态 `status`；JSON 输入/结果 `config`、`metrics`、`best_parameters`、`top_results`；文件指针 `report_path`。
- `walk_forward_folds` / `WalkForwardFoldORM`：`id`、所属运行 `research_run_id`、折号 `fold_index`；训练/测试时间 `train_start`、`train_end`、`test_start`、`test_end`；JSON `metrics`、`parameters`。
- `strategy_versions` / `StrategyVersionORM`：字符串主键 `id`；`strategy_name`；JSON `parameters`、`factor_config`；`code_version`、`created_at`、可选父版本 `parent_id`。
- `strategy_instances` / `StrategyInstanceORM`：字符串主键 `id`；实例名 `name`；标的/交易场所 `symbol`、`exchange`；品类/日历 `asset_class`、`calendar`；`quote_currency`；主周期 `primary_interval`；JSON 周期列表 `auxiliary_intervals`；执行模式 `execution_mode`；启用标记 `enabled`。
- `strategy_activations` / `StrategyActivationORM`：`id`；`instance_id`、`strategy_version_id`；生效时间 `effective_at`；审批人 `approved_by`；可选回滚来源 `rollback_of`。
- `shadow_runs` / `ShadowRunORM`：字符串主键 `id`；实例/版本 `instance_id`、`strategy_version_id`；`started_at`、`last_observed_at`；权益与回撤 `initial_equity`、`current_equity`、`peak_equity`、`max_drawdown`；观测计数 `observation_count`；状态 `status`。
- `factor_observations` / `FactorObservationORM`：`id`；标的/场所 `symbol`、`exchange`；因子标识 `factor_name`；数值 `value`；观测/可用时间 `observed_at`、`available_at`；可选 `interval`；JSON 元数据 `metadata_json`。
- `etf_money_flow_daily` / `EtfMoneyFlowDailyORM`：`id`；标的/市场/交易日 `symbol`、`market`、`trade_date`；资金流与比例 `main_net_inflow`、`main_inflow_ratio`、`extra_large_net_inflow`、`large_net_inflow`、`large_inflow_ratio`、`calculated_main_net_inflow`、`main_component_gap`；行情 `open_price`、`high_price`、`low_price`、`close_price`、`volume`、`turnover`、`turnover_rate`、`pct_change`；时间/来源 `available_at`、`fetched_at`、`source`；重试计数 `retry_count`。OHLCV 列由 `20260803_04` 迁移增加，可空以兼容历史流量行。
- `etf_flow_ingestion_runs` / `EtfFlowIngestionRunORM`：`id`；幂等键 `run_key`；`started_at`、`finished_at`；状态 `status`；任务计数 `requested_count`、`successful_count`、`failed_count`、`retry_count`；JSON 审计详情 `details`。

当前 ORM 定义和迁移没有声明外键关系；关联完整性由仓储在部分写入前检查，数据库本身不会替所有引用做约束。

## 仓储方法与使用方式

### `MarketDataRepository`

- `upsert_bars(bars)` 按 `(symbol, exchange, interval, datetime)` 查询已有行；已有行更新 OHLCV 和 `is_synthetic`，不存在则插入。返回值只统计插入数，更新数不计入。
- `upsert_bars_from_payloads(payloads)` 先用 `BarRecord.model_validate` 验证整批 payload，再调用上面的 upsert；有无效 payload 时不会进入该次写入。
- `fetch_bars(symbol, interval, start, end, exchange=None)` 按时间升序返回 `BarRecord`。时间范围是闭区间 `>= start`、`<= end`；`symbol` 精确匹配；传 `exchange` 时会对查询值调用 `.upper()`。
- `fetch_latest_bars(..., limit, exchange=None)` 按时间倒序取 `limit` 条，再反转为时间升序返回，便于指标/策略消费。两种查询都同时尝试传入周期的原样、小写和大写形式，以兼容 OKX 的 `4h`/`4H` 漂移。
- 上述同步方法有相应的 `_async` 包装，实际仍在工作线程中执行同步 SQLAlchemy 操作；不要在多个线程共享同一个 Session。

行情表没有数据库级 `(symbol, exchange, interval, datetime)` 唯一约束。当前幂等性是“应用层先查再更新/插入”，所以重复历史同步在单线程/单事务场景下会返回 0 新插入（见 `tests/integration/test_history_sync.py`），但并发写入不能依赖数据库唯一键防止竞态。`interval` 在写入时按传入值原样保存，因此同一时间的 `4h` 与 `4H` 会被视为不同应用键；新增调用方应统一使用 `Interval.value`。需要按交易所隔离可成交 OKX 与 TradingView 代理行情时，必须在查询中传 `exchange`，不能只按 symbol/interval 混合（见 `tests/unit/test_exchange_isolation.py`）。

### `EtfMoneyFlowRepository`

- `upsert_daily(frame, market, available_at, fetched_at=None, source="akshare", retry_count=0)` 接收 DataFrame 或字典序列；标的代码左补零到 6 位，交易日转换为 `date`，非有限/不可转数字值变为 `None`。按 `(symbol, trade_date)` 插入或更新，并返回新增行数。
- `create_run(run_key, started_at, requested_count)` 创建一次采集审计并 `flush()` 取得整数 ID；`complete_run(...)` 更新状态、结束时间、成功/失败/重试计数和 `details`。
- `count_daily(symbol=None)` 统计已规范化的日频行；`fetch_daily(symbols=None, start_date=None, end_date=None, limit=None)` 按交易日倒序、symbol 正序返回普通字典；日期过滤为闭区间。`fetch_latest_runs(limit=20)` 按 `started_at` 倒序返回审计字典。

`etf_money_flow_daily` 有数据库唯一约束 `uq_etf_money_flow_daily(symbol, trade_date)`；`etf_flow_ingestion_runs.run_key` 也有唯一约束。仓储仍采用 select-then-update/insert，不使用 PostgreSQL `ON CONFLICT`；重复 `run_key` 或并发冲突可能直接抛 SQLAlchemy 唯一约束异常。ETF 服务把每个采集任务拆成创建审计、逐标的写入、完成审计等多个仓储事务，不能把它们视作一个跨步骤原子事务。

### `StrategyRepository`

策略治理接口包括：

- `create_version` 保存不可变策略快照：`parameters`、`factor_config`、`code_version`、`created_at`、`parent_id`。修改参数应创建 `StrategyVersion.clone()` 产生新版本，而不是更新旧行。
- `create_instance` 保存实例及其 `Instrument`、主/辅助周期、执行模式和启用状态；`get_instance`、`list_instances(enabled_only=False)` 按 ID 或名称读取。
- `activate` 先检查实例和版本存在，再追加激活记录；`active_version(instance_id, at)` 只选择 `effective_at <= at` 的记录，按生效时间倒序、ID 倒序取最新版本。激活表没有业务唯一键，是追加审计记录。
- `create_shadow_run` 检查实例/版本后创建影子运行；`get_shadow_run`、`list_shadow_runs(instance_id=None)` 读取；`record_shadow_equity` 要求权益大于 0、运行仍为 `active`，更新当前/峰值权益、最大回撤、最后观测时间和计数；`finalize_shadow_run` 只接受 `passed`/`failed`，通过时要求至少一条观测、默认至少 28 天且绝对回撤不超过 `0.10`。
- `upsert_factor` 按 `symbol`、`exchange`、`factor_name`、`observed_at`、`interval` 更新或插入因子；`factors_available_at(instrument, at)` 同时要求 `available_at <= at` 和 `observed_at <= at`，按观测时间升序返回，防止回测前视。

对应的数据库唯一约束是 `strategy_instances.name`、`factor_observations` 的 `uq_factor_observation(symbol, exchange, factor_name, observed_at, interval)`，以及各表主键；版本、激活、影子运行没有额外的业务幂等键。仓储对未知实例/版本/运行抛 `ValueError`，不会静默创建缺失引用。

### `ResearchRunRepository`

- `create_sync_job`/`complete_sync_job` 写入 `sync_jobs` 的开始和结束审计；完成时找不到 ID 会抛 `ValueError`。
- `create_research_run(report, config)` 创建 `started` 运行并保存研究模式、策略/标的/周期、JSON `config`、初始报告指标/参数/Top 结果；`get_research_run` 返回 `(ResearchReport, config, status)`，并把对应折按 `fold_index` 组装回 `FoldResult`；`list_research_runs` 按 ID 倒序、可按 symbol 过滤，默认最多 20 条，但不加载折结果。
- `add_fold_result(run_id, fold)` 追加 `walk_forward_folds` 行；当前不检查父运行是否存在，也没有 `(research_run_id, fold_index)` 唯一约束。
- `finalize_research_run` 更新状态、指标、最佳参数、Top 结果和 `report_path`；未知运行抛 `ValueError`。这些方法也提供同步/异步版本，但文件写入和数据库更新不在同一事务中。

注意：当前 `complete_sync_job_async` 和 `finalize_research_run_async` 的实现把被调用方法的 keyword-only 参数按位置参数传入；在修复并补测试前，不要把这两个异步完成接口当作可用的生产路径。新增异步封装必须使用 `partial` 或显式关键字传参。

研究运行和同步任务没有业务幂等键；每次 `create_*` 都会新建一条记录。若同一业务任务需要重试，调用方必须自己决定 run key/去重策略，不能用数据库自动合并。

## 幂等、事务与查询约定

1. 单个仓储方法内的循环写入共享一个 `Database.session()`，异常会整体 rollback；方法成功退出才 commit。跨方法的流程不是一个事务：例如历史同步的 `create_sync_job`、行情 upsert、`complete_sync_job` 各自提交；研究报告的文件写入、`create_research_run` 和 `finalize_research_run` 也不共享事务。
2. 应用层 upsert 只能在写入键稳定、并发受控时保证幂等。需要强并发幂等时，应同时设计数据库唯一约束/索引和对应 Alembic 迁移，不能只在仓储中再加一次 `select`。
3. 查询结果应在会话内物化，并映射为领域对象/字典；缺失的 `get_instance`、`get_shadow_run`、`get_research_run`、`active_version` 返回 `None`，列表查询无结果返回 `[]`。
4. 所有时间列声明为 `DateTime(timezone=True)`（ETF 交易日是 `Date`）。优先从 `models.py` 的 UTC 规范化模型进入仓储；不要直接把无时区 datetime 混入生产写入。
5. JSON 列 `config`、`metrics`、`best_parameters`、`top_results`、`parameters`、`factor_config`、`auxiliary_intervals`、`metadata_json`、`details` 只承载结构化 JSON。研究配置使用 `model_dump(mode="json")` 是现有调用方式。

## 研究读写路径

### 历史同步

`HistorySyncService.sync` 的实际顺序是：

1. `ResearchRunRepository.create_sync_job` 创建 `sync_jobs` 的 `started` 记录。
2. OKX 公共 K 线或 TradingView 研究代理返回 payload；`clean_bars` 去重/补缺并构造 `BarRecord`。
3. `MarketDataRepository.upsert_bars` 写入 `bars`。
4. 成功时更新同步计数为 `completed`；任意异常时写 `failed` 和错误文本后重新抛出。

TradingView 输出使用独立 symbol 前缀和 `TRADINGVIEW` exchange；研究和实时可成交数据必须继续通过 `exchange` 隔离。

### 回测、寻优、走查

- `ResearchService._load_bars` 从 `bars` 读取主周期；辅助周期再次调用 `fetch_bars`；如果启用因子仓储，则通过 `factors_available_at` 建立点时数据上下文。
- `backtest`/`optimize` 在计算后写入固定命名的报告 Markdown/JSON，再创建并完成 `research_runs`。`walk_forward` 先创建 `started` 的运行，逐折追加 `walk_forward_folds`，最后写报告并完成运行。
- `research_runs` 保存摘要和报告路径，不保存 `TradeRecord` 列表或完整权益曲线；这些只存在内存/报告产物中。报告文件和数据库没有原子一致性保证，文件写失败不会由 storage 自动补偿。

## 实时/组合读写路径

`CommandContext`、Gradio 服务初始化和实时组合运行复用同一个 PostgreSQL DSN：

- `PortfolioRuntimeService` 从 `strategy_instances` 找启用实例；`MonitoringService.monitor_instance_once` 从 `strategy_instances`、`strategy_versions`、`strategy_activations` 解析某根已收盘 K 线生效的版本，并从 `bars` 读主/辅助周期；因子从 `factor_observations` 做点时读取。
- `MonitoringService` 从 `MarketDataRepository.fetch_latest_bars` 读最新行情。当前 Redis 键包括 `signal:{symbol}:{interval}:{strategy_name}`、`processed_bar_ts:...`、`position_opened_bar_ts:...`、`cooldown_until_bar_ts:...`；live 状态写入 `vntdr:live_status`、`vntdr:live_statuses` hash 和最多保留 100 条的 `vntdr:live_logs` 列表。
- RedisSignalStore 只存整数信号/时间戳；OKX 执行器负责读取账户权益和外部持仓。Telegram 的 `rank:last`（按 chat ID 带 `vntdr:` 前缀、7 天 TTL）和 `watch`（30 天 TTL）也是 Redis 状态，Telegram `/config` 修改的配置则走共享 JSON 覆盖文件。
- 信号变化会发送 Telegram 通知并更新 Redis。当前发布代码故意不执行生成的 `OrderInstruction`，没有 `orders`、`positions` 或 signal-history 表；若未来接入真实订单持久化，必须另行定义实体、幂等键、外部订单 ID 和迁移，不能复用 `bars` 或 `research_runs`。

Redis、文件和 PostgreSQL 之间没有分布式事务。任何跨存储流程都必须允许一侧成功、另一侧失败，并设计重试/重建逻辑；数据库中的策略版本和研究数据不能以 Redis 或配置文件作为回源。

## PostgreSQL、Redis、配置文件的具体职责

配置加载顺序是环境/`.env` → `Settings` 基线 → `ConfigService` 读取 JSON 覆盖。`ConfigService.set()` 会就地更新共享 `Settings` 并保存覆盖；`reset`/`reset_all` 删除覆盖后恢复启动基线。quant_core 每次刷新运行配置时重新加载该文件，所以 webapp/Telegram 的设置改动可被实时进程看到。

配置覆盖文件不是数据库：没有事务、版本号或并发锁；读取到无效 JSON/不可读文件时 `ConfigService` 将覆盖视为空字典，写入错误则向上抛出。SecretStr 在保存覆盖时会转为普通字符串，因此不要把生产密钥放进可提交文件或日志，volume 权限也必须受控。

研究报告的 `report_path` 只是数据库中的路径字符串；文件被覆盖、移动或删除时，仓储不会验证路径。需要可审计归档时应使用唯一文件名/外部对象存储策略，并保留数据库引用，而不是把大文件塞进 JSON 列。

## Alembic 与部署要求

Alembic 的 `migrations/env.py` 将 `Base.metadata` 设为目标元数据，并在 `VNTDR_DATABASE_URL` 存在时覆盖 `alembic.ini` 的 URL。当前迁移链为：

- `20260725_01`：`strategy_versions`、`strategy_instances`、`strategy_activations`、`factor_observations`。
- `20260725_02`：`shadow_runs`。
- `20260730_03`：`etf_money_flow_daily`、`etf_flow_ingestion_runs`。
- `20260803_04`：为 `etf_money_flow_daily` 增加可空 ETF 日 OHLCV 列，供 Gradio K 线叠加和收盘后观察点估算。

当前链**没有**为 `bars`、`sync_jobs`、`research_runs`、`walk_forward_folds` 提供 revision；它们虽然在 `Base.metadata` 中声明，却主要由 `Database.create_schema()` 创建。不要据此推断 `alembic upgrade head` 会创建全部 11 张表，也不要用 `create_schema()` 代替生产迁移。

容器 entrypoint 仅在 `VNTDR_RUN_MIGRATIONS=true` 时执行迁移；compose 中 quant_core 开启，webapp 和 etf_ingest 关闭。若旧版已用 `create_schema()` 创建完整迁移表但没有 `alembic_version`，入口会先 stamp 到 `20260730_03` 再升级；这只覆盖完整 legacy schema，不替代生产迁移，也不会掩盖部分 schema/已有 revision 的失败。多个启动路径仍调用 `create_schema()` 作为测试/新环境兜底，但策略平台只读页面的仓储初始化不应被视为迁移替代。因此生产发布顺序应是：

1. 使用目标 PostgreSQL DSN 执行 `uv run alembic upgrade head`（或让唯一负责迁移的容器执行）。
2. 再启动 webapp、quant_core、etf_ingest 等读写服务。
3. schema 有变化时新增向前兼容的 revision，正确设置 `down_revision`，同步更新 ORM、仓储映射和迁移集成测试；不要修改已经在生产执行过的 revision。

新增表/列/唯一键必须同时考虑 PostgreSQL 和测试 SQLite 的类型、默认值、索引与 downgrade 行为。对已有数据增加唯一键前先清理/核对重复数据；不要把应用层 select-then-insert 当成迁移约束。

## 错误处理与测试

Storage 层的错误语义是“回滚并抛出”，不是静默修复：

- 会话内 SQLAlchemy/JSON/唯一约束异常会 rollback 后原样传播；仓储没有统一异常包装或自动重试。
- `ValueError` 用于未知同步任务、研究运行、实例/版本/影子运行，非法影子状态/权益/通过条件，以及模型输入校验失败。
- `get_*`/查询无数据用 `None` 或空列表表示。`HistorySyncService` 和 ETF 入库服务在更高层负责外部请求重试、失败审计和 `retryable` 状态；不要把外部 HTTP 重试逻辑塞进仓储。
- RedisSignalStore 的底层 Redis 异常通常向上抛；live 状态日志写入在监控服务中单独捕获并记录 warning。文件配置读取坏 JSON 会降级为空覆盖，文件写入异常不会被吞掉。

现有测试主要用临时 SQLite + `create_schema()`，重点包括：

- `tests/unit/test_repository_case_insensitivity.py`：周期大小写查询、结果排序；`tests/unit/test_exchange_isolation.py`：交易所隔离。
- `tests/integration/test_history_sync.py`：外部重试、同步审计和重复同步的 0 新增；`tests/integration/test_research_workflows.py`、`tests/unit/test_optimization_methods.py`：研究读取、运行/折结果和报告文件。
- `tests/unit/test_strategy_versioning.py`、`test_strategy_runtime.py`、`test_monitoring_instance.py`、`test_governance.py`、`test_shadow_runs.py`：版本快照、生效时间、审批、回滚、影子权益/回撤。
- `tests/unit/test_factor_sync.py`：`available_at` 点时约束；`tests/unit/test_etf_flow_ingestion.py`：日频幂等 upsert、`run_key` 审计和重试状态。
- `tests/integration/test_alembic_migration.py`：在 SQLite 上运行 `alembic upgrade head`，目前断言策略/因子、影子和 ETF 表存在；它不会覆盖未写入迁移的四张基础表。

改动 storage 后至少运行 `uv run pytest`，并在涉及 schema 时同时运行仓储测试、迁移测试和一个真实 PostgreSQL smoke test。涉及 Redis 或文件时使用测试替身/临时文件验证 TTL、键命名、坏数据和共享配置重载；不要把本地 Redis/生产数据库状态带入单元测试。

## 扩展约束

1. 新增持久化实体时同时更新：领域模型（若有）、`database.py` ORM、仓储方法、Alembic revision、迁移测试和本 Wiki；只加 `create_schema()` 不算完成。
2. 业务代码应依赖仓储返回的领域对象/字典，不要新增对 ORM 类或 `Session` 的直接依赖。当前 `cli.py` 的 `strategy-approve` 为了读取版本做了一处直接 ORM 查询，这是现存例外，不应扩散。
3. 先定义业务身份、唯一键和重试语义，再实现 upsert。需要并发安全时优先数据库唯一约束 + 原子 upsert；明确“插入数”和“更新数”的返回含义。
4. 保持 OKX/TradingView 的 `exchange` 隔离；保持 `FactorObservation.available_at` 的点时约束；保持策略版本追加式、激活可审计、影子运行状态校验。
5. 不跨线程共享 SQLAlchemy Session，不返回 detached ORM 行；异步包装用新会话/同步仓储，并正确传递 keyword-only 参数。
6. PostgreSQL 是持久事实源，Redis 是运行状态，JSON 文件是配置覆盖，报告文件是派生产物。新增读写必须明确恢复来源、失败补偿和是否需要幂等，不能把这些存储混用成隐式缓存。
