# `migrations/versions` Revision Wiki

本目录的脚本是当前数据库结构的可执行事实。所有脚本均显式使用 `op.create_table`、`op.create_index`、`sa.UniqueConstraint` 和 `op.drop_table`；没有自动生成、外键、检查约束或服务端默认值。字段类型、长度、空值和索引以下列出的“脚本定义”为准，不能只看 `src/vntdr/storage/database.py` 的 ORM 默认值。

## Revision 链

```text
20260725_01_strategy_platform  (2026-07-25, down_revision=None)
        ↓
20260725_02_shadow_runs        (2026-07-25, down_revision=20260725_01)
        ↓
20260730_03_etf_flow           (2026-07-30, down_revision=20260725_02)
        ↓
20260803_04_etf_ohlcv          (2026-08-03, down_revision=20260730_03, head)
```

四个脚本的 `branch_labels` 和 `depends_on` 都是 `None`，当前没有分支或合并点。生产升级必须按上述顺序；降级必须严格反向。下面的“索引”只列脚本显式创建的普通索引；`unique=True` 或 `UniqueConstraint` 产生的唯一约束/数据库内部唯一索引另行列出。

## `20260725_01_strategy_platform.py`

该 revision 是根 revision（`revision="20260725_01"`、`down_revision=None`），创建策略治理和外部因子事实所需的四张表。

### `strategy_versions`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `String(36)`，非空 | 主键；策略版本 ID |
| `strategy_name` | `String(128)`，非空 | 策略插件/名称 |
| `parameters` | `JSON`，非空 | 该版本的固定策略参数快照 |
| `factor_config` | `JSON`，非空 | 因子配置快照 |
| `code_version` | `String(128)`，非空 | 代码版本标识 |
| `created_at` | `DateTime(timezone=True)`，非空 | 版本创建时间 |
| `parent_id` | `String(36)`，可空 | 父版本 ID；脚本没有外键 |

显式索引为 `ix_strategy_versions_strategy_name(strategy_name)` 和 `ix_strategy_versions_created_at(created_at)`。除主键外没有唯一约束；版本不可变、克隆产生子版本是 `StrategyRepository`/领域服务约定，不是数据库约束。该表供版本创建、激活解析、治理审批和监控按闭合 K 线解析参数使用。

### `strategy_instances`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `String(36)`，非空 | 主键；实例 ID |
| `name` | `String(128)`，非空 | 列级 `unique=True`；实例名 |
| `symbol` | `String(64)`，非空 | 标的 |
| `exchange` | `String(32)`，非空 | 交易所/数据源边界 |
| `asset_class` | `String(32)`，非空 | 资产类别 |
| `calendar` | `String(32)`，非空 | 交易日历/连续交易语义 |
| `quote_currency` | `String(16)`，可空 | 计价币种 |
| `primary_interval` | `String(16)`，非空 | 主周期 |
| `auxiliary_intervals` | `JSON`，非空 | 辅助周期列表 |
| `execution_mode` | `String(32)`，非空 | `notify_only`/`paper`/`live` 等执行模式字段 |
| `enabled` | `Boolean`，非空 | 是否启用 |

显式索引为 `ix_strategy_instances_name(name)`。`name` 的列级唯一约束没有在脚本中指定约束名；它与显式普通索引同时存在。该表是 `StrategyRepository` 创建/查询实例、组合运行时筛选启用实例、运行时解析策略版本和监控绑定标的/周期的入口。

### `strategy_activations`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `Integer`，非空 | 主键；激活审计记录 ID |
| `instance_id` | `String(36)`，非空 | 实例 ID；无外键 |
| `strategy_version_id` | `String(36)`，非空 | 版本 ID；无外键 |
| `effective_at` | `DateTime(timezone=True)`，非空 | 生效时间 |
| `approved_by` | `String(128)`，非空 | 审批人 |
| `rollback_of` | `String(36)`，可空 | 被回滚的激活/版本关联；无外键 |

显式索引为 `ix_strategy_activations_instance_id(instance_id)`、`ix_strategy_activations_strategy_version_id(strategy_version_id)` 和 `ix_strategy_activations_effective_at(effective_at)`。没有业务唯一键，因此同一实例可以追加多个生效记录；`StrategyRepository.active_version()` 按 `effective_at` 倒序、再按 ID 倒序选择当前版本。`StrategyGovernanceService` 在 backtest、walk-forward、shadow 和回撤门禁通过后写入该表；策略回滚也是追加带 `rollback_of` 的记录，不是删除旧记录。

### `factor_observations`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `Integer`，非空 | 主键 |
| `symbol` | `String(64)`，非空 | 标的 |
| `exchange` | `String(32)`，非空 | 数据源/交易所隔离 |
| `factor_name` | `String(128)`，非空 | 因子名称 |
| `value` | `Float`，非空 | 因子值 |
| `observed_at` | `DateTime(timezone=True)`，非空 | 因子观察时间 |
| `available_at` | `DateTime(timezone=True)`，非空 | 因子对决策可用的时间 |
| `interval` | `String(16)`，可空 | 周期；是唯一键的一部分 |
| `metadata_json` | `JSON`，非空 | 来源等元数据 |

唯一约束为 `uq_factor_observation(symbol, exchange, factor_name, observed_at, interval)`；显式索引为 `ix_factor_observations_symbol(symbol)`、`ix_factor_observations_factor_name(factor_name)`、`ix_factor_observations_observed_at(observed_at)` 和 `ix_factor_observations_available_at(available_at)`。`FactorSyncService` 通过 `StrategyRepository.upsert_factor()` 写入，研究/策略读取时必须同时满足 `observed_at <= decision_at` 与 `available_at <= decision_at`，防止前视。`interval` 可空意味着数据库对 NULL 的唯一性遵循 PostgreSQL/SQLite 方言语义，不能把该约束误认为所有 NULL 行都能被数据库唯一去重；仓储当前仍是先查再更新/插入。

### Upgrade / downgrade 风险

`upgrade()` 的建表顺序为 `strategy_versions`、`strategy_instances`、`strategy_activations`、`factor_observations`，索引随表创建。`downgrade()` 按 `factor_observations`、`strategy_activations`、`strategy_instances`、`strategy_versions` 删除。降级会永久删除因子事实、激活/回滚审计、实例和版本快照；没有外键并不降低数据损失风险。回退到本 revision 之前必须先回退后续 `20260725_02` 和 `20260730_03`。

## `20260725_02_shadow_runs.py`

该 revision（`down_revision="20260725_01"`，创建日期 2026-07-25）为策略审批提供影子观察审计。

### `shadow_runs`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `String(36)`，非空 | 主键；影子运行 ID |
| `instance_id` | `String(36)`，非空 | 绑定的策略实例；无外键 |
| `strategy_version_id` | `String(36)`，非空 | 绑定的策略版本；无外键 |
| `started_at` | `DateTime(timezone=True)`，非空 | 观察开始时间 |
| `last_observed_at` | `DateTime(timezone=True)`，可空 | 最近权益观察时间 |
| `initial_equity` | `Float`，非空 | 初始权益 |
| `current_equity` | `Float`，非空 | 当前权益 |
| `peak_equity` | `Float`，非空 | 峰值权益 |
| `max_drawdown` | `Float`，非空 | 最大回撤，当前实现以负数记录 |
| `observation_count` | `Integer`，非空 | 权益观察次数 |
| `status` | `String(16)`，非空 | 服务层状态：`active`、`passed`、`failed` |

显式索引为 `ix_shadow_runs_instance_id(instance_id)`、`ix_shadow_runs_strategy_version_id(strategy_version_id)` 和 `ix_shadow_runs_started_at(started_at)`。没有状态 `CHECK`、唯一业务键或外键；状态值、权益大于 0、运行必须为 `active`、通过所需至少 28 天/一条观测/回撤不超过 10% 均由 `StrategyRepository` 校验。CLI 的 `shadow-start`/`shadow-record-equity`/`shadow-finish` 直接使用该仓储，治理审批和 Gradio 看板消费其结果。

### Upgrade / downgrade 风险

`upgrade()` 创建一张汇总型影子运行表，不单独保存每次权益观测明细。`downgrade()` 删除 `shadow_runs`，会丢失影子权益、回撤和审批证据；当前集成迁移测试没有断言该表，不能据此认为它未被生产使用或可安全删除。

## `20260730_03_etf_flow.py`

该 revision（`down_revision="20260725_02"`，创建日期 2026-07-30）为 ETF 资金流研究数据和批次审计提供持久化结构；后续 `20260803_04` 只增加 ETF 日行情列。

### `etf_money_flow_daily`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `Integer`，非空 | 主键 |
| `symbol` | `String(16)`，非空 | ETF 代码，服务层规范为 6 位 |
| `market` | `String(8)`，非空 | `sh`/`sz`/`bj` 等市场 |
| `trade_date` | `Date`，非空 | 交易日 |
| `main_net_inflow` | `Float`，可空 | 主力净流入 |
| `main_inflow_ratio` | `Float`，可空 | 主力净流入比例 |
| `extra_large_net_inflow` | `Float`，可空 | 超大单净流入 |
| `large_net_inflow` | `Float`，可空 | 大单净流入 |
| `large_inflow_ratio` | `Float`，可空 | 大单净流入比例 |
| `calculated_main_net_inflow` | `Float`，可空 | 由分项计算的主力净流入 |
| `main_component_gap` | `Float`，可空 | 主力值与分项计算值的差额 |
| `open_price` / `high_price` / `low_price` | `Float`，可空 | `20260803_04` 增加的日线开高低，供 K 线展示 |
| `volume` / `turnover` / `turnover_rate` | `Float`，可空 | `20260803_04` 增加的成交量/成交额/换手率；公共 fallback 缺失时保持 NULL |
| `close_price` | `Float`，可空 | 收盘价 |
| `pct_change` | `Float`，可空 | 涨跌幅 |
| `available_at` | `DateTime(timezone=True)`，非空 | 研究可用时间；服务默认按交易日 `Asia/Shanghai` 16:10 建模 |
| `fetched_at` | `DateTime(timezone=True)`，非空 | 实际抓取时间 |
| `source` | `String(32)`，非空 | 数据源，例如 `akshare` |
| `retry_count` | `Integer`，非空 | 本次来源请求重试次数 |

唯一约束为 `uq_etf_money_flow_daily(symbol, trade_date)`；显式索引为 `ix_etf_money_flow_daily_symbol(symbol)`、`ix_etf_money_flow_daily_trade_date(trade_date)`、`ix_etf_money_flow_daily_available_at(available_at)` 和 `ix_etf_money_flow_daily_fetched_at(fetched_at)`。`market` 不在唯一键中，当前设计将同一 `symbol + trade_date` 视为同一事实；跨市场同代码场景必须在服务/数据规范化层先处理。`EtfMoneyFlowRepository.upsert_daily()` 以该唯一键先查再更新/插入，面向 Gradio ETF 面板和研究读取，不进入 OKX 执行链路。

### `etf_flow_ingestion_runs`

| 字段 | 脚本类型与空值 | 约束/语义 |
|---|---|---|
| `id` | `Integer`，非空 | 主键；批次审计 ID |
| `run_key` | `String(96)`，非空 | 列级 `unique=True`；批次幂等键 |
| `started_at` | `DateTime(timezone=True)`，非空 | 批次开始时间 |
| `finished_at` | `DateTime(timezone=True)`，可空 | 批次结束时间 |
| `status` | `String(16)`，非空 | `started`/`success`/`retryable` 等任务状态 |
| `requested_count` | `Integer`，非空 | 请求标的数 |
| `successful_count` | `Integer`，非空 | 成功标的数 |
| `failed_count` | `Integer`，非空 | 失败标的数 |
| `retry_count` | `Integer`，非空 | 批次内累计重试数 |
| `details` | `JSON`，非空 | 观察池快照、失败列表等审计详情 |

显式索引为 `ix_etf_flow_ingestion_runs_run_key(run_key)` 和 `ix_etf_flow_ingestion_runs_started_at(started_at)`；`run_key` 的列级唯一约束没有指定约束名，并与显式普通索引同时存在。`EtfFlowIngestionService` 每批先创建该记录，再逐 ETF 写入日数据，最后完成审计；调度器依据失败/`retryable` 安排任务级重试，Gradio 展示最近批次。

## `20260803_04_etf_ohlcv.py`

该 revision（`down_revision="20260730_03"`，创建日期 2026-08-03）不新增表，只为
`etf_money_flow_daily` 增加可空的 `open_price`、`high_price`、`low_price`、`volume`、
`turnover` 和 `turnover_rate`。这些列由 ETF 入库服务从日线行情源合并，用于 Gradio 的
K 线/资金流叠加图和收盘后买入观察点估算；历史资金流行在迁移后保持可读，未能补齐的
行情列保持 NULL。`downgrade()` 删除这六列，会使依赖 OHLCV 的图表退化为资金流视图。

### Upgrade / downgrade 风险

`upgrade()` 先创建 `etf_money_flow_daily` 及其索引，再创建 `etf_flow_ingestion_runs` 及其索引。`downgrade()` 先删批次审计表，再删日数据表。降级会丢失研究事实、`available_at`/来源/重试信息和任务失败审计；降级前必须备份并确认没有依赖这两张表的运行/页面。虽然脚本按审计表优先删除，数据库没有外键，生产风险仍是不可逆数据删除。

## ORM、仓储、服务与测试对照

| 迁移表 | storage/仓储边界 | 服务/入口 | 当前测试关系 |
|---|---|---|---|
| `strategy_versions`、`strategy_instances`、`strategy_activations` | `StrategyVersionORM`、`StrategyInstanceORM`、`StrategyActivationORM`；统一由 `StrategyRepository` 读写 | CLI `strategy-create`/`strategy-approve`/`strategy-rollback`，`StrategyRuntimeService`、`StrategyGovernanceService`、监控/组合运行时 | `tests/unit/test_strategy_versioning.py`、`test_strategy_runtime.py`、`test_governance.py`，以及 CLI 集成测试；fixture 多用 `create_schema()` |
| `shadow_runs` | `ShadowRunORM`；`StrategyRepository.create_shadow_run`、`record_shadow_equity`、`finalize_shadow_run` | CLI `shadow-*`、治理门禁、Gradio 影子运行展示 | `tests/unit/test_shadow_runs.py`、webapp helper 测试；迁移集成测试当前未断言该表 |
| `factor_observations` | `FactorObservationORM`；`StrategyRepository.upsert_factor`、`factors_available_at` | `FactorSyncService` 写入；`ResearchService`/策略按点时上下文读取 | `tests/unit/test_factor_sync.py`、`test_strategy_versioning.py` 的可用时间断言；多用 `create_schema()` |
| `etf_money_flow_daily`、`etf_flow_ingestion_runs` | `EtfMoneyFlowDailyORM`、`EtfFlowIngestionRunORM`；`EtfMoneyFlowRepository` | `EtfFlowIngestionService`、`EtfFlowScheduler`、ETF CLI 和 Gradio 只读/采集入口 | `tests/unit/test_etf_flow_ingestion.py` 验证 upsert、run 审计和重试；迁移集成测试断言两张表存在 |
| `bars`、`sync_jobs`、`research_runs`、`walk_forward_folds` | 对应 ORM 和 `MarketDataRepository`/`ResearchRunRepository` | `HistorySyncService`、`ResearchService`、研究 CLI | 研究/同步单元和集成测试验证业务；当前没有对应 Alembic revision，fixture 多用 `create_schema()` |

`tests/integration/test_alembic_migration.py` 的真实行为是：在临时 SQLite 文件上设置 `VNTDR_DATABASE_URL`，子进程执行 `python -m alembic upgrade head`，只检查六张策略/因子/ETF 表名。它不检查字段类型、nullable、唯一约束、显式索引、`shadow_runs`、四张基础表或 downgrade；新增 revision 时必须按实际 schema 风险补充断言。

## 新 revision 的脚本规则

1. 新脚本必须位于本目录，声明唯一新的 `revision`，把 `down_revision` 指向当前 head；除非明确做分支/合并设计，不得重写旧脚本或制造第二个 head。
2. 只做一个可审计的结构职责；字段类型、长度、空值、默认值、唯一约束、索引、外键/检查约束和删除顺序全部显式写出，并在模块 docstring 中记录 revision、前置 revision 和创建日期。
3. 先检查已有生产数据，再增加非空列、唯一键或类型限制。必要时拆分为兼容阶段，完成回填和重复清理后再收紧约束；不要让 `create_schema()` 的 Python 默认值掩盖真实 DDL 要求。
4. `downgrade()` 必须说明是否会删除数据、依赖关系和备份要求；删表/删列/重命名不能用“可回滚”笼统描述。若应用层没有可逆数据迁移，生产应优先准备前向修复或恢复方案。
5. 修改 ORM 后同时核对 `Base.metadata`、仓储 select/upsert 的业务键、服务读取/写入、PostgreSQL 与 SQLite 行为、迁移集成测试和 PostgreSQL smoke test；当前脚本没有外键，新增引用要明确数据库约束是否必要。
6. 使用 `uv run alembic upgrade --sql head` 审阅离线 SQL，并在临时 SQLite 上验证 upgrade/必要的 downgrade；执行 `uv run pytest tests/integration/test_alembic_migration.py` 及受影响仓储/服务测试。测试通过不代表可以跳过生产备份、单一迁移 owner 和发布顺序检查。
