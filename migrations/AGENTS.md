# `migrations` 数据库迁移 Wiki

本目录是生产数据库结构的 Alembic 入口。迁移脚本、`src/vntdr/storage/database.py` 的 ORM、仓储/服务代码和测试共同定义实际契约；文档不能把 `Base.metadata` 的声明误写成已经由 Alembic 版本化的结构。当前 revision 链是单线链，head 为 `20260803_04`，没有分支、合并 revision 或 `depends_on`。

## 迁移入口与执行语义

| 文件 | 当前真实职责 |
|---|---|
| `alembic.ini` | `script_location = migrations`，`prepend_sys_path = .`；`sqlalchemy.url` 只是 `postgresql+psycopg://placeholder` 占位值。日志级别为 Alembic `INFO`、SQLAlchemy `WARN`。生产和本地执行都应提供真实 DSN，不能依赖占位 URL。 |
| `migrations/env.py` | 从 `VNTDR_DATABASE_URL` 覆盖 `alembic.ini` 的 URL；导入 `vntdr.storage.database.Base` 并把 `Base.metadata` 作为 `target_metadata`。离线模式使用 URL、`literal_binds=True` 和命名参数；在线模式用 `engine_from_config`、`pool.NullPool` 建立连接，在 `context.begin_transaction()` 中执行迁移。 |
| `migrations/script.py.mako` | 新 revision 的模板；生成 `revision`、`down_revision`、`branch_labels`、`depends_on` 以及空的 `upgrade()`/`downgrade()`。模板不会自动推导业务语义，生成后必须人工审阅。 |
| `migrations/versions/` | 当前 4 个显式 DDL revision，按 `revision`/`down_revision` 串成线；每个脚本都自己声明表、列、唯一约束和索引。 |

`target_metadata` 只提供 Alembic 上下文和结构参照，不会在 `alembic upgrade head` 时自动调用 `create_all()`。迁移脚本没有配置 `compare_type` 等自动差异策略，也不应把 ORM 变更当作已经迁移；所有生产结构变更必须落入新的 revision。

本地从仓库根目录执行：

```bash
uv run alembic current
uv run alembic history
uv run alembic upgrade head
uv run alembic downgrade <revision>
uv run alembic upgrade --sql head
```

`VNTDR_DATABASE_URL` 是 `env.py` 直接读取的唯一 Alembic URL 覆盖变量。容器入口会先用 `Settings.from_env().database.dsn` 生成并导出该变量；仅设置 `PG_*` 而直接运行 Alembic 时，不能假定 `env.py` 会自行组装 DSN。

## 当前 revision 覆盖范围

当前 4 个 revision 共创建 7 张应用表，并由 Alembic 维护自己的 `alembic_version` 表；最后一个 revision 只增加 ETF 日行情列：

```text
20260725_01_strategy_platform
        ↓
20260725_02_shadow_runs
        ↓
20260730_03_etf_flow
        ↓
20260803_04_etf_ohlcv (head)
```

已版本化的 7 张表及用途、完整字段和显式索引见 [`versions/AGENTS.md`](versions/AGENTS.md)；`20260803_04` 不新增表，只为 `etf_money_flow_daily` 增加可空 OHLCV 列。`Base.metadata` 当前还声明以下 4 张表，但没有对应 revision：

- `bars`：行情 K 线；
- `sync_jobs`：历史同步审计；
- `research_runs`：回测/寻优/走查运行摘要；
- `walk_forward_folds`：走查折结果。

因此，空数据库执行 `alembic upgrade head` 不等于创建 `Base.metadata` 的全部 11 张应用表。`Database.create_schema()` 通过 `Base.metadata.create_all()` 创建缺失表，适合临时 SQLite、测试和当前启动兜底，但不记录 Alembic revision、不会替生产迁移，也不能弥补已应用 revision 的结构演进。

## Compose/生产执行顺序

`docker-entrypoint.sh` 的顺序是：

```text
VNTDR_RUN_MIGRATIONS=true
  → Settings.from_env().database.dsn
  → export VNTDR_DATABASE_URL
  → legacy schema 检测（必要时 stamp 20260730_03）
  → alembic upgrade head
  → exec 容器命令
```

旧版镜像可能已经通过 `Database.create_schema()` 创建完整的 7 张迁移管理表、但没有
`alembic_version`。入口只在确认这组表完整且 Alembic 状态缺失/为空时 stamp 到
`20260730_03`，再继续升级；部分 schema 或已有 revision 的迁移失败仍会阻止启动，不会删除数据或吞掉异常。

当前 `docker-compose.yml` 中只有 `quant_core` 设置 `VNTDR_RUN_MIGRATIONS=true`；`webapp` 和 `etf_ingest` 明确为 `false`。`etf_ingest` 等待 `quant_core` 健康后再启动，但 `webapp` 只依赖数据库/Redis 健康，并不等待迁移完成。生产发布应把迁移当作单独的、唯一的 schema owner：

1. 先确认目标 PostgreSQL、备份/恢复点、维护窗口、当前 `alembic current` 和待执行 `alembic history`；确认旧应用与新结构的兼容关系。
2. 等待数据库健康后，由一个受控进程使用目标 `VNTDR_DATABASE_URL` 执行 `uv run alembic upgrade head`；Compose 部署可让 `quant_core` 的入口脚本承担这一次执行，不要让多个服务并发迁移。
3. 检查 `alembic current`、表/列/约束/索引和应用 smoke test；确认 head 后再允许 webapp、`etf_ingest` 和 `vntdr live` 读写新结构。不要把进程存活或 `create_schema()` 成功当作迁移完成。
4. 若迁移失败，保留错误现场，先判断事务是否已回滚以及 `alembic_version` 是否变化；不要盲目重跑或手工删除表。按预先审阅的修复/恢复方案处理。

CLI 的 `CommandContext`、ETF 入库构造路径和部分 webapp 初始化仍调用 `Database.create_schema()`，所以本地启动可能掩盖迁移缺失；这些调用不是生产迁移授权。生产 schema 的事实来源仍是 PostgreSQL 上按顺序应用的 revision。

## 升级、降级与数据风险

- revision 的执行顺序由 `down_revision` 决定，不能跳过前置 revision。当前从 head 回退必须按 `20260730_03 → 20260725_02 → 20260725_01` 的逆序执行；`alembic downgrade base` 会删除当前链创建的全部 7 张业务表。
- 现有脚本没有声明外键、`CHECK`、级联删除或服务端默认值。`instance_id`、`strategy_version_id`、`rollback_of` 等关联完整性由 `StrategyRepository`/治理服务检查，数据库不会替服务拒绝悬空引用；ORM 的 Python `default` 也不会替代迁移脚本的非空约束。
- 当前 downgrade 都是删表型操作，不能视为无损回滚：策略版本/激活、因子事实、影子审批证据、ETF 日数据和采集审计都会丢失。生产不得把 schema downgrade 当作策略回滚；策略版本回滚应走治理服务追加 `StrategyActivation` 审计记录。
- `20260730_03` 的 downgrade 先删 `etf_flow_ingestion_runs`，再删 `etf_money_flow_daily`；`20260725_02` 删除 `shadow_runs`；`20260725_01` 删除 `factor_observations`、`strategy_activations`、`strategy_instances`、`strategy_versions`。即使当前没有外键，脚本顺序表达了数据依赖和审计优先级。
- 任何降级前都必须有可验证备份，并确认旧版本代码能理解降级后的结构；删除、重命名、改变非空/唯一语义或大表索引都要单独评估锁表、耗时、数据迁移和恢复窗口。

## 与 storage、服务和测试的关系

`src/vntdr/storage/database.py` 是 ORM 对照表，`src/vntdr/storage/repositories.py` 是应用写入/查询边界。策略平台与因子表由 `StrategyRepository` 使用：策略实例/版本/激活被 CLI、`StrategyRuntimeService`、`StrategyGovernanceService`、组合运行时和监控读取；影子表由 Repository/CLI 管理并供 Gradio 展示；因子表由 `FactorSyncService` 写入，研究上下文按 `observed_at` 和 `available_at` 做点时读取。ETF 两表由 `EtfMoneyFlowRepository` 和 `EtfFlowIngestionService` 使用，调度器写入批次审计，Gradio 只读展示。

测试边界必须区分 ORM 与迁移：

- `tests/integration/test_alembic_migration.py` 在临时 SQLite 上设置 `VNTDR_DATABASE_URL`，以子进程执行 `python -m alembic upgrade head`，当前只断言 `strategy_versions`、`strategy_instances`、`strategy_activations`、`factor_observations`、`etf_money_flow_daily`、`etf_flow_ingestion_runs` 六张表存在；它没有断言 `shadow_runs`、四张未版本化基础表、完整字段/索引或 downgrade。
- `tests/unit/test_strategy_versioning.py`、`test_strategy_runtime.py`、`test_governance.py`、`test_shadow_runs.py`、`test_factor_sync.py` 和 `tests/unit/test_etf_flow_ingestion.py` 主要用临时 SQLite 的 `Database.create_schema()`，验证仓储/服务语义，不验证 Alembic 脚本能否重建同一结构。
- `tests/integration/test_history_sync.py`、`test_research_workflows.py` 等研究/同步测试同样依赖 `create_schema()`；它们覆盖 `bars`、`sync_jobs`、`research_runs`、`walk_forward_folds` 的使用，但不证明这些表已进入迁移链。

涉及 schema 的改动至少要同时核对 ORM 字段、仓储映射、服务读写、迁移集成测试和一个 PostgreSQL 方言 smoke test；不要只因 `create_schema()` 测试通过就认为生产迁移安全。

## 新 revision 规则

1. 先确定领域模型、业务身份和兼容发布方案，再更新 `database.py` ORM/仓储契约；一个 revision 保持单一、可审计职责，提交前明确 `revision`、`down_revision`、创建日期，默认接在当前唯一 head 上。
2. 不得修改已在共享/生产数据库执行过的 revision；表、列、类型、长度、`nullable`、唯一约束、索引和默认值都要在新脚本中显式声明。除非经过专门设计，不要假定 Alembic 会从 `Base.metadata` 自动生成变更。
3. 对已有数据的非空列、唯一键或类型变化必须先检查生产数据：必要时拆成“可空/回填/校验/收紧”多个阶段，先处理重复值和空值，再创建约束；评估 PostgreSQL 锁与 SQLite 测试兼容性。
4. `upgrade()` 与 `downgrade()` 必须成对审阅。涉及删除、重命名、截断或不可逆转换时，文档写明数据备份、顺序和恢复路径；如果不安全，不要伪装成无损 downgrade。
5. 关联表要明确是否需要数据库外键、删除策略和索引；当前仓库没有外键，新增关系不能悄悄依赖应用层检查。唯一键要与仓储的 upsert/幂等语义一致，并评估并发下的原子性。
6. 运行临时 SQLite 的 `upgrade head`/必要的 `downgrade`，再在目标 PostgreSQL 做结构和数据 smoke test；检查 `alembic current`、离线 SQL、`git diff --check` 以及迁移集成测试。测试 fixture 使用 `create_schema()` 的部分必须明确补迁移覆盖，不能以它替代迁移验证。
7. 新表/字段/索引完成后同步检查 `src/vntdr/storage/AGENTS.md`、对应服务/测试文档和根 Wiki；本次仅限迁移文档的变更不得顺带改动迁移脚本、代码或其他文档。
