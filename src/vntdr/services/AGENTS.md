# `src/vntdr/services` 服务层 Wiki

本文档以当前磁盘上的 Python 实现为准，覆盖本目录的全部文件：`__init__.py`、`akshare_fund_flow.py`、`config_service.py`、`data_context.py`、`data_quality.py`、`etf_flow_ingestion.py`、`etf_flow_scheduler.py`、`etf_factor_model.py`、`external_factors.py`、`factor_sync.py`、`governance.py`、`history.py`、`metrics.py`、`monitoring.py`、`portfolio.py`、`portfolio_runtime.py`、`position_sizing.py`、`research.py`、`risk.py`、`strategy_runtime.py`、`telegram_research.py`、`tradingview_history.py`。实现是事实来源；上层说明若与下文的“实现事实/风险”冲突，以代码为准。

## 1. 边界、依赖与持久化总览

服务层不直接创建交易所订单表，也不承载 Gradio/Telegram 的协议细节。典型依赖方向是：

```text
CLI / Gradio / Telegram adapter
        ↓ 组装 Settings、Repository、Notifier、Executor、SignalStore
services
        ├─ MarketDataRepository / ResearchRunRepository / StrategyRepository / EtfMoneyFlowRepository
        ├─ PostgreSQL（或测试 SQLite）
        ├─ Redis：信号、监控心跳和日志
        └─ OKX、FRED、CFTC、AkShare、TradingView 等外部 Provider
```

`services/__init__.py` 只有包说明字符串，不导出服务，不执行初始化或副作用。

服务层实际写入或依赖的持久化对象如下。ORM 定义位于 `storage/database.py`，写入/查询由 `storage/repositories.py` 完成。

| 对象 | 生产者/消费者 | 关键内容 |
|---|---|---|
| `bars` / `BarORM` | `HistorySyncService` 写入；`ResearchService`、`MonitoringService`、`PortfolioRuntimeService` 读取 | `(symbol, exchange, interval, datetime)` 复合业务键、OHLCV、`is_synthetic` |
| `sync_jobs` / `SyncJobORM` | `HistorySyncService` | 同步区间、`started/completed/failed`、插入/清洗/去重计数、错误 |
| `research_runs` / `ResearchRunORM` | `ResearchService` | 模式、策略、数据身份、配置 JSON、指标、最佳参数、Top 结果、报告路径、状态 |
| `walk_forward_folds` / `WalkForwardFoldORM` | `ResearchService.walk_forward` | 每折 train/test 区间、参数和样本外指标 |
| `strategy_versions` / `StrategyVersionORM` | CLI/治理流程创建；`StrategyRuntimeService` 解析 | 参数、因子配置、代码版本、父版本；版本快照不原地改写 |
| `strategy_instances` / `StrategyInstanceORM` | CLI 创建；运行时读取 | 标的、交易所、主/辅助周期、执行模式、启用标志 |
| `strategy_activations` / `StrategyActivationORM` | `StrategyGovernanceService` | 版本在某一 `effective_at` 生效、审批人、回滚来源 |
| `shadow_runs` / `ShadowRunORM` | 当前主要由 CLI/Repository 管理；Gradio 展示 | 影子权益、峰值、最大回撤、观察次数、`active/passed/failed` |
| `factor_observations` / `FactorObservationORM` | `FactorSyncService`；`ResearchService`/策略读取 | `observed_at`、`available_at`、值、来源元数据；唯一键含周期 |
| `etf_money_flow_daily` / `EtfMoneyFlowDailyORM` | `EtfFlowIngestionService` | ETF 每日资金流、可用时间、抓取时间、来源、重试数；按 `(symbol, trade_date)` 幂等 upsert |
| `etf_flow_ingestion_runs` / `EtfFlowIngestionRunORM` | `EtfFlowIngestionService`、调度器/看板 | 一次观察池任务的成功/部分失败/可重试审计 |
| `reports/*.{md,json}` | `ResearchService._persist_report` | 回测、寻优、走查报告；目录来自 `settings.research.report_dir` |
| `config_override.json` | `ConfigService` | 运行时覆盖；默认路径为 `~/.vntdr/config_override.json`，值以 JSON 保存 |

服务层使用的 Redis 状态键：

- 监控每个策略/标的/周期使用 `signal:{symbol}:{interval}:{strategy}`、`processed_bar_ts:*`、`position_opened_bar_ts:*`、`cooldown_until_bar_ts:*`；`RedisSignalStore` 只负责整数 `get/set`。
- `MonitoringService._save_live_status()` 在存在 `signal_store.client` 时写 `vntdr:live_status`、哈希 `vntdr:live_statuses` 和列表 `vntdr:live_logs`，日志最多保留 100 条；服务本身不设置 TTL。
- Telegram 适配器另存最近排名（7 天）和 watch 配置（30 天），键带 chat id；这不是服务层的交易状态。

## 2. 数据：行情、时点上下文与质量门禁

### `history.py`

核心契约和调用链如下：

| 接口 | 实现与调用关系 |
|---|---|
| `HistoryClient.fetch_candles()` | Provider 协议；输入 `(symbol, interval, start, end, limit)`，输出原始字典行。`HistorySyncService` 依赖这个边界，因此可替换为测试客户端或 TradingView 客户端。 |
| `OkxHistoryClient.fetch_candles()` | 通过 OKX 公共 `MarketData.MarketAPI(flag="0")` 分页向过去拉 K 线；小时周期转大写（例如 `4h→4H`），输入时间转 UTC，过滤 `[start,end]`，按时间升序返回含 symbol/exchange/interval/OHLCV 的字典。每页请求固定 100 行，`limit` 用于服务契约但当前实现没有直接作为每页请求值。 |
| `OkxHistoryClient.fetch_candles_async()` | 把同步抓取放入线程池，不改变结果。 |
| `HistorySyncService.sync()` | 创建 `sync_jobs` 记录 → 用 `tenacity.Retrying` 按 `settings.research.sync_retry_count` 重试完整远端抓取（固定等待 0 秒）→ `clean_bars()` → `MarketDataRepository.upsert_bars()` → 完成任务并返回 `SyncResult`。清洗/写库异常会把任务标为 `failed` 后重新抛出。 |
| `HistorySyncService.sync_async()` | 在线程池调用 `sync()`，参数通过 `partial` 传递。 |

`clean_bars()`（直属包的 `cleaning.py`）负责解析 UTC、复合键去重、排序、按 `continuous/weekday` 检测缺口；允许 `fill_missing=True` 时用上一根收盘价生成 `is_synthetic=True` 的合成 K 线。服务层只保存 `clean_bars()` 的计数和结果，不把 `gaps_filled` 写入 `sync_jobs` 表（`SyncResult` 会返回它）。

### `data_context.py`

`MarketDataContext` 是策略插件取得多周期/因子数据的时点边界：

- `__init__(bars_by_interval, factors)` 将周期经 `Interval` 规范化、按 `bar.datetime` 排序；因子按 `(available_at, observed_at)` 排序。
- `closed_bars(interval, at)` 只返回 `bar.datetime + interval.seconds <= at` 的 K 线；`latest_closed_bar()` 返回最后一根；`coverage()` 检查数量。
- `available_factors(factor_name, at)` 同时要求 `observed_at <= at` 和 `available_at <= at`；`latest_factor()` 取排序后的最后一条。

`ResearchService._load_data_context()` 将主周期、`ResearchJobConfig.auxiliary_intervals` 的同标的 K 线以及 `StrategyRepository.factors_available_at(..., config.end)` 组合进该上下文；策略（例如多因子策略）仍必须通过 `closed_bars/latest_factor` 读取，不能绕过上下文直接使用未来数据。

### `data_quality.py`

`assess_bars(bars, interval, checked_at, minimum_bars=1, max_stale_intervals=2, calendar="continuous")` 返回 `DataQualityReport`：

- 数量不足立即 `usable=False`，原因为 `requires at least ... bars`。
- 排序后统计相邻时间差大于 `1.5 * interval` 且被 `_is_open_gap()` 判定为开放时段缺口的次数。
- `age = checked_at - last_bar.datetime`，超过 `interval * max_stale_intervals` 为 `stale`。
- `usable` 只有在不 stale、无缺口且满足最小数量时才为真；优先原因是 `stale data`，其次 `bar gaps`。

监控调用默认 `minimum_bars=1`；Gradio 的“数据健康”表显式用 `minimum_bars=50`，这是两个不同门槛，不能把 UI 的 50 根误认为监控服务的硬编码门槛。

## 3. 研究：回测、寻优、走查、指标与 Telegram 排名

### `metrics.py`

`calculate_metrics(returns, equity_curve, trade_count, interval="1h")` 输入逐步收益、权益曲线和交易数，输出统一字典：`total_return`、`sharpe_ratio`、`max_drawdown`、`trade_count`、`win_rate`、`profit_factor`、`cagr`、`sortino_ratio`、`calmar_ratio`。

- 年化周期数按 `1m/3m/5m/15m/30m/1h/2h/4h/6h/12h/1d` 映射，未知周期回退到 `8760`。
- Sharpe 用样本标准差；Sortino 用负收益平方均值的平方根；胜率只统计正/负收益，零收益不进分母。
- 最大回撤按权益曲线峰值滚动计算；CAGR 采用归一化权益的对数年化，并封顶为有限值；无亏损时 profit factor 使用 `99.9` 哨兵值。
- 空 `returns` 返回全零指标，但非空收益要求 `equity_curve` 有起点和终点。

### `research.py`

核心数据类：`BacktestOutcome(metrics, equity_curve, signals, trades)` 是内部执行结果；`BacktestResult(outcome, bars, parameters)` 是带明细的非持久化结果；`CostModel` 封装费用、滑点、价差和每 bar 资金费。

#### 公共 API 与调用链

| 接口 | 输入/输出与持久化 |
|---|---|
| `backtest(config)` | `_load_bars` 从 `MarketDataRepository.fetch_bars` 读取闭区间数据，`_load_data_context` 组装上下文，`_build_report` 执行事件驱动回测，再 `_persist_report` 写 Markdown/JSON 和 `research_runs`；返回 `ResearchReport`。 |
| `backtest_with_details(config)` | 同样加载数据但只返回 `BacktestResult`，不写报告/研究运行记录。 |
| `factor_ablation(config, variants)` | 在完全相同的 bars/context 上逐个合并显式 overrides；不为每个变体重新寻优，返回 `AblationResult`，公共服务本身不持久化。 |
| `optimize(config, method)` | `_evaluate_parameter_space` 评估参数空间，按 `return` 或默认 Sharpe 排序，返回最佳指标、最佳参数和最多 5 个 Top 结果；写 `research_runs` 和报告。 |
| `walk_forward(config)` | 以 bar 数切 train/test；训练段寻优，`train + test` 作为指标 warm-up，只把 test 期间转换计入样本外结果；每折写 `walk_forward_folds`，最后写聚合 `research_runs`/报告。 |
| `validate_candidate(backtest_config, walk_forward_config, ...)` | 校验两份配置模式与数据身份一致，依次运行回测和走查，门槛为回测交易数、走查折数、走查最大回撤和正总收益，返回 `ResearchValidationResult`。此方法本身不运行 shadow。 |
| `latest_signal(...)` | 加载策略，合并默认参数，优先调用 `target_position_for_context`，其次 `target_position_for_index`，最后 `signal_for_index`；最后应用 `trade_mode`，返回 `-1/0/1`。 |
| `default_parameters/default_parameter_space` | 动态导入 `vntdr.strategies.{strategy_name}`，读取模块常量；参数覆盖与默认值合并，不替换新安全默认值。 |
| `optimize_parameters(...)` | 给监控使用的轻量寻优入口，返回 `(best_parameters, best_metrics, evaluations)`；`optimize_parameters_async` 设计上是线程池包装，但当前调用方式需见风险项。 |
| `backtest_async/optimize_async/walk_forward_async/latest_signal_async` | 对同步公共 API 做线程池包装。 |

动态策略加载由 `_load_strategy()` 完成：必须存在 `Strategy` 类；模块级 `DEFAULT_PARAMETERS`/`DEFAULT_PARAMETER_SPACE` 会挂到类上。`_merged_strategy_parameters()`、`_filter_signal_by_trade_mode()` 和 `_execute_with_context()` 分别负责参数完整性、方向过滤和有无上下文的兼容调用。

#### 参数寻优行为

`_evaluate_parameter_space()` 的实际选择规则：

- 组合数 `<=1000` 时强制精确 grid；`heuristic/bfs/astar` 且组合数 `<=10000` 时也转精确 grid。
- `grid` 枚举所有笛卡尔积；`heuristic/bfs/astar` 在更大空间上使用固定随机种子 42 的中心点/最多 3 个随机种子，邻居步长为各维 `±1`，最多评估 100 个节点。
- 其他方法（包括 `ga`）进入遗传搜索：种群 `max(20, 10 * 维度)`、15 代、固定随机种子 42、前 20% 为父代、保留前 2 个精英。
- 无交易的组合在排序时使用 `-999` 惩罚；目标为 `return` 时主排序总收益、次排序 Sharpe，否则主排序 Sharpe、次排序总收益。

#### 事件驱动回测与成本/交易模式

`_execute_backtest()` 的时序是“用当前已关闭 bar 计算信号 → 在下一根 bar 的 open 成交”，不是同收盘成交，最后一根持仓按最后收盘平仓并计入退出成本。

- `CostModel.execution_cost_rate = fee + (slippage_bps + spread_bps / 2) / 10000`；买/卖成交价按不利方向调整；持仓期间每 bar 扣 `funding_rate_per_bar`。
- 费用选择 `settings.research.use_maker_fee` 对应 maker，否则 taker；参数可启用 ATR sizing，使用窗口内真实波幅，调用 `AtrRiskSizer` 得到 exposure，受最大名义比例限制。
- `min_holding_bars` 防止过早反转；`cooldown_bars` 在平仓后保持空仓；方向参数只接受 `both/long_only/short_only`，禁用方向被转为 0。
- 每个 bar transition 产生一条逐步收益，`TradeRecord` 保存方向、进出时间/价、gross/net return、持仓 bar 数、交易成本和资金费；最后追加 turnover 和交易级胜率/盈亏比。

走查中 `decision_start_index=len(train_bars)-1`，保留训练历史用于指标 warm-up，第一笔样本外信号在训练最后一根闭合后产生并于第一根 test bar 开盘成交；这保留了指标历史且避免同收盘未来数据。上下文函数仍按决策时间过滤辅助 K 线/因子，是防泄漏的第二道边界。

`validate_candidate()` 的服务级门槛是：默认回测至少 10 笔、走查至少 3 折、走查最大回撤不超过 10%、走查总收益大于 0。**实现差异：** `ValidationGate.approved` 还要求 shadow 通过，但 `ResearchService.validate_candidate()` 不产生或检查 shadow；CLI 的 `validate-strategy`/`strategy-approve` 另外检查已通过的 shadow 运行。

### `telegram_research.py`

`TelegramResearchService` 是 Telegram 排名所需的编排器，不是 Bot 协议实现：

- 默认周期为 `15m/30m/1h/4h`，默认方法列表为 `heuristic/ga/grid`，默认回看 24 小时；实际 symbol、策略和回看小时来自 `Settings.research`。
- `available_strategies()` 扫描 `vntdr/strategies/*.py`，跳过私有、`__init__` 和 `base`。
- `rank_intervals()` 对每个周期先调用 `HistorySyncService.sync(fill_missing=True)`，再用 `ResearchService.optimize()`；每个结果含收益、Sharpe、回撤、交易数、最佳参数和同步插入数，最终**按总收益降序**而不是按 `optimize_target` 排名。
- `format_rankings()` 只生成 Telegram 文本；排名结果本身由 adapter 写入 Redis（7 天）。

## 4. 策略生命周期：版本解析与激活

### `strategy_runtime.py`

`ResolvedStrategy` 是不可变的 `(StrategyInstance, StrategyVersion)` 配对。`StrategyRuntimeService.resolve(instance_id, at)` 依次：

1. 从 `StrategyRepository.get_instance()` 取实例；不存在报错。
2. `enabled=False` 立即拒绝。
3. 调 `active_version(instance_id, at)`，仓储按 `effective_at <= at` 倒序取最新激活；没有生效版本时报错。
4. 返回实例和版本快照，供监控调用。

`MonitoringService.monitor_instance_once()` 用最后一根已闭合主 bar 的 `bar.datetime + interval` 作为 `at` 解析版本，因此历史重启不会误用未来激活版本。

### `governance.py`

`StrategyGovernanceService` 是激活/回滚边界：

- `approve_activation()` 要求 `ValidationGate.approved` 为真，即 backtest、walk-forward、shadow 三项都通过；`abs(validation.max_drawdown)` 若超过 `max_drawdown_limit`（默认 10%）拒绝；随后创建 `StrategyActivation` 并交给 `StrategyRepository.activate()` 做实例/版本存在性检查。
- `rollback()` 先取当前有效版本；没有当前版本或目标已是当前版本则拒绝；重新激活目标版本并设置 `rollback_of=current.id`，形成审计链。它假定目标版本此前已被验证，服务自身不再重复验证目标的指标。

生命周期状态可概括为：

```text
创建 StrategyInstance + StrategyVersion
        ↓ backtest → walk-forward → shadow(active)
shadow passed/失败
        ↓ 三项门禁 + 回撤门槛
StrategyActivation 生效
        ↓ runtime 按时间解析
新版本激活 / rollback_of 记录的回滚
```

影子权益的具体 `active → passed/failed` 更新由 `StrategyRepository.record_shadow_equity/finalize_shadow_run` 和 CLI 完成；本目录没有单独的 shadow service。通过的 shadow 默认至少需要一次观测、观察时长 28 天、最大回撤不超过 10%（这些默认值在仓储中，而非 `governance.py`）。

## 5. 组合、风控与运行时

### `portfolio.py`

`PortfolioAllocator.allocate(decisions, annualized_volatility_by_symbol=None, correlations=None)` 把多个 `StrategyDecision` 汇总为 `PortfolioDecision`：

1. 每个决策先用 `signal * confidence * max_strategy_weight`，并限制在单策略权重上下限。
2. 按资产类别累计绝对权重，超过 `max_asset_class_weight` 时按比例缩放。
3. 同标的净合并后应用 `max_symbol_weight`；如提供年化波动率且高于目标，则按 `target_annual_volatility / volatility` 缩放。
4. 对相关系数绝对值达到 `correlation_threshold` 的符号对合并成连通簇，簇绝对暴露超过 `max_correlation_cluster_weight` 时整体缩放。
5. 总 gross exposure 超过 `max_gross_exposure` 时全组合同比例缩放。

输出 `target_weights`、`gross_exposure`、`net_exposure` 和所有 `scaling_reasons`。`_apply_correlation_cluster_caps()` 是原地修改权重的内部函数；allocator 不写数据库、不下单。

### `portfolio_runtime.py`

`PortfolioRuntimeService.run_enabled(volume, lookback_bars=120)`：

- 从 `StrategyRepository.list_instances(enabled_only=True)` 遍历实例。
- 每个实例调用 `MonitoringService.monitor_instance_once(instance_id, runtime, volume, lookback_bars)`，把结果 signal 转为 `StrategyDecision`，confidence 当前固定为 `1.0`，reason 包含策略名和版本 id。
- 单实例异常写入 `errors[instance.name]`，不阻断其他实例；再由 `_risk_inputs()` 从市场仓储读取每个标的最近 120 根 bar，计算按 interval 年化的样本波动率和至少 3 个共同时间点的相关系数。
- 最后调用 allocator，返回 `PortfolioRunResult`。该服务只产生目标权重和错误，当前不执行再平衡订单。

### `position_sizing.py`

`AtrRiskSizer` 在构造时要求 `risk_fraction`、`stop_atr_multiple`、`max_notional_fraction` 均为正。`size(equity, price, atr)`：

- 非正 equity/price/ATR 返回全零 `PositionSizingDecision`。
- `risk_budget = equity * risk_fraction`，`stop_distance = atr * stop_atr_multiple`，`raw_units = risk_budget / stop_distance`。
- 再以 `equity * max_notional_fraction / price` 限制 units，返回 units、名义金额、预算、止损距离和 `capped`。

它只做数量数学，不知道合约乘数、最小下单量或交易所精度。

### `risk.py`

`RiskManager` 的权益峰值只存在进程内：`update_equity()` 更新 current/peak，`get_current_drawdown()` 返回 `(peak-current)/peak`；尚未收到权益时 `check_max_drawdown()` 返回 False（允许继续）。

`filter_instructions()` 对每条指令先验证 symbol 在 `allowed_symbols` 且 volume 不超过 `max_order_size`；超过最大回撤时丢弃开仓动作、保留平仓动作；正常时另外遵守 `allow_opening_trades`。`_is_opening_action()` 只把 `buy_long` 对应 next signal 1、`sell_short` 对应 -1 认作开仓，平仓动作不被这个开仓开关拦截。

`RiskSettings.max_strategy_capital/max_total_exposure` 不在 `RiskManager` 内消费；它们对应的组合上限由 `PortfolioAllocator` 的独立默认参数控制，当前 `CommandContext` 也没有把所有 Settings 风控字段逐一映射到 allocator。修改配置前应核对实际构造路径。

## 6. 监控与通知

### `monitoring.py` 的协议与状态

文件定义三个注入协议：`Notifier.notify(message)`、`OrderExecutor.execute/get_current_positions/get_account_equity`、`SignalStore.get/set`。`MonitoringService` 注入 `ResearchService`、`MarketDataRepository`、通知器、执行器、状态存储和 `RiskManager`，并为阻塞 IO 提供线程池 async 包装。

`reconcile_positions(symbol)` 从执行器读取持仓，按 `posSide=long/short` 且 `pos>0` 推断 `1/-1`；没有持仓时实际返回 `0`。**实现与 docstring 有细微冲突：** docstring 把“无法确定/无持仓”列为 `None`，代码对空列表返回 0。它假定策略同一时间只持有一个方向，多个方向同时存在时返回遇到的第一个有效方向。

`update_account_info()` 从执行器取权益并更新风险峰值；异常只记录 warning，不阻断监控。

### `monitor_once()` 一次闭合 bar 的顺序

1. 读取 Redis 的 signal、已处理 bar、持仓开始 bar、冷却截止 bar 四类状态。
2. 从仓储取最近 `lookback_bars`；按 `bar.datetime + interval` 和当前 UTC 时间筛选已完成 bar，当前无完整 bar 则报错。
3. 更新权益；若没有缓存信号，尝试持仓对账并缓存结果，对账失败则进入“不知道起点”的监控模式。
4. 若最后闭合 bar 的 timestamp 等于 `processed_bar_ts`，保存 `already_processed_closed_bar` 状态并返回稳定信号，不重复优化、通知或动作。
5. 参数优先级为显式 `parameters` → 给定 `parameter_space` 调 `ResearchService.optimize_parameters()` → `default_parameters()`；只用闭合 bar 计算 `latest_signal()`，可带辅助周期和已同步因子上下文。
6. 用 `assess_bars()` 做数据质量门禁。坏/过期/有缺口的数据不能开新暴露；已有持仓在信号改变时仍允许生成平仓方向。
7. 应用版本化的 `min_holding_bars` 和 `cooldown_bars`：未达到最小持仓时保持旧方向；反转后可先转 0，并把 `cooldown_until_bar_ts` 持久化为当前 bar 加冷却 bar 数。
8. 校验 symbol。首次观察（`previous_signal is None`）是 bootstrap：只保存状态，不通知、不生成动作。
9. 非 bootstrap 且信号变化时，`_build_instructions()` 生成平旧仓再开新仓的 `OrderInstruction`；经过 `RiskManager.filter_instructions()` 后才进入结果和通知消息。
10. `_build_message()` 以 HTML 文本包含标的/周期/新旧信号/闭合时间/动作/参数/当前回撤；通知异常只记日志。最后写 signal、processed bar、持仓开始时间和 live status，返回 `MonitorResult`。

`_build_instructions()` 的动作映射：`sell_long` 平多、`buy_short` 平空、`buy_long` 开多、`sell_short` 开空。`_build_potential_alert_message()` 只是盘中潜在信号文案生成器，当前文件没有调用它。

### 交易模式的真实边界

`monitor_once()` 接收 `execution_mode`，也可从 `settings.research.execution_mode` 取得，但当前实现明确记录“orders intentionally suppressed”，**没有调用 `order_executor.execute()`**。因此 `MonitorResult.actions` 只是拟执行动作/通知内容，`execution_error` 在这条路径上不会反映下单错误。`StrategyInstance.execution_mode` 和 Settings 中的 `notify_only/paper/live` 是配置与审计字段，不能据此推断服务会真实下单。

CLI 虽会在有完整 OKX 凭据时组装 `OkxOrderExecutor`、无凭据时组装 `SimulatedOrderExecutor`，但 MonitoringService 当前仍不提交指令。OKX 下单适配器的瞬时错误重试属于 `adapters/orders.py`，不属于本服务层，只有未来显式接通执行调用后才会生效。

## 7. 外部数据与资金流

### `akshare_fund_flow.py`

这是研究侧的 A 股/ETF 资金流 Provider，不向 OKX 执行路径供数。

| 接口 | 行为 |
|---|---|
| `AkShareFlowConfig` | 请求间隔、最大重试、指数退避初始值、随机抖动和退避上限；构造 Provider 时逐项校验非负。 |
| `_load_akshare()` | 延迟导入可选依赖，缺失抛 `AkShareUnavailableError`。 |
| `_market_for_code()` / `_numeric()` | 代码补 6 位并映射 `sh/sz/bj`；数值不可解析转 NaN。 |
| `_fetch_public_flow_frame()` | AkShare wrapper 失败时先访问已验证的 Eastmoney 移动 H5 日资金流端点，再回退同源 desktop `push2his` 端点；兼容 13/15 字段响应，空/异常响应报 `AkShareDataError`。 |
| `_fetch_public_price_frame()` / `_normalize_price_frame()` | ETF 日 K 线 wrapper 失败时使用 Sina 公共日线 fallback；统一规范化开高低收、成交量和可选成交额/涨跌字段，fallback 不估算缺失成交额。 |
| `normalize_flow_frame()` | 强制检查日期、主力/大单/超大单字段；缺列报错，不把缺失数据伪造为 0；生成 `calculated_main_net_inflow` 与 `main_component_gap`，返回规范化、按日期排序 DataFrame。 |
| `summarize_flow_trend()` | 空数据返回空表和 `status=empty`；非空时生成按日 breadth、按股票累计摘要和紧凑 trend。趋势用最近最多 3 日均值减最早最多 3 日均值，阈值为 `max(abs(early_mean)*5%, 1.0)`，结果为 `震荡/转强/转弱`。 |
| `get_csi300_constituents()` | 优先 `index_stock_cons_csindex`，失败回退 `index_stock_cons`；代码/名称字段兼容两种 schema，去重排序，少于 250 只视为异常。 |
| `_with_retries()` / `_retry_delay()` | 每次操作总尝试 `max_retries+1` 次；指数退避 `base*2**attempt`，上限 30 秒，再加随机 jitter；最后包装为 `AkShareDataError` 并保留 cause。 |
| `_fetch_one()` / `fetch_symbol_frame()` | 先调用 `stock_individual_fund_flow`，失败再调用公共 fallback；每次新的 symbol fetch 会重置最近一次 retry 计数。输出可直接入库的未聚合 DataFrame。 |
| `fetch_etf_universe()` | 调 `fund_etf_spot_em`，强制 `代码/名称/总市值`，按当前总市值阈值过滤、降序和可选数量截断；这是当前市值发现快照，不是可用于历史回测的 point-in-time AUM。 |
| `fetch_etf_price_frame()` | 调 `fund_etf_hist_em` 获取 ETF 日 OHLCV；Eastmoney 断连时在同一重试边界内回退 Sina 公共日线，按请求日期窗口过滤，供 Gradio K 线/成交量比和买入、卖出/减仓观察点使用。 |
| `fetch_month()` / `fetch_symbol()` | 批量沪深 300 或单标的按日期过滤；单股失败不阻断批次，输出 daily/stock/summary，并记录成功数、失败详情、retry_count、source。 |
| `month_bounds()` | 返回指定日期（默认今天）所在月 1 日至当天。 |

### `etf_flow_ingestion.py`

- `EtfWatchTarget` 将代码补 6 位、市场转小写，只接受 `sh/sz/bj`，并可携带 AkShare 返回的 ETF 名称。`DEFAULT_ETF_WATCHLIST` 仅作为显式兼容清单；`parse_watchlist()` 解析 `symbol:market` 逗号串，缺市场时用 `_market_for_code()`，按 symbol 去重，空输入回兼容清单。
- `EtfFlowIngestionService.run()` 的日期默认是当前时区本地日到此前 `lookback_days` 个自然日；可由 `start_date/end_date/run_key` 覆盖。每次运行先创建 `etf_flow_ingestion_runs`，可选 `watchlist_resolver` 在运行开始时刷新市值观察池，并在审计 details 中保存代码/名称快照。当前 Gradio ETF 面板默认回看 60 个交易日（采集批次可以保留更长的自然日窗口），展示层再按最近交易日裁剪。
- 每个 target 调 `provider.fetch_symbol_frame()`，过滤日期；生产 Provider 还会调用 `fetch_etf_price_frame()` 合并同日 OHLCV，给每行添加交易日 `16:10 Asia/Shanghai`（可配置）的 `available_at`，再调用 `EtfMoneyFlowRepository.upsert_daily()`。单个 ETF 失败只进入 failures，继续其他 target；结果有 `success/partial/failed` outcome，但只要有失败，任务状态就是 `retryable`。
- 返回/审计字段包括 universe、请求/成功/失败数、rows_seen/rows_inserted、总 retry_count 和失败列表。重复运行按 `(symbol, trade_date)` 更新而不新增。

### `etf_flow_scheduler.py`

`EtfFlowScheduler` 用 APScheduler `BlockingScheduler`：

- `start()` 注册周一至周五的 Cron 任务，默认 `16:10 Asia/Shanghai`，`coalesce=True`、`max_instances=1`、misfire grace 3600 秒。
- `_scheduled_run()` 调 `ingestion_service.run()`；异常或返回 `retryable=True` 都抛 `RetryableEtfFlowError` 并安排一次性 retry job。任务级退避从 `task_retry_base_seconds` 开始按 2 倍增长，封顶 `task_retry_max_seconds`；同一 retry job 已存在时保留更早任务。
- 完整成功才将 `_task_retry_attempt` 清零；`run_once()` 只是直接执行并返回，不自动安排 retry；`shutdown()` 非阻塞关闭。

### `etf_factor_model.py`

这是 ETF 资金流面板和 CLI 共用的研究侧多因子模型，不属于采集或交易执行路径：

- `build_etf_factor_frame()` 只使用 T 日收盘时可得的资金流、OHLCV、滚动收益、均线偏离、波动率、量比和横截面排名；标签是 T+1 开盘进入、持有指定交易日后的收盘收益，避免把 T 日收盘成交混入特征。
- `run_etf_factor_model()` 使用 `SimpleImputer`、`StandardScaler` 和带类别平衡的 `LogisticRegression`；每个样本外折只用更早交易日训练，按模型概率选 Top-K，扣除往返成本，并与全市场等权事件收益比较。
- 输出 `latest_scores`、`fold_metrics`、`event_returns`、`feature_importance`、指标和警告；非重叠样本外事件太少或历史太短时保留 `insufficient_data`/警告，不得把模型分数解释成已验证盈利信号。

### `external_factors.py` 与 `factor_sync.py`

`ExternalFactorProvider` 规定 `fetch(instrument, start, end) -> list[FactorObservation]`。

- `FredCsvProvider` 拉 FRED CSV，跳过空值/`.`，日期按 UTC 解释，只取区间内记录；默认给观察值加 1 天 `availability_delay`，元数据含 source/series id。
- `CftcPositioningProvider.normalize()` 接收每行 `report_date/long/short`，按区间过滤，计算 `(long-short)/(long+short)`（分母为 0 时为 0），默认延迟 3 天可用，保存 long/short 元数据。
- `OkxDerivativesProvider.fetch()` 调 OKX 公共 funding history 和 open interest；资金费按事件时间，OI 按 API snapshot 时间，不把后来的观察回填到更早决策。OI 为允许调用端结束时间后 5 分钟内的窄容差；两类接口非 0 code 都抛异常，输出按 observed time/factor name 排序。
- `FactorSyncService.sync()` 是唯一建议的持久化边界：Provider fetch → 对每条调用 `StrategyRepository.upsert_factor()` → 返回观察数。仓储按 symbol/exchange/factor/observed/interval 幂等更新。

所有因子进入策略前必须经过 `available_at` 与 `observed_at` 双重过滤；Provider 的延迟只是数据建模，真正的无未来读取由 Repository/`MarketDataContext` 共同保证。

### `tradingview_history.py`

这是非官方 TradingView 浏览器 WebSocket 历史行情适配器，不能当作可执行交易所数据：

- `frame_message()` 编码 `~m~length~m~payload`；`decode_frames()` 支持一个 raw 中多个 frame、坏 JSON/坏长度跳过或停止；`tradingview_resolution()` 把 `m/h/d/w` 映射为 TradingView resolution。
- `TradingViewHistoryClient` 强制输入 `EXCHANGE:SYMBOL`，输出 symbol 必须以 `TV:` 开头；默认使用 unauthorized token，也可注入 token、regular/extended session、超时、最大 bar 数和 WebSocket 工厂。
- `_bar_count()` 按时间跨度和 interval 估算请求数量并受 `max_bars` 限制；`_extract_bars()` 只处理 `timescale_update`。`fetch_candles()` 创建 chart/quote session、解析 symbol、建立 series，持续接收并回显 heartbeat，series 完成但尚未覆盖 start 时请求更多数据，最后返回 UTC、`exchange=TRADINGVIEW`、隔离 symbol 的 OHLCV。
- 若通过 `HistorySyncService` 调用，外层同步重试会覆盖 WebSocket fetch 异常；该客户端自身没有独立重试/速率退避。

## 8. 配置与治理横切面：`config_service.py`

`ConfigService` 持有共享的 `Settings` 对象，并保存启动时深拷贝 `_base_settings`。初始化会创建配置目录、加载 JSON 覆盖、原地恢复基础 Settings 再应用覆盖；这样 CLI/Web 的同一个 Settings 引用不会被替换。

| 接口 | 行为 |
|---|---|
| `_load_overrides()` | 配置文件不存在或 JSON/IO/类型错误时视为空覆盖；然后 `_restore_base_settings()`。 |
| `_set_setting()` / `_apply_overrides()` | 支持一层或两层 dotted key；识别 `SecretStr` 并在内存中包装，持久化时写出 secret 原值。 |
| `get()` / `set()` | `set()` 只接受存在的一级/二级字段；根据当前值把 bool/int/float 做简单转换，失败返回 False，然后写覆盖文件。列表/字典不做通用 schema 校验。 |
| `list_all()` | 列出 OKX、研究、风控白名单字段；`CONFIG_LABELS` 只提供中文显示名，不改变字段语义。 |
| `reset()` / `reset_all()` | 删除一个或全部覆盖，保存文件，再原地恢复启动基线。 |

配置不是版本化策略参数的替代品：研究中的策略参数由 `ResearchService.default_parameters()` 合并，持久化实例版本则由 `StrategyVersion` 保存。监控目标 `research.monitored_targets` 是配置覆盖中的 JSON 列表，每个目标可持有独立 parameters。

**安全事实：** 配置覆盖文件会以 JSON 明文保存 secret；`list_all()` 返回的 OKX secret 字段是 `SecretStr` 对象，但 UI 会显式取出其值用于密码框。本文不包含任何 token、API key、passphrase 或账户凭据；部署时应限制覆盖文件权限并避免把它提交到版本库/日志。

## 9. 端到端业务流程与入口接入

### 研究/部署主链路

```text
Settings.from_env + ConfigService 覆盖
        ↓
sync-history / UI 拉取 / TradingView 隔离拉取
        ↓
HistorySyncService：Provider → clean_bars → bars + sync_jobs
        ↓
ResearchService：bars + MarketDataContext → 回测/寻优/走查
        ↓
ResearchRun + Fold + reports/*.md/*.json
        ↓
StrategyVersion/Instance → shadow 观察 → Governance 激活
        ↓
StrategyRuntime 按最后闭合 bar 的有效时间解析版本
        ↓
Monitoring：质量门禁 → 信号/持仓状态 → RiskManager → Telegram 通知
        ↓
PortfolioRuntime：多实例决策 → 波动/相关性/组合上限 → target weights
```

### CLI 接入

`cli.CommandContext` 统一构造数据库、四个 Repository、`HistorySyncService`、`ResearchService`、`MonitoringService`、`StrategyRuntimeService`、`StrategyGovernanceService`、`PortfolioRuntimeService` 和 `TelegramResearchService`。主要命令映射：

| 命令 | 服务层入口 |
|---|---|
| `sync-history` | `HistorySyncService.sync` |
| `sync-tradingview` | 新建 `TradingViewHistoryClient` + `HistorySyncService.sync`，输出必须是 `TV:` symbol |
| `sync-okx-derivatives` | `OkxDerivativesProvider` + `FactorSyncService.sync` |
| `akshare-csi300-flow` | `AkShareFundFlowProvider.fetch_symbol/fetch_month`，输出 CSV/JSON 报告文件，不进 OKX 执行路径 |
| `etf-universe-scan` / `etf-flow-ingest` / `etf-flow-scheduler` | AkShare 市值发现、`EtfFlowIngestionService.run`、`EtfFlowScheduler.start/run_once` |
| `backtest` / `optimize` / `walk-forward` / `ablate-strategy` | `ResearchService` 对应公共 API |
| `research-runs` | `ResearchRunRepository.list_research_runs`，查看研究证据 |
| `validate-strategy` | `ResearchService.validate_candidate`；CLI 另外用 research run、指标和 shadow 记录做审批前检查 |
| `strategy-create` / `strategy-approve` / `strategy-rollback` | 创建版本/实例、`StrategyGovernanceService` 激活/回滚 |
| `shadow-start/record-equity/finish` | 当前直接调用 `StrategyRepository` 的 shadow API |
| `portfolio-run` | `PortfolioRuntimeService.run_enabled`，只打印组合目标/缩放原因/实例错误 |
| `live` | 每个目标“增量同步 → `MonitoringService.monitor_once`”，多目标线程池隔离；启动时尝试 Redis/OKX 持仓对账，并可在后台启动 Telegram Bot |

`CommandContext.refresh_runtime_config()` 每次监控前重新加载覆盖；OKX 凭据、demo、订单参数签名改变时才重建历史/订单客户端。配置热加载是运行时行为，不等于已激活的 StrategyVersion 变更。

### Gradio UI 接入

`webapp.py` 模块级懒加载 `ConfigService`、`Database`、`MarketDataRepository`、`ResearchRunRepository`、`StrategyRepository`、`HistorySyncService`、`ResearchService`：

- 行情区域调用 `HistorySyncService.sync`；来源为 TradingView 时临时创建 `TradingViewHistoryClient`，强制 `TV:`/`TRADINGVIEW` 隔离。
- 回测、参数寻优、走查按钮分别构造 `ResearchJobConfig`，调用 `ResearchService.backtest/optimize/walk_forward`，再把 `ResearchReport` 转成表格、Plotly K 线/MACD、交易记录和折曲线。
- ETF 面板调用 `EtfFlowIngestionService.run` 或仓储查询；常驻调度由 CLI 的 scheduler 负责，UI 的“立即采集”只是一次有界任务。面板按最近交易日窗口显示资金流明细；未筛选时显示动态池每只 ETF 最近一条候选，选择单只 ETF 后由下拉框变更事件自动刷新 OHLCV K 线、MA5、正负资金流和该标的完整候选列表，并以收盘后可用的 3 日流入、MA5 和量比启发式生成买入候选，以及针对已有多头持仓的卖出/减仓候选及参考价/观察区间，明确不是下单信号。
- “数据健康”用 `StrategyRepository.list_instances` + `MarketDataRepository.fetch_latest_bars` + `assess_bars(minimum_bars=50)`；监控看板从 Redis 读取 `live_statuses/live_logs`，并可单独从 OKX 查询账户与持仓。
- 设置和监控目标 CRUD 通过 `ConfigService.set` 写共享覆盖文件；`quant_core`/CLI 下一轮会重新加载。

### Telegram 接入的实现边界

有两条不同链路，不能混为一谈：

1. `MonitoringService` 注入的 `TelegramNotifier`（位于 adapter）在信号变化时直接向 Telegram HTTP API 发送 HTML，失败时尝试纯文本 fallback；这条是通知链。
2. `TelegramCommandBot` 接收 `TelegramResearchService` 和 `monitor_once_callback`，理论上可把排名结果送回研究/监控；`TelegramResearchService` 也提供周期排名所需的完整编排。

**当前实现事实/冲突：** `TelegramCommandBot.build_application()` 实际只注册 `/start`、`/status` 和 `m:status/stop` 回调；代码中保留的 `/rank`、`/run`、`/auto`、`/config`、`/stop` handler 函数未注册。因而不能依据旧说明断言这些命令当前可用；现行 Telegram 入口是状态查询和由 quant_core 触发的信号推送，回测/寻优/配置主入口是 CLI/Gradio。`tests/integration/test_telegram_bot_commands.py::test_legacy_interactive_commands_are_not_registered` 明确锁定这一行为。

## 10. 测试映射

当前 `.venv/bin/pytest` 对以下服务相关单元/集成集合可通过；系统 PATH 中没有 `pytest` 命令时应使用仓库虚拟环境路径。测试映射如下：

| 服务文件/职责 | 主要测试 |
|---|---|
| `akshare_fund_flow.py` | `tests/unit/test_akshare_fund_flow.py`：字段规范化、组件差额、breadth/trend、资金流与价格 wrapper/fallback 重试、ETF 市值筛选 |
| `etf_flow_ingestion.py` / `etf_flow_scheduler.py` | `tests/unit/test_etf_flow_ingestion.py`：观察池解析去重、幂等 upsert、动态 universe、失败任务 retry job |
| `config_service.py` | `tests/unit/test_config.py`、`test_indicator_strategies.py`、`test_monitored_target_parameters.py`、`test_responsive_webapp.py`、`tests/integration/test_cli.py`：原地 reset、策略/目标覆盖、热加载 |
| `data_context.py` / `data_quality.py` | `tests/unit/test_data_context_and_factors.py`、`test_data_quality_and_packs.py`、`test_multi_factor_strategy.py`、`test_multi_timeframe_research.py`：闭合辅助 bar、可用时间、缺口/stale、周期包 |
| `history.py` / `tradingview_history.py` | `tests/integration/test_history_sync.py`、`tests/unit/test_okx_history_client.py`、`test_tradingview_history.py`、`test_exchange_isolation.py`、`test_repository_case_insensitivity.py`：重试、分页、协议 frame、TV 隔离和周期大小写 |
| `external_factors.py` / `factor_sync.py` | `tests/unit/test_external_factors.py`、`test_factor_sync.py`、`test_strategy_versioning.py`：FRED/CFTC/OKX 归一化、延迟可用、持久化与时点读取 |
| `metrics.py` | `tests/unit/test_metrics.py`：空输入、典型指标、无波动、盈亏比哨兵值 |
| `research.py` | `tests/unit/test_backtest_costs.py`、`test_event_driven_backtest.py`、`test_optimization_methods.py`、`test_research_validation.py`、`test_trade_mode.py`、`test_factor_ablation.py`、`test_multi_timeframe_research.py`、`tests/integration/test_research_workflows.py`：下一开盘成交、成本/资金费、持仓/冷却、ATR sizing、寻优、方向过滤、走查和报告 |
| `strategy_runtime.py` / `governance.py` | `tests/unit/test_strategy_runtime.py`、`test_governance.py`、`test_strategy_versioning.py`、`test_shadow_runs.py`、`tests/integration/test_cli.py`：有效时间版本解析、三门禁、回滚审计和影子权益 |
| `position_sizing.py` / `risk.py` | `tests/unit/test_position_sizing.py`、`test_risk_manager.py`、`test_notify_only.py`：ATR/名义上限、symbol/订单量/回撤/开仓过滤、默认通知模式 |
| `portfolio.py` / `portfolio_runtime.py` | `tests/unit/test_portfolio.py`、`test_portfolio_runtime.py`：同标的净额、资产/总敞口、波动/相关簇、实例错误隔离 |
| `monitoring.py` | `tests/integration/test_monitoring.py`、`tests/unit/test_monitoring_instance.py`、`test_monitored_target_parameters.py`、`test_notify_only.py`：闭合 bar 幂等、反转动作、最小持仓、实例版本/辅助数据和“不执行订单” |
| `telegram_research.py` | `tests/unit/test_telegram_research_service.py`、`tests/integration/test_okx_real_api.py`：排名排序/文案、默认周期和真实 API 探针 |
| Telegram/CLI/UI 接入 | `tests/integration/test_telegram_bot_commands.py`、`test_cli.py`、`test_responsive_webapp.py`、`test_webapp_helpers.py`：实际注册的状态命令、旧交互命令不注册、CLI 热加载/输出和 UI 辅助函数 |
| `__init__.py` | 无专门行为测试；包导入由各模块测试间接覆盖 |

## 11. 已知风险、实现冲突与维护提示

以下不是假设，而是阅读当前实现后应在变更或上线评审中显式检查的边界：

1. **真实下单未接通。** 监控会计算并通知 `OrderInstruction`，但有意不调用执行器；即使 Settings/StrategyInstance 标为 `live`，当前服务仍是 notification/shadow 语义。要实现买多、卖多、开空、平空，必须另行设计执行、幂等、成交回报和失败补偿，不能只改配置。
2. **旧 Telegram 说明不可直接作为接口契约。** `/rank`、`/run`、`/auto`、`/config` 等函数存在但未注册；当前可用的注册命令以 `build_application()` 和测试为准。
3. **研究验证与治理验证分层。** `ResearchService.validate_candidate()` 只跑回测/走查；shadow 由 CLI/Repository 另行完成，直接调用服务不能得到完整三门禁结论。
4. **配置覆盖可能暴露凭据。** 覆盖 JSON 保存 secret 原文，UI 也会把 SecretStr 解包到密码输入框；限制文件权限、不要提交覆盖文件、不要把 `ConfigService.list_all()` 原样打日志。
5. **权益峰值不持久化。** `RiskManager` 重启后丢失 peak/current；模拟执行器权益为 0，不能提供真实回撤基线。对账失败也会被记录后继续，缺少“无权益不得开仓”的硬门。
6. **服务实例状态键不含 instance id。** `MonitoringService` 的 signal/processed/cooldown key 按 symbol+interval+strategy 命名；多个 StrategyInstance 共用同三元组时会共享状态，且 `portfolio_runtime` 可能把实例隔离降级为同一状态。
7. **拒绝开仓后仍会保存新 signal。** 风控过滤可以把开仓指令变为空，但后续仍写入 confirmed signal/processed bar；若风险条件随后解除，同一信号不一定再次触发开仓，需要专门的订单意图/执行状态设计。
8. **组合上限与 Settings 不完全连线。** `RiskManager` 不消费 `max_strategy_capital/max_total_exposure`；`PortfolioAllocator` 在 `CommandContext` 中用自己的默认上限构造，配置页修改风控字段不必然改变组合分配。
9. **数据质量门槛存在 UI/监控差异。** 监控默认只要求 1 根 bar，UI 数据健康表要求 50 根；`assess_bars` 的 stale age 基于最后 bar timestamp，调用者必须确保传入的是闭合数据。
10. **外部数据的历史可比性有限。** AkShare ETF 市值 universe 是当前快照；OKX open interest 是当前 API 返回的 snapshot；TradingView 是非官方代理；这些都不能不加说明地当作长期、可执行、严格 point-in-time 的回测数据。
11. **公共源重试不等于数据正确。** FRED/CFTC/OKX factor provider 没有统一的重试/质量校验；AkShare schema/反爬/限流变化会影响结果。重试成功只说明请求成功，不说明跨源口径一致。
12. **研究存在选择偏差风险。** live 监控若每个新 bar 都传入大 parameter space，会在同一近期窗口重复寻优；固定随机种子保证可复现，不保证样本外有效。应以足够历史、走查和 shadow 证据审批版本。
13. **Provider/服务之间的时间语义必须保持一致。** bar timestamp 表示 bar 起点，闭合判定要加 interval；因子分别有 observed/available；ETF 可用时间按本地时区 16:10。新增策略或数据源不能只比较 observed/date 字段。
14. **并发与热加载没有统一锁。** CLI 的 `CommandContext.refresh_runtime_config()` 有锁，Gradio/Telegram 各自读取覆盖文件；多进程同时写 JSON 或同时重建客户端时需要部署层协调。
15. **`optimize_parameters_async` 的签名风险。** `optimize_parameters()` 在 `*` 后要求关键字参数，但当前 async 包装通过 `run_in_executor` 以位置参数调用；未专门修复前，直接使用该 async 入口可能抛 `TypeError`，不能把所有 async 方法都视为等价可用。

修改本目录服务时，至少同步检查对应 Repository/Model、CLI、Gradio、Telegram adapter 和上述测试；本 Wiki 只描述当前实现，不替代交易上线前的权限、密钥、订单幂等和风控验收。
