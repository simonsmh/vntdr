# `vntdr.factors` Wiki

本目录是 OHLCV 因子的纯计算层，以及按资产类别整理的因子清单。当前实现不联网、不读写数据库、不负责时间点可用性、不做因子聚合或加权、不注册策略，也不下单。生产代码没有从 `vntdr.factors` 自动发现或执行因子；只有本目录内部模块依赖它，测试通过公共入口导入因子和 pack。

## 文件与公共入口

```text
factors/__init__.py
├── ohlcv.py ──> base.py ──> models.BarRecord
└── packs.py ──> base.py
                └── ohlcv.py
```

| 文件 | 当前职责 |
| --- | --- |
| [`base.py`](base.py) | 定义结构化类型协议 `FactorPlugin`。 |
| [`ohlcv.py`](ohlcv.py) | 定义 `TrendFactor`、`BreakoutFactor`、`AtrRatioFactor` 三个 OHLCV 因子。 |
| [`packs.py`](packs.py) | 定义 `AssetPack`，以及 `gold_pack()`、`equity_index_proxy_pack()`、`crypto_pack()` 三个显式工厂。 |
| [`__init__.py`](__init__.py) | 公开导出 `AssetPack`、三个因子和三个工厂；不导出 `FactorPlugin`。协议需从 [`base.py`](base.py) 导入。 |

`ohlcv.py` 和 `packs.py` 虽然导入了 `FactorPlugin`，三个具体因子没有显式继承协议；它们通过同名 `name` 属性和 `compute` 方法满足结构化类型约定。

## `FactorPlugin` 协议

实现位于 [`base.py`](base.py)：

```python
class FactorPlugin(Protocol):
    name: str

    def compute(self, bars: list[BarRecord], index: int) -> float | None: ...
```

- `name` 是因子标识；协议不检查名称唯一性、单位、版本、数值尺度或是否与 pack 中其他因子冲突。
- `compute` 是同步的单点计算接口。调用方传入一段 `BarRecord` 列表和当前整数索引，返回一个 `float` 或 `None`。协议没有 `fit`、批量计算、权重、聚合、时间戳、`available_at`、数据源、异常处理或生命周期方法。
- 协议没有 `@runtime_checkable`、注册表或自动发现机制；运行时不会因为对象符合接口而自动执行它，也不能把 `isinstance` 检查当作当前契约的一部分。
- 当前三个实现是无状态的 `@dataclass(frozen=True)`；协议本身并不强制无状态或冻结。实现可以在构造时覆写 `window` 和 `name`，但新增参数的边界和含义必须由实现与测试自行规定。

## 输入与调用前提

输入类型是 [`BarRecord`](../models.py)，包含 `symbol`、`exchange`、`interval`、UTC 归一化的 `datetime`、`open/high/low/close`，以及 `volume`、`is_synthetic`。三个当前因子只读 `high`、`low`、`close`，不使用 `open`、`volume` 或 `is_synthetic`。

因子实现本身：

- 不排序、不去重、不检查 K 线是否连续，不校验所有 bar 是否属于同一 `symbol`、`exchange`、`interval`，也不把 `interval` 解析成时间长度；`Interval` 的规范化是模型/服务层职责。
- 不校验 `window` 为正数、不校验 `index` 在列表范围内，也不校验 OHLC 的价格关系。非法索引通常直接抛出 Python 的索引异常；非法窗口可能产生异常或不符合预期的切片结果，不能当作受支持输入。
- 不主动处理 `NaN`、无穷值、负价格或其他坏数据，也没有统一的缺失值填充、丢弃、标准化或异常策略。`None` 只代表各实现明确写出的不可计算边界，调用方不能假定所有因子用同一种方式解释它。
- 正常使用要求 `bars` 已按时间升序排列，并且 `index` 指向决策时可用的当前 K 线。因子不接收决策时间，因此无法自行判断当前 K 线是否已收盘。

## 三个当前 OHLCV 因子

| 类 | 默认 `window` / `name` | 当前实现 |
| --- | --- | --- |
| `TrendFactor` | `20` / `trend_return` | 当 `index < window`，或滞后 bar `bars[index - window].close == 0` 时返回 `None`；否则返回 `bars[index].close / bars[index - window].close - 1`。不截断，也不检查当前收盘价是否为零。 |
| `BreakoutFactor` | `20` / `breakout_position` | 当 `index < window` 返回 `None`。取不含当前 bar 的 `bars[index - window:index]`，令 `H=max(high)`、`L=min(low)`；`H == L` 返回 `0.0`，否则返回 `2 * (bars[index].close - L) / (H - L) - 1`。当前收盘价若超出历史区间，结果可以超出 `[-1, 1]`，不会截断。 |
| `AtrRatioFactor` | `14` / `atr_ratio` | 当 `index < max(1, window)`，或当前收盘价为零时返回 `None`。对 `offset = index - window + 1` 至 `index`（含端点）的每根 bar 计算 `TR=max(high-low, abs(high-previous.close), abs(low-previous.close))`，取这些 TR 的算术平均，再除以当前收盘价。 |

因此，默认参数下 `TrendFactor` 和 `BreakoutFactor` 最早在 `index == 20` 可计算，`AtrRatioFactor` 最早在 `index == 14` 可计算。ATR 计算需要前一根 bar 的收盘价，所以至少要求 `index >= 1`；当前实现用 `max(1, window)` 统一表达这个边界。

三个输出都是无量纲数值：趋势收益和突破位置未做标准化，ATR 是平均真实波幅与当前收盘价之比。实现没有统一的范围承诺；只有 `BreakoutFactor` 的平坦历史区间明确返回 `0.0`。

## 预热、缺失值与避免未来数据

因子只按传入的 `index` 读取数据，不知道哪些 bar 在现实时间已经发布。要避免前视，调用者必须先截断到已闭合 K 线，再调用 `compute`；不能把未收盘 bar 或未来 bar 放在当前索引之后/之前供因子读取。

[`MarketDataContext`](../services/data_context.py) 提供的是服务层的点时边界，而不是 `FactorPlugin` 适配器：

- 构造时把 interval key 规范化，并按 `datetime` 排序；`closed_bars(interval, at)` 只返回满足 `bar.datetime + interval.seconds <= at` 的 K 线，`latest_closed_bar` 和 `coverage` 基于同一规则。
- 它不会自动调用任何插件，也不会把列表转换成某个因子结果。直接调用插件时，调用方仍须使用 `closed_bars` 的结果或自行按决策时间截断，并把正确的末尾索引传入。
- context 层的 bars 默认由调用方按标的和交易所准备；`MarketDataContext` 本身不会再按 `symbol`/`exchange` 过滤 bars。插件也不会检查输入列表的标的、周期、顺序或缺口。

插件自身明确的不可计算规则只有：预热不足时的 `None`、`TrendFactor` 的滞后收盘价为零时的 `None`、`AtrRatioFactor` 当前收盘价为零时的 `None`，以及 `BreakoutFactor` 平坦区间的 `0.0`。其余 `None`、零值、NaN/无穷值和缺口如何处理，必须由上层策略定义，不能把 `None` 自动当成中性值、零仓位或前值。

## `AssetPack` 与三个资产包

[`packs.py`](packs.py) 中的 `AssetPack` 是：

```python
@dataclass(frozen=True)
class AssetPack:
    name: str
    asset_class: str
    factors: list[FactorPlugin] = field(default_factory=list)
```

它只是名称、资产类别和插件清单；没有聚合公式、权重、归一化、缺失值策略、周期声明、版本字段或计算方法。外层 dataclass 是 frozen，但 `factors` 是普通 list，仍可原地修改，属于浅层不可变。`asset_class` 也只是普通字符串，不由该类校验允许值。

三个工厂每次返回新的 `AssetPack` 和新的因子对象，当前清单顺序如下：

| 工厂 | `name` / `asset_class` | 因子清单 |
| --- | --- | --- |
| `gold_pack()` | `gold` / `commodity` | `TrendFactor(20)`、`BreakoutFactor(20)`、`AtrRatioFactor(14)` |
| `equity_index_proxy_pack()` | `equity_index_proxy` / `equity` | `TrendFactor(50)`、`BreakoutFactor(20)`、`AtrRatioFactor(20)` |
| `crypto_pack()` | `crypto` / `crypto` | `TrendFactor(20)`、`BreakoutFactor(20)`、`AtrRatioFactor(14)` |

当前没有按 `asset_class` 自动选择 pack 的注册表，没有 pack 聚合器，也没有“pack → 策略”接线。改变清单顺序会改变调用方遍历顺序，但当前仓库没有代码依赖该顺序。

## 与 `MarketDataContext`、外部因子和多因子策略的边界

### 外部因子不是 `FactorPlugin`

外部/持久化因子使用 [`FactorObservation`](../models.py)，字段为 `instrument`、`factor_name`、`value`、`observed_at`、`available_at`，以及可选 `interval` 和自由格式 `metadata`。模型会把两个时间归一为 UTC，并拒绝 `available_at < observed_at`。这套时间点契约不在 `FactorPlugin` 中。

服务层入口见 [`external_factors.py`](../services/external_factors.py) 和 [`factor_sync.py`](../services/factor_sync.py)：

- `ExternalFactorProvider.fetch(instrument, start, end)` 返回 `list[FactorObservation]`。
- `FredCsvProvider` 跳过 `.`、空字符串等缺失观测，默认将可用时间延后一天；`CftcPositioningProvider` 计算 `(long - short) / (long + short)`，分母为零时给 `0.0`，默认延后三天；`OkxDerivativesProvider` 输出 `okx_funding_rate` 和 `okx_open_interest`，按 OKX 返回的事件/快照时间作为观测和可用时间。
- `FactorSyncService.sync()` 负责一次拉取和逐条 `StrategyRepository.upsert_factor()`；CLI 的 `sync-okx-derivatives` 是当前 OKX 外部因子同步入口。provider、HTTP 和持久化代码不应移入 `factors` 目录。
- 仓储按 `symbol + exchange + factor_name + observed_at + interval` upsert/唯一约束，读取 `factors_available_at()` 时同时要求 `observed_at <= at` 和 `available_at <= at`。`metadata` 可保存来源，但当前没有统一名称注册表、单位/尺度校验、来源冲突仲裁或修订策略。

`MarketDataContext.available_factors()` / `latest_factor()` 只按 `factor_name` 和两个时间字段过滤；不会调用 `FactorPlugin`，也不会在方法内部按 `FactorObservation.interval` 或 instrument 再过滤。正常服务路径先由仓储按 instrument 查询，再把结果传入 context；直接构造 context 时，调用方必须自己保证范围正确。

### `multi_factor` 使用自己的公式

[`strategies/multi_factor.py`](../strategies/multi_factor.py) 没有导入三个 OHLCV 插件，也没有遍历 `AssetPack`。它在策略内部计算：

- EMA 距离形式的趋势分数、此前窗口的突破动量、效率率制度和 ATR 波动率门控；默认预热长度是 `max(trend_window, breakout_window, regime_window, atr_window)`，默认值为 50，不等于插件 pack 的窗口约定。
- `target_position_for_context()` 在 `daily_trend_weight`、`funding_weight` 或 `open_interest_weight` 非零且收到 context 时，才在主分数上合并已闭合日线、`okx_funding_rate` 和 `okx_open_interest`。日线至少需要 20 根闭合 bar；open interest 至少需要两个可用观测且前一值非零；决策时间由当前主 bar 的起始时间加上其 interval 得到。
- 如果三个外部权重全为零，context 方法直接回退到策略自己的 `target_position_for_index()`；没有 context 时，研究服务也不会凭空创建插件结果。

### 研究与监控调用路径

当前路径可概括为：

```text
MarketDataRepository 主/辅助 K 线
StrategyRepository.factor_observations（可选）
        ↓
ResearchService._load_data_context()
        ↓
MarketDataContext
        ↓
策略的 target_position_for_context()
        ↓
backtest / optimize / walk_forward / latest_signal
```

- [`ResearchService`](../services/research.py) 在配置含辅助周期或注入 `factor_repository` 时构造 context；因子先读取截至研究 `end` 的观测，再由每个决策时刻的 `MarketDataContext` 过滤，避免把未来可用时间带入早期决策。
- 回测在闭合当前 bar 上计算信号、在下一根 bar 的开盘成交；walk-forward 保留训练段作为指标 warm-up，只统计测试段；`factor_ablation()` 对相同 K 线使用显式参数变体，不选择或实例化 pack。
- `latest_signal()` 和事件回测在有 context 且策略提供 `target_position_for_context` 时优先调用它，否则依次回退到 `target_position_for_index` 或 `signal_for_index`。研究服务没有 `FactorPlugin` 专用适配器，因此新增插件不会自动进入回测、寻优、走查或监控。
- `ResearchService.optimize(config)` 会把已构造的 context 传入评估；但公共 `optimize_parameters()` 没有 `data_context` 参数。`MonitoringService.monitor_once()` 在已闭合主 bar 上完成最终 `latest_signal` 时可传 context，却会先通过不带 context 的公共 `optimize_parameters()` 做参数寻优。因此外部因子可以影响最终信号，但当前监控参数寻优不会使用外部因子。
- `MonitoringService.monitor_instance_once()` 按最后闭合 K 线解析生效的策略版本，并从 `StrategyRepository` 传入该时刻可用的辅助 bars 和外部因子。`StrategyVersion.factor_config` 虽会持久化，但当前研究/监控没有用它来实例化 `AssetPack`。

## 扩展约束

1. 新增 OHLCV 因子必须满足 `name` 和 `compute(bars, index)` 契约，明确公式、输出尺度、预热规则、零值/平坦区间、输入排序和闭合 K 线假设；优先使用 frozen dataclass 保存参数，并为异常窗口和索引决定明确的支持范围。
2. 因子实现不得直接访问 HTTP、数据库、Redis、Telegram、订单适配器或策略治理服务。数据获取、清洗、时间点可用性和持久化分别留在 services/storage 边界。
3. 若要成为公共导入，更新 [`__init__.py`](__init__.py)；若要进入某资产默认清单，显式修改对应 pack 工厂并记录因子名称、顺序、窗口和资产类别。不要假设 pack 会自动被选择或执行。
4. 若要接入策略，必须另写适配层：在已闭合数据上调用插件，定义多个因子的归一化、缺失值和权重/聚合语义，再由策略的 `signal_for_index`、`target_position_for_index` 或 `target_position_for_context` 消费。回测、走查和监控应复用同一配置及同一可用时间规则。
5. 外部因子应在 services 中实现 provider，归一化为 `FactorObservation`，通过 `FactorSyncService` 和仓储写入，再从 `MarketDataContext` 读取；必须保留 `observed_at` 与 `available_at`，不能以抓取时间或后来的修订值回填早期决策。
6. 扩展至少补充精确数值、预热、缺失/零值、平坦区间、时间顺序和公共导出测试；若接入策略，还要覆盖 context、回测/监控一致性、无前视以及 pack/配置的版本语义。

## 测试映射

| 测试 | 当前验证范围 |
| --- | --- |
| [`test_data_context_and_factors.py`](../../../tests/unit/test_data_context_and_factors.py) | 三个插件的预热和基本数值；辅助日线只在闭合后可见；`FactorObservation.available_at` 过滤。样例也验证 `BreakoutFactor` 的突破结果为 `1.0`，并未覆盖全部公式边界。 |
| [`test_data_quality_and_packs.py`](../../../tests/unit/test_data_quality_and_packs.py) | `gold`/`crypto`/`equity_index_proxy` 的资产类别，以及 equity pack 的 50 根趋势窗口；其余断言属于独立的数据质量服务。 |
| [`test_multi_factor_strategy.py`](../../../tests/unit/test_multi_factor_strategy.py) | 多因子策略自己的预热、趋势/突破分数、滞回、可用 funding、64 组合预注册空间和安全默认值；不证明 `factors` 插件或 pack 已被策略调用。 |
| [`test_multi_timeframe_research.py`](../../../tests/unit/test_multi_timeframe_research.py) | 研究 context 在 4h 决策中不泄漏尚未收盘的 1d bar。 |
| [`test_external_factors.py`](../../../tests/unit/test_external_factors.py)、[`test_factor_sync.py`](../../../tests/unit/test_factor_sync.py) | FRED/CFTC/OKX provider 的归一化与可用延迟，以及同步服务逐条持久化。 |
| [`test_strategy_versioning.py`](../../../tests/unit/test_strategy_versioning.py) | 仓储因子读取同时遵守 `observed_at` 和 `available_at`；也覆盖策略版本激活时间，但不实例化 pack。 |
| [`test_factor_ablation.py`](../../../tests/unit/test_factor_ablation.py) | 相同 K 线上的显式参数变体；这是研究服务测试，不是 pack 选择或插件调用测试。 |
| [`test_monitoring_instance.py`](../../../tests/unit/test_monitoring_instance.py) | 监控按已生效版本传参，并把辅助 bars/因子列表交给 `monitor_once` 的边界；未执行三个插件的公式。 |

当前没有测试：运行时协议注册（因为没有注册机制）、三个公式全部精确值和异常输入/NaN/非法窗口、bar 顺序/缺口、所有 pack 的名称/完整顺序、`AssetPack` 内部 list 的浅层可变性、`FactorObservation.interval` 在 context 中的筛选，以及“pack → multi_factor → ResearchService”的端到端接线。也没有测试监控参数寻优在外部因子存在时的行为差异。

## 已知限制

- `FactorPlugin` 目前只是静态协议；没有运行时注册、版本、来源、单位、尺度或自动发现，因子和 pack 不会自动改变任何策略信号。
- 三个实现缺少参数、索引、OHLC、NaN/无穷值和数据顺序校验；预热/缺失值语义分散在各类中，且 `None` 没有跨因子统一解释。
- `AssetPack` 没有聚合器、权重、周期和版本字段，外层 frozen 不能阻止 `factors` list 被修改；pack 工厂也没有按资产类别自动选择或治理冲突的能力。
- `MarketDataContext` 能治理闭合 K 线和外部因子的观测/可用时间，但不会计算插件；直接传入 context 的 bars/factors 若范围错误，context 不会替调用方修正。它也不会按因子 interval 做筛选。
- 当前多因子策略使用内部公式和外部 `FactorObservation`，不是三个 OHLCV 插件的组合；监控最终信号可读 context，但其独立参数寻优入口不带 context。
- 外部因子没有统一的名称、单位、尺度、修订和冲突治理。短历史、数据缺口、外部源字段变化或延迟会影响上层研究结果；本目录不提供数据质量门或收益保证。
