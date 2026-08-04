# src/vntdr/strategies 策略层 Wiki

本文以当前实现和测试为准。策略层只把调用方提供的、按时间排序的
BarRecord 序列转换为方向信号/目标仓位；不负责行情拉取、数据清洗、持仓
对账、Telegram、风控或订单提交。研究和运行时的调用边界在
../services/AGENTS.md，公共模型在 ../AGENTS.md。策略名中出现的 MTF 不能
推导出完整的 VeighNa 多时间框架能力。

## 1. 基础接口、方向约定和注册表

### 基础类与可选钩子

base.py 的 ReviewedStrategyBase 继承 VeighNa 的 CtaTemplate；未安装
vnpy_ctastrategy 时使用空兼容类，因此单元测试不依赖 VeighNa。唯一规定的
基础类方法是：

~~~python
@classmethod
def signal_for_index(
    cls,
    bars: list[BarRecord],
    index: int,
    parameters: dict[str, Any],
) -> int:
    ...
~~~

返回值约定为 1=多方向、-1=空方向、0=无目标仓位/平仓方向。基础类不检查
index、排序、周期、交易所或参数范围；实现应明确说明所需预热长度。策略
方法本身也不应通过网络或 Repository 读取数据。

研究服务识别但不由基础类声明的可选方法为：

- score_for_index(bars, index, parameters) -> (float, dict[str, float])，用于
  可解释的中间分数；当前只有 multi_factor 提供。
- target_position_for_index(bars, index, parameters, current_position) -> int，
  用于需要当前持仓状态、例如滞后退出的策略。
- target_position_for_context(bars, index, parameters, current_position, data_context) -> int，
  用于消费已按决策时间过滤的辅助周期或外部因子；当前只有 multi_factor
  提供。

ResearchService 的调用优先级是：有 data_context 且策略提供
target_position_for_context 时优先；否则有 target_position_for_index 时
调用；最后才调用 signal_for_index。因此新策略提供状态或上下文钩子后，必须
同步测试其被研究服务实际选中。

### trade_mode 的统一处理

研究服务定义的合法值只有 both、long_only、short_only，并在策略原始信号/
目标仓位生成后统一过滤：long_only 把负信号变为 0，short_only 把正信号
变为 0，非法值抛出 ValueError。它不是进程级全局开关，而是合并进策略参数
快照的方向参数。

内置模块的真实情况不同：

- cm_macd_ult_mtf、demo_momentum、multi_factor 的默认参数包含
  trade_mode="both"，但模块只负责原始算法；CM 明确把它从指标缓存参数中
  排除，demo 直接忽略它，多因子也不在自身钩子中过滤。
- kdj、rsi、volume 的模块默认参数没有 trade_mode。研究服务缺省取 both，
  调用方仍可传入 trade_mode 让服务过滤；直接调用这些策略类不会自动过滤
  方向。

### 动态发现和 UI 元数据

registry.py 的 available_strategy_names() 使用 pkgutil 扫描
vntdr.strategies 包：跳过私有模块以及 base、indicators、registry，导入失败
的模块跳过，只有存在 Strategy 属性的模块才进入结果。排序不是文件系统顺序，
而是 DISPLAY_ORDER：

~~~text
cm_macd_ult_mtf, kdj, rsi, volume, multi_factor, demo_momentum
~~~

当前六个内置模块均可发现；新增模块会按显示顺序未命中时的模块名字典序排在
末尾。

strategy_configs() 对每个发现的模块返回以下浅拷贝元数据，供研究入口和
Gradio 使用，不负责参数校验或执行寻优：

~~~text
label        模块 STRATEGY_LABEL；缺失时使用 registry 的显示标签或模块名
description  模块 STRATEGY_DESCRIPTION；缺失时为空字符串
defaults     DEFAULT_PARAMETERS，缺失时为空字典
space        DEFAULT_PARAMETER_SPACE，缺失时为空字典
bounds       DEFAULT_PARAMETER_BOUNDS；缺失时回退为 space
~~~

当前显示标签是：CM 为 CM MACD Ult MTF、KDJ 为 KDJ 随机指标、RSI 为 RSI
相对强弱、成交量为 成交量突破、多因子为 多因子策略、demo 为 示例动量策略。
其中 CM、多因子和 demo 没有模块级 description，registry 返回空描述；KDJ、
RSI、volume 的描述由各自模块声明。

## 2. 指标辅助函数与缓存契约

indicators.py 不依赖外部指标库，也不持有缓存。它提供：

- bars_fingerprint(bars)：空列表返回 (0,)；非空时组合长度、首根和末根的
  datetime/open/high/low/close/volume。它不包含中间 K 线、symbol/exchange/
  interval。
- rolling_mean(values, window, index, include_current=True)：默认切片为
  values[max(0, index + 1 - window):index + 1]；include_current=False 时不
  包含当前值。窗口不足时使用已有的短样本，空样本返回 0.0。
- rsi_series(closes, period)：Wilder RSI；period 至少按 1 处理，索引
  0..period-1 为 None，索引 period 使用最初 period 个涨跌差初始化，后续
  按 Wilder 递推。平均损失为零时，平均收益大于零返回 100，否则返回 50。
- kdj_series(bars, k_period, d_period, j_period)：从 k_period-1 开始计算
  RSV。K、D 初值都是 50，K 使用 d_period 平滑，D 使用 j_period 平滑，
  J=3*K-2*D；更早位置的三个序列均为 None。三个周期都会按至少 1 处理。

策略把指标预热值转换成 0 或保持空仓，不得把 None/零值误读成有效买卖点。
KDJ、RSI、volume 在类级 _cache 中缓存完整信号列表，键为
(id(bars), tuple(sorted(合并参数.items())))，并额外比较 bars_fingerprint；
CM 使用同样的 id + 参数结构，但用自己的 fingerprint 且剔除 trade_mode。
demo 和 multi_factor 不缓存。

缓存是进程内类变量，没有淘汰策略；它服务于同一组 bars 上逐索引调用的研究
场景，不是持久化事实源。bars_fingerprint 只看首尾和长度，所以在原列表中
间位置发生变化时不会失效；调用方应避免原地修改已传入的序列。CM 的私有
fingerprint 甚至不包含首尾成交量，通用 fingerprint 也不区分 symbol、exchange
和 interval，跨资产/周期复用列表会有碰撞风险。

## 3. 内置策略的真实实现

下文的 space 是当前离散寻优空间，bounds 是 registry/UI 使用的连续边界
字符串；bounds 不会由策略模块自动执行范围校验。未列入某策略 space 的默认
参数不会因为存在 bounds 就自动参与 walk-forward 拟合。

### 3.1 cm_macd_ult_mtf

模块没有自己的 STRATEGY_LABEL 或 STRATEGY_DESCRIPTION，标签由 registry
回退为 CM MACD Ult MTF，描述为空。

默认参数：

~~~python
{
    "fast_length": 6,
    "slow_length": 21,
    "signal_length": 3,
    "trend_window": 7,
    "trade_mode": "both",
}
~~~

离散 space：

~~~python
{
    "fast_length": [2, 4, 6, 8, 10, 12],
    "slow_length": [10, 15, 20, 25, 30],
    "signal_length": [3, 5, 7, 9],
    "trend_window": [3, 5, 7, 9],
}
~~~

连续 bounds：fast_length="2~30"、slow_length="10~80"、
signal_length="2~25"、trend_window="2~40"。

信号逻辑：实现先从所有输入收盘价计算快/慢 EMA，再计算 MACD line、signal
line 和 histogram；EMA 从第一根收盘价初始化，signal line 从第一根 MACD
line 初始化。对 index >= slow_length 且趋势窗口已经覆盖的位置，计算当前
histogram、最近 trend_window 根（包含当前）的 histogram 均值，以及
close - slow_ema：

- 当前 histogram、窗口均值都严格大于零，且收盘价不低于慢 EMA，返回 1；
- 当前 histogram、窗口均值都严格小于零，且收盘价不高于慢 EMA，返回 -1；
- 其余返回 0。

有效正参数下的最早候选索引是 max(slow_length, trend_window - 1)；
fast_length >= slow_length 或 len(bars) <= slow_length 时整段信号全为 0。
它不是交叉事件策略，而是在每个索引返回满足条件时的方向目标。

CM 的缓存预计算整段信号，缓存参数不含 trade_mode，因此改变方向过滤不会
重复计算原始指标。其 fingerprint 为长度、首尾时间和首尾 OHLC，不含首尾
成交量、中间 K 线、symbol/exchange/interval。

名称中的 Ult MTF 只表示设计来源/命名风格。该模块只有一组 bars，没有
target_position_for_context，不会读取 1d 或其他辅助周期，也没有完整的
VeighNa 多周期聚合、周期同步或跨周期状态机。辅助数据只有在服务层传入且
策略实现上下文钩子时才有意义；对 CM 实际不会生效。

### 3.2 demo_momentum

模块没有自己的标签/描述；registry 标签为 示例动量策略，定位是测试和本地
验证用的最小策略。

默认参数：

~~~python
{"lookback": 3, "trade_mode": "both"}
~~~

离散 space 为 {"lookback": [2, 3, 4, 5]}，bounds 为 lookback="1~20"。

当 index < lookback 时返回 0。否则取当前 K 线之前的 lookback 根收盘价
（不含当前），若当前收盘价大于等于该均值返回 1，否则返回 0。它没有独立
做空逻辑、没有持仓状态、没有缓存；trade_mode 在模块方法中被忽略，所以
直接调用永远不会产生 -1，研究服务的 short-only 过滤也不能凭空创造空信号。

### 3.3 kdj

模块标签/描述为 KDJ 随机指标 / 超卖金叉做多、超买死叉做空，反向交叉或
极值退出。

默认参数：

~~~python
{
    "k_period": 9,
    "d_period": 3,
    "j_period": 3,
    "oversold": 20.0,
    "overbought": 80.0,
}
~~~

离散 space：

~~~python
{
    "k_period": [5, 9, 14],
    "d_period": [3, 5],
    "j_period": [3, 5],
    "oversold": [15.0, 20.0, 25.0],
    "overbought": [75.0, 80.0, 85.0],
}
~~~

连续 bounds：k_period="3~30"、d_period="2~15"、j_period="2~15"、
oversold="5~40:5"、overbought="60~95:5"。

策略先缓存 K/D/J 序列，然后从空仓 position=0 开始逐根推进：

- K/D 金叉定义为前一根 K <= D 且当前 K > D；死叉定义为前一根 K >= D
  且当前 K < D。
- 空仓时，金叉且（前一根 K 不高于超卖线或当前 J 不高于超卖线）建立多仓；
  死叉且（前一根 K 不低于超买线或当前 J 不低于超买线）建立空仓。
- 多仓遇死叉或当前 J 达到超买线退出；空仓遇金叉或当前 J 达到超卖线
  退出。

k_period-1 之前 K/D/J 为 None，因此对应信号为 0；还需要一根前值，最早
从 index=k_period 才能判交叉。trade_mode 不在模块默认参数中，直接策略
调用不做方向过滤，研究服务在结果生成后统一过滤。缓存保存的是从输入序列
开头重放得到的状态序列；如果调用方截断历史，初始 position 也会从截断点
重新开始。

### 3.4 rsi

模块标签/描述为 RSI 相对强弱 / RSI 从超卖区回升做多、从超买区回落做空，
并在中轴/极值退出。

默认参数：

~~~python
{
    "rsi_period": 14,
    "oversold": 30.0,
    "overbought": 70.0,
    "exit_midline": 50.0,
}
~~~

离散 space：

~~~python
{
    "rsi_period": [7, 14, 21],
    "oversold": [20.0, 30.0, 35.0],
    "overbought": [65.0, 70.0, 80.0],
    "exit_midline": [45.0, 50.0, 55.0],
}
~~~

连续 bounds：rsi_period="2~50"、oversold="5~45:5"、
overbought="55~95:5"、exit_midline="35~65:5"。

使用 Wilder RSI 的完整缓存序列，从空仓状态推进：

- 前一 RSI 不高于超卖且当前 RSI 上穿超卖线（previous <= oversold < current）
  时建立多仓；
- 前一 RSI 不低于超买且当前 RSI 下穿超买线（previous >= overbought > current）
  时建立空仓；
- 多仓在当前 RSI 达到超买，或从中轴上方下穿 exit_midline 时退出；
- 空仓在当前 RSI 低于等于超卖，或从中轴下方上穿 exit_midline 时退出。

rsi_period 个初始位置为 None；因为交叉还需要前一有效值，最早的入场判断
通常在 index=rsi_period+1。模块没有 trade_mode 默认项，方向开关由研究
服务统一处理；模块级缓存使用通用 bars fingerprint。

### 3.5 volume

模块标签/描述为 成交量突破 / 放量突破前高/前低时建立方向仓位，反向突破
时反转。

默认参数：

~~~python
{
    "volume_window": 20,
    "volume_multiplier": 1.5,
    "price_window": 20,
}
~~~

离散 space：

~~~python
{
    "volume_window": [10, 20, 30],
    "volume_multiplier": [1.2, 1.5, 2.0],
    "price_window": [10, 20, 30],
}
~~~

连续 bounds：volume_window="3~80"、volume_multiplier="1~3:0.25"、
price_window="3~80"。

实现把成交量负值截为零，warmup=max(volume_window, price_window)；在
index >= warmup 时：

- 阈值均值是当前 K 线之前 volume_window 根成交量的均值，明确不包含当前
  成交量；
- 突破参考是之前 price_window 根的收盘价最高/最低，不是 candle high/low；
- 当前成交量大于等于均值乘 volume_multiplier 且当前收盘突破前高，持仓设为
  1；满足放量且跌破前低，持仓设为 -1；
- 没有放量反向突破时，多仓跌破前低退出，空仓突破前高退出；放量反向突破
  则直接把状态改成相反方向。

策略逐根保留本次预计算中的 position，预热前为 0，使用通用 fingerprint
缓存完整信号；模块没有 trade_mode 默认项，方向过滤仍由研究服务完成。

### 3.6 multi_factor

模块没有自己的 label/description；registry 标签为 多因子策略，实现是
可解释的 OHLCV 基线加可选的已同步上下文因子，不是通用因子投票引擎。

完整默认参数如下；其中 trade_mode、最小持仓、冷却和 sizing 字段也会进入
策略版本快照：

~~~python
{
    "trade_mode": "both",
    "trend_window": 50,
    "breakout_window": 20,
    "regime_window": 20,
    "min_efficiency": 0.15,
    "atr_window": 14,
    "max_atr_ratio": 0.04,
    "entry_threshold": 0.6,
    "exit_threshold": 0.2,
    "trend_weight": 0.5,
    "momentum_weight": 0.5,
    "daily_trend_weight": 0.0,
    "funding_weight": 0.0,
    "open_interest_weight": 0.0,
    "funding_rate_scale": 0.001,
    "open_interest_change_scale": 0.05,
    "min_holding_bars": 3,
    "cooldown_bars": 2,
    "enable_volatility": True,
    "enable_atr_sizing": True,
    "risk_fraction": 0.01,
    "stop_atr_multiple": 2.0,
    "max_notional_fraction": 0.30,
}
~~~

默认离散 space 只有六个、共 2*2*2*2*2*2=64 个组合；这是预注册的
walk-forward 小空间，不代表所有默认参数都要每折重拟合：

~~~python
{
    "trend_window": [30, 50],
    "breakout_window": [15, 20],
    "min_efficiency": [0.10, 0.20],
    "max_atr_ratio": [0.03, 0.04],
    "entry_threshold": [0.55, 0.65],
    "exit_threshold": [0.15, 0.25],
}
~~~

连续 bounds 为：

~~~text
trend_window=20~100:10              breakout_window=10~40:5
regime_window=10~40:5               min_efficiency=0.05~0.4:0.05
max_atr_ratio=0.01~0.08:0.01        entry_threshold=0.4~0.8:0.1
exit_threshold=0.05~0.4:0.05        trend_weight=0~1:0.25
momentum_weight=0~1:0.25             daily_trend_weight=0~0.5:0.25
funding_weight=0~0.5:0.25            open_interest_weight=0~0.5:0.25
funding_rate_scale=0.0005~0.003:0.0005
open_interest_change_scale=0.02~0.15:0.01
min_holding_bars=1~10                cooldown_bars=0~10
risk_fraction=0.0025~0.02:0.0025    stop_atr_multiple=1~4:0.5
max_notional_fraction=0.1~0.5:0.1
~~~

#### OHLCV 分数、预热和滞后

score_for_index() 的预热长度是
max(trend_window, breakout_window, regime_window, atr_window)，默认是
50 根；预热索引及当前收盘价为零的索引返回零分和全零解释字段。有效
索引上：

- ATR 使用包含当前 K 线的 true range 均值；趋势 EMA 使用包含当前收盘的
  trend_window 根；趋势分数为 (close - EMA)/(2*ATR) 并截断到 [-1, 1]。
- 动量分数比较当前收盘与之前 breakout_window 根的收盘价区间，区间位置
  映射到 [-1, 1]；之前区间高低相等时为零。
- 效率比为最近 regime_window 根路径变化中的净位移比例；达到
  min_efficiency 才令 regime=1，否则为零门控。
- atr/close <= max_atr_ratio 才令波动门控为 1；enable_volatility=False
  时跳过该门控。趋势和动量按 trend_weight、momentum_weight 归一化加权，
  再乘 regime 与 volatility，得到最终 score。

target_position_for_index() 使用 entry/exit hysteresis：score 大于等于
entry_threshold 返回 1，小于等于负 entry 返回 -1；否则已有多仓且 score
严格大于 exit_threshold 时继续多仓，已有空仓且 score 严格小于负 exit 时
继续空仓，其余返回 0。signal_for_index() 固定传入 current_position=0，
所以直接逐索引调用不会保留滞后状态；研究服务会优先调用带 current_position
的目标仓位钩子。该模块不缓存分数或信号。

min_holding_bars=3、cooldown_bars=2 不在上述分数公式中执行，而是在研究
回测和监控服务得到策略信号后统一执行。enable_atr_sizing、
risk_fraction、stop_atr_multiple、max_notional_fraction 也不是策略分数的
单位/仓位输出；研究事件回测启用它们时交给通用 AtrRiskSizer 计算敞口，策略
返回的仍只是方向。当前监控通知链不会因此提交真实订单。

#### 辅助周期和外部因子钩子

只有调用方提供 MarketDataContext 时，target_position_for_context() 才会
使用额外输入；三类上下文权重全为零时直接回退到
target_position_for_index()。非零权重时，主周期 score 权重固定为 1.0，
再加入可获得的组件并按总权重归一化：

- daily_trend_weight：从 closed_bars("1d", decision_at) 取已收盘日线，至少
  20 根才加入；末根收盘高于最近 20 根 EMA 为 +1，否则为 -1。
- funding_weight：读取名称为 okx_funding_rate 的最新可用因子，分数为
  -funding.value / funding_rate_scale 后截断到 [-1, 1]；正 funding 会压低
  多头分数。
- open_interest_weight：读取名称为 okx_open_interest 的可用序列，至少两个
  观测且前值非零时计算最新/前值减一，再除以 open_interest_change_scale
  并截断。

decision_at 是当前主 K 线的 datetime + Interval(interval).seconds，即该
根 K 线收盘时刻。MarketDataContext.closed_bars() 只有在
bar.datetime + 周期秒数 <= decision_at 时暴露辅助 K 线；因子同时要求
observed_at <= decision_at 和 available_at <= decision_at。策略只消费上下文，
不知道 Provider、网络或数据库；当前模块也只硬编码上述两个 OKX 衍生品因子
名，不会自动接入任意 FRED/CFTC/其他外部因子。

## 4. 研究、运行时与版本治理接线

### 参数解析和统一信号入口

ResearchService._load_strategy() 按模块名导入 Strategy，并把模块的
DEFAULT_PARAMETERS、DEFAULT_PARAMETER_SPACE 挂到类上供研究使用。
default_parameters() 先取模块默认值，再合并
settings.research.strategy_parameters[strategy_name] 覆盖；旧覆盖不会替换
新默认字典，因此新增的安全字段仍会保留。监控实例的
StrategyVersion.parameters 是显式快照，运行时不会再用当前 UI 默认值覆盖它。

latest_signal() 会合并完整默认参数，按第 1 节的钩子优先级取方向，最后才
应用 trade_mode。辅助周期不是自动拼接到主 bars；没有上下文钩子的策略仍
只看到主序列。

### 事件回测、成本和通用门禁

ResearchService._execute_backtest() 的基本事件顺序是“使用当前已关闭 bar
计算信号，在下一根 bar 的 open 成交”。最后一根持仓在最后收盘平仓；费用、
滑点、半价差和持仓期间资金费都进入逐主周期收益。回测在策略信号之后统一
应用：

- trade_mode 方向过滤；
- min_holding_bars 和 cooldown_bars，反转先平仓、冷却后才能重新开仓；
- 若 enable_atr_sizing，使用通用 AtrRiskSizer 和风险/止损/名义比例字段。

这些规则不是各策略自行实现的；直接调用策略类得到的原始信号不含成交时点、
成本或风控结果。

### 寻优

optimize() 和监控的 optimize_parameters() 使用显式传入的 parameter space，
参数先与 base/default 合并。组合数不超过 1000 时，无论指定何种方法都执行
精确 grid；heuristic/bfs/astar 在组合数不超过 10000 时也转精确 grid。
更大的 heuristic 使用固定随机种子 42 的中心点/最多三个随机点和每维 ±1
邻居，最多评估 100 个节点；其他方法进入遗传搜索，种群为
max(20, 10 * 维度)、15 代、随机种子 42，保留前 20% 父代和前 2 个精英。
默认按 Sharpe 排序，optimize_target="return" 时按总收益排序；零交易组合
以 -999 惩罚。

寻优结果是研究候选，optimize() 会写入 research_runs 和
reports/<strategy>_optimize.{md,json}，不会自动激活版本，也不等于实盘保证。
当前 CM 的默认离散空间就是本项目 XAU 研究的现行空间，不能用旧文档中更窄
的范围代替。

### 消融

factor_ablation(config, variants) 先加载一次相同的主 bars/context，再对每个
显式 overrides 合并配置并回测；它不会为每个 variant 重新寻优，防止把不同
拟合参数伪装成因子消融。该公共方法返回 AblationResult，当前自身不持久化
研究报告；如需纳入审批证据，必须由上层另行保存和审阅。

### Walk-forward 与验证门

walk_forward() 每折只用训练窗口寻优；把完整训练 bars 与测试 bars 拼接给
指标作为 warm-up，但只把测试窗口的转移计入样本外收益。训练最后一根闭合
bar 产生的第一笔样本外决策在第一根测试 bar 开盘成交，避免把测试未来带入
指标。每折写入 walk_forward_folds，聚合结果和报告也写入
research_runs/reports/<strategy>_walk_forward.{md,json}。

validate_candidate() 要求回测和 walk-forward 指向同一 strategy/symbol/
exchange/interval，默认门槛为：回测至少 10 笔交易、至少 3 个走查折、走查
最大回撤不超过 10%、走查总收益大于零。该方法只运行回测和走查，不创建或
检查 shadow；shadow 是后续版本审批的独立门。

### 版本、影子和审批

strategy-create 先通过 _load_strategy() 验证插件，再创建 StrategyInstance
和包含完整默认/显式参数的 StrategyVersion；实例默认 execution_mode=
notify_only，辅助周期写入实例配置。版本是参数/代码版本的研究快照，修改
参数应创建新版本或 clone，不应就地改旧证据。

strategy-approve 要求研究运行已完成、模式分别为 backtest 和 walk-forward，
且与实例版本的 strategy、symbol、interval 匹配；CLI 复核至少 10 笔交易、
至少 3 折、走查正收益和回撤门槛。它还必须收到属于同一 instance/version
且状态为 passed 的 shadow run，不能用布尔标志冒充影子证据。影子通过默认
还需要至少一次权益观测、28 天观察时长和不超过 10% 回撤。

StrategyGovernanceService.approve_activation() 最终要求 backtest、
walk-forward、shadow 三门全通过，并再次检查回撤上限后写入激活记录；
strategy-rollback 重新激活已有版本并写入 rollback_of 审计链。运行时按
最后一根已闭合主 K 线的收盘时刻解析当时已生效的版本，不会因未来激活记录
回溯使用新参数。

### 监控中的策略调用和当前安全边界

monitor_instance_once() 将已生效版本的参数、登记的辅助 bars 和按决策时间
获取的因子传给 monitor_once()；普通 monitor_once() 只处理已收盘主 K 线，
按已处理 bar 时间做幂等，参数优先级为显式参数、闭 K 线上的寻优结果、策略
默认值。之后由监控层执行数据健康、最小持仓、cooldown、风险白名单/数量门禁，
首次 bootstrap 不发送通知和动作。

当前发布实现即使请求的 execution_mode 是 paper 或 live，仍会记录
orders intentionally suppressed，不会调用 order_executor.execute()；动作
只是可观测的拟执行指令，Telegram 通知也不等于已买入/卖出。策略层不应把
有 Strategy、有 OKX adapter 或有版本激活记录描述成已授权真实交易。

## 5. 测试映射

策略和研究契约的主要可执行索引如下：

| 测试 | 覆盖事实 |
|---|---|
| tests/unit/test_cm_macd_strategy.py | CM 多空输出、缓存 fingerprint 变化后重算。 |
| tests/unit/test_indicator_strategies.py | 注册发现顺序、KDJ/RSI 双方向、成交量确认，以及启用策略配置持久化。 |
| tests/unit/test_multi_factor_strategy.py | 多因子预热、趋势/突破分数、entry/exit hysteresis、funding available_at、64 组合预注册 space、旧参数覆盖与新安全默认合并。 |
| tests/unit/test_trade_mode.py | ResearchService 对 CM 原始多空信号执行 both/long-only/short-only 过滤。 |
| tests/unit/test_event_driven_backtest.py | 下一根开盘成交、训练 bars warm-up、逐周期成本、最小持仓/cooldown 和 ATR sizing。 |
| tests/unit/test_multi_timeframe_research.py | 4h 决策不能看到尚未收盘的 1d bar；验证上下文钩子实际接线。 |
| tests/unit/test_data_context_and_factors.py、tests/unit/test_external_factors.py | 辅助 K 线关闭时间、因子 observed/available 点时约束和外部因子归一化边界。 |
| tests/unit/test_optimization_methods.py | grid/heuristic/GA 路径、固定随机行为、CM space、return 目标、精确中等空间和成本影响。 |
| tests/unit/test_factor_ablation.py | 相同 bars 上的显式变体，不重新寻优。 |
| tests/unit/test_research_validation.py、tests/integration/test_research_workflows.py | 验证门、回测/寻优/走查报告、折收益拼接和 XAU CM 候选。 |
| tests/unit/test_strategy_runtime.py、tests/unit/test_strategy_versioning.py | 闭合时间的版本解析、版本 snapshot/clone、按可用时间读取因子。 |
| tests/unit/test_governance.py、tests/unit/test_shadow_runs.py | 三门审批、回滚审计、影子权益/28 天/回撤约束。 |
| tests/unit/test_monitoring_instance.py、tests/unit/test_monitored_target_parameters.py | 实例版本参数、辅助 bars/因子传递和目标级参数覆盖。 |
| tests/integration/test_monitoring.py、tests/unit/test_notify_only.py | 闭 K 幂等、通知/动作状态、最小持仓和当前不调用 executor 的安全边界。 |

修改策略公式、默认值、space/bounds、缓存键、hook 优先级或时间语义时，应先
在对应测试文件补充正向、反向、预热和失败/边界用例，再同步本 Wiki 与受影响
的研究/治理文档。

## 6. 新策略扩展规范

新增策略至少应满足以下约定：

- 新建 src/vntdr/strategies/<strategy_name>.py，提供可导入的
  Strategy(ReviewedStrategyBase)；若希望进入 UI，提供 STRATEGY_LABEL、
  STRATEGY_DESCRIPTION、完整的 DEFAULT_PARAMETERS、有意参与寻优的
  DEFAULT_PARAMETER_SPACE 和给 UI 的 DEFAULT_PARAMETER_BOUNDS。文件放入
  包后会自动被 registry 扫描，无需手工改注册表。
- 让默认参数可独立运行，并写清所有预热、状态、current_position 和
  data_context 假设。trade_mode 应交给 ResearchService 过滤，不在每个策略
  复制一套方向开关；若模块不支持做空，应像 demo 一样明确记录。
- 若缓存整段计算结果，键必须区分参数和 bars 内容，并处理列表身份复用、原地
  修改及不同资产/交易所/周期；不要把不完整 fingerprint 当成通用事实唯一键。
  若使用辅助周期/因子，只接受 MarketDataContext，按决策时刻检查已关闭和
  available_at，不得在策略里直接请求 Provider。
- 对双方向输出、反向退出、预热零值、参数异常、缓存失效/时序泄漏分别补单元
  测试；若实现状态钩子、辅助周期、寻优或版本快照，再补研究服务和治理集成
  测试。测试应使用临时数据库/fake 边界，不读生产凭据。
- 研究结果只能作为候选证据；短窗口、高 Sharpe 或一次寻优胜出不能表述为实盘
  保证。任何改变执行授权的变更都必须同时经过交易安全评审、demo 验证、订单
  幂等/成交确认测试和审批，而不是只新增一个策略模块。

## 7. 已知限制

- cm_macd_ult_mtf 目前只在单一 bars 序列上计算 EMA/MACD；没有完整 MTF 引擎。
  multi_factor 的 1d/funding/open-interest 上下文也只在服务明确传入
  MarketDataContext 且权重非零时生效。
- 基础接口没有统一参数验证；各策略仅做少量 int/float 转换或窗口截断，例如
  不满足 fast < slow 时 CM 会整段归零，其他非法/退化参数可能产生空信号、
  零分或运行时错误。bounds 只是元数据，不是硬门禁。
- KDJ、RSI、volume 的状态序列从传入 bars 的第一根重放；监控的短 lookback
  或手工截断会改变预热和持仓状态。demo 没有缓存，也没有做空信号；多因子
  默认的风险 sizing 字段不改变方向分数。
- 类级缓存没有淘汰，fingerprint 只看首尾/长度，不能检测中间 K 线变更，且不
  完整区分资产身份；这要求调用方保持 bars 隔离和不可变习惯。
- 辅助周期和外部因子虽然有点时过滤，但数据是否完整、是否陈旧、是否存在
  缺口仍由服务层数据健康检查负责；策略层不会自行补齐或验证历史覆盖。
- 当前 live/monitoring 版本只通知和记录拟执行动作，不自动下单；有 OKX
  executor、execution_mode=live 或已审批策略版本都不能越过这一边界。
