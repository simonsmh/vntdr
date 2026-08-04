# `src/vntdr/adapters` 模块 Wiki

本文档以当前代码为准，记录适配器的边界、公开接口、调用时机、状态结构和测试映射。它不包含任何 API、Telegram 或数据库凭据的实际值。

## 先看结论：通知、查询与下单是三条不同的路径

```text
OKX 公共行情
  └─ services.history.OkxHistoryClient(flag="0")
       └─ HistorySyncService → Postgres bars → ResearchService
                                      └─ MonitoringService
                                           ├─ RedisSignalStore：信号/处理进度
                                           ├─ TelegramNotifier：信号变化通知
                                           ├─ OrderInstruction：计划动作（当前不执行）
                                           └─ order_executor：持仓/权益查询、启动时对账

TelegramCommandBot
  ├─ /status：Redis live status + order_executor 持仓查询
  └─ /start：说明与状态按钮
```

`MonitoringService.monitor_once()` 当前明确处于影子/仅通知边界：它可以生成 `buy_long`、`sell_long`、`sell_short`、`buy_short` 这些计划动作并写入 `MonitorResult`/Redis，但不会调用 `order_executor.execute()`。所以 Telegram 中显示“动作”或 `notification_sent=True` 都不代表 OKX 已成交，更不代表存在订单号或成交回报。

`orders.py` 中有交易适配器，但它只有在代码被直接调用，或未来监控执行闸门真正接通后，才会提交订单。配置 `execution_mode="live"` 目前只保留在监控结果/实例语义中，不能绕过 `MonitoringService` 的通知-only 保护。

## 文件边界与 OKX 公共/交易分工

| 文件 | 责任 | 当前不负责什么 |
| --- | --- | --- |
| `orders.py` | OKX Trade/Account SDK 交易、持仓、账户权益；无 key 时的模拟执行器 | 不拉取 K 线；不决定策略信号；不提供下单幂等或成交回报 |
| `state.py` | Redis 信号状态的同步/异步读写 | 不保存订单、持仓或权益；不提供 TTL、事务或 compare-and-set |
| `telegram.py` | 同步发送 Telegram HTML 通知，并在解析失败时退回纯文本 | 不接收命令；不监控；不执行订单 |
| `telegram_bot.py` | `python-telegram-bot` 命令应用、状态面板和（保留中的）排名/监控 job 路径 | 当前不注册交易命令，也没有订单确认/成交回报界面 |
| `__init__.py` | 空模块初始化 | 不导出统一 facade |

适配器目录中没有实际的 OKX 公共行情类。公共行情在 `services/history.py::OkxHistoryClient`：使用 `okx.MarketData.MarketAPI(flag="0", domain=...)`，不需要交易 key；`HistorySyncService.sync()` 负责有限重试、清洗、补缺和写入数据库。`orders.py` 中的 `okx.PublicData` 导入当前未使用，不应据此认为这里已经实现了公共数据适配。

## 数据结构

### `vntdr.models.OrderInstruction`

`orders.py` 和 `monitoring.py` 共用的 Pydantic 模型：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `symbol` | `str` | OKX `instId`，例如某个合约标识 |
| `action` | `str` | 仅支持下表四种动作 |
| `volume` | `float` | 下单数量，发送前格式化为字符串 |
| `reason` | `str` | 计划动作原因；OKX 下单请求不会发送此字段 |

`MonitoringService._build_instructions()` 的换仓顺序可能是先平旧仓、再开新仓；例如多转空为 `sell_long` + `sell_short`。

### `vntdr.models.MonitorResult`

监控和 Telegram 格式化使用的结果字段为 `symbol`、`interval`、`strategy_name`、`signal`、`previous_signal`、`best_parameters`、`actions`、`notification_sent`、`error`、`strategy_version_id`。其中：

- `signal` 使用 `1=LONG`、`-1=SHORT`、`0=空仓`。
- `actions` 是计划动作名称列表，不是已执行动作列表。
- `notification_sent` 只表示 `TelegramNotifier.notify()` 本次没有抛错；不表示订单执行成功。
- 当前监控路径没有调用 executor，因此 `error` 不会承载下单错误；查询/数据/通知错误通常在上层抛出或记录日志。

### OKX 持仓与权益原始结构

`OkxOrderExecutor.get_current_positions()` 原样返回 OKX `data` 中的持仓字典（只保留非零仓位），典型读取字段为 `instId`、`posSide`、`pos`、`avgPx`、`markPx`/`last`、`upl`。`MonitoringService.reconcile_positions()` 只根据 `posSide` 和正的 `pos` 推断 `1/-1/0`，假定策略同一标的同时只持有一个方向。

`get_account_equity()` 读取 `data[0].totalEq` 并转成 `float`，单位按 OKX 账户总权益解释为 USDT。它供 `MonitoringService.update_account_info()` 更新 `RiskManager` 的峰值/当前权益；Telegram `/status` 面板目前只展示持仓，不展示账户权益，Web UI 的 live 状态页另行查询权益。

### Redis key 与 JSON 状态

`RedisSignalStore` 接受任意 key；`MonitoringService` 约定使用：

| key | 值 |
| --- | --- |
| `signal:{symbol}:{interval}:{strategy_name}` | 整数 `-1/0/1`，上次确认信号 |
| `processed_bar_ts:{symbol}:{interval}:{strategy_name}` | 已处理收盘 K 线的 Unix 秒时间戳 |
| `position_opened_bar_ts:{symbol}:{interval}:{strategy_name}` | 开仓信号对应的收盘时间戳；平仓时写 `0` |
| `cooldown_until_bar_ts:{symbol}:{interval}:{strategy_name}` | 冷却截止 Unix 秒时间戳 |
| `vntdr:live_status` | 最新监控 JSON |
| `vntdr:live_statuses` | 按 `{symbol}:{interval}:{strategy_name}` 分组的监控 JSON hash |
| `vntdr:live_logs` | 监控 JSON 日志列表，保留最近 100 条 |

live status JSON 由 `MonitoringService._save_live_status()` 写入，包含 `time`、`heartbeat`、`strategy_name`、`symbol`、`interval`、`signal`、`previous_signal`、`best_parameters`、`actions`、`notification_sent`、`error`、`completed_bar_time`、`skipped_reason`。

`TelegramCommandBot` 的 chat 隔离 key 为：

- `vntdr:rank:last:{chat_id}`：最近一次排名配置与结果，TTL 7 天；
- `vntdr:watch:{chat_id}`：保留中的 WatchConfig，TTL 30 天。

排名依赖的 `services.telegram_research.IntervalResearchResult` 是一个 dataclass，字段为 `interval`、`total_return`、`sharpe_ratio`、`max_drawdown`、`trade_count`、`best_parameters`、`sync_inserted_count`。它表示一个周期的研究/寻优结果，不表示实时信号、持仓或成交。

## `orders.py`：OKX 交易、持仓和权益适配

### 常量与异常

- `TRANSIENT_ORDER_CODES`：`{"50013", "50026", "50004", "50011"}`。这些 code 可表示系统繁忙、超时或限流一类瞬时错误；会同时检查响应顶层 `code` 和首个 `data[0].sCode`。
- `TransientOrderError`：`_place_one()` 判断为瞬时错误时抛出，供 Tenacity 重试。
- `PermanentOrderError`：非成功且不在瞬时集合中的 OKX 拒单，重试没有意义。

### `SimulatedOrderExecutor`

公开接口：

- `execute(instructions: list[OrderInstruction]) -> list[OrderInstruction]`：原样返回传入列表，不访问 OKX，也不改变本地持仓。
- `execute_async(...)`：直接异步包装同步方法。
- `get_current_positions(symbol: str | None = None) -> list[dict[str, Any]]` 及 `_async` 版本：恒返回空列表；当前签名已经接受可选 `symbol`，可被 `MonitoringService.reconcile_positions(symbol=...)` 和 Telegram 状态查询调用。
- `get_account_equity() -> float` 及 `_async` 版本：恒返回 `0.0`。

它是“没有完整交易凭据时不触碰交易 API”的执行器，不是带本地撮合、虚拟余额或持仓账本的 paper broker。因此模拟模式下权益回撤跟踪没有真实账户意义。

### `OkxOrderExecutor`

构造函数 `OkxOrderExecutor(...)` 接受 `api_key`、`secret_key`、`passphrase`、`demo_trading`，以及 `margin_mode`（默认 `cross`）、`order_type`（默认 `market`）、`order_retry_count`（至少 1 次）、`order_retry_wait_seconds`（非负）和可注入的 `trade_api`/`account_api` 测试 double。

- `demo_trading=True` 使 Trade/Account SDK 使用 `flag="1"`；`False` 使用 `flag="0"`。它只选择 OKX API 环境，不等于已经通过上层安全闸门，也不等于成交成功。
- `execute_async()` 把阻塞的 `execute()` 放入内部 `ThreadPoolExecutor(max_workers=4)`。
- `get_current_positions(symbol=None)` 调 `account_api.get_positions()`；顶层 `code` 非 `"0"` 立即抛 `RuntimeError`。仅保留 `avgPx > 0` 且 `pos != 0` 的仓位；提供 `symbol` 时按 `instId` 精确过滤。
- `get_account_equity()` 调 `account_api.get_account_balance()`；顶层 `code` 非 `"0"` 立即抛 `RuntimeError`，否则读取首个 data 项的 `totalEq`。
- 对应的 `_async` 方法同样通过线程池执行阻塞 SDK 调用。

#### 四种动作到 OKX 参数的映射

| `OrderInstruction.action` | `side` | `posSide` | `reduceOnly` | 语义 |
| --- | --- | --- | --- | --- |
| `buy_long` | `buy` | `long` | `false` | 开多 |
| `sell_long` | `sell` | `long` | `true` | 平多 |
| `sell_short` | `sell` | `short` | `false` | 开空 |
| `buy_short` | `buy` | `short` | `true` | 平空 |

`_translate_instruction()` 遇到其他动作抛 `ValueError`。`_format_volume()` 使用 `format(volume, "g")`，例如 `1.0` 发送为 `"1"`。单笔请求固定发送 `instId`、`tdMode`、`side`、`posSide`、`ordType`、`sz`、`reduceOnly`。

#### 重试与错误边界

`_place_one_with_retry()` 只对 `TransientOrderError` 按 `stop_after_attempt(order_retry_count)` 和 `wait_exponential(multiplier=order_retry_wait_seconds, min=...)` 重试，并将最终异常重新抛出。永久错误只调用一次。SDK 直接抛出的网络/解析/连接异常不在这个重试条件内，会直接穿过；没有客户端订单 ID 或幂等键，也没有查询订单确认来消除“请求已被交易所接受但响应丢失”的歧义。

`execute()` 按输入列表顺序逐笔处理：

1. 开仓动作失败（瞬时重试耗尽或永久拒单）立即抛出，不再执行后续指令。
2. 平仓动作失败记录 critical 日志并继续后续指令，以便换仓的开仓腿仍有机会执行；所有指令结束后聚合为一个 `RuntimeError`，消息明确提示仓位可能仍然裸露。
3. 没有平仓失败时才返回原始 `instructions` 列表；返回值同样不是 OKX 成交回报。

这个“平仓继续、末尾报错”的策略降低了因平仓异常而完全跳过后续动作的概率，但不会自动补偿、反查仓位或撤销已成功的开仓腿。直接调用交易适配器前必须由调用方承担重试、对账和告警后的人工处置。

## `state.py`：Redis 信号状态适配

### `RedisSignalStore`

构造函数接收一个 `redis.Redis` 兼容客户端，并创建 `ThreadPoolExecutor(max_workers=4)`。

- `get(key: str) -> int | None`：调用 `client.get()`；非空值转 `int`，因此 Redis 中不是整数的内容会抛 `ValueError`/转换异常。
- `set(key: str, value: int) -> None`：调用 `client.set()`，不设置 TTL。
- `get_async()`、`set_async()`：通过线程池运行对应同步方法，适合监控 async facade。

该类只提供信号/时间戳的薄封装；`MonitoringService._save_live_status()` 通过其公开的 `.client` 属性直接写 hash/list/JSON，所以替换 signal store 时，如果需要 live status 和 Telegram `/status`，还必须兼容 `.client` 的 `set`、`hset`、`lpush`、`ltrim`。读取、写入不是原子事务，也没有版本号或锁；多进程/多 job 同时监控同一 target 时可能发生最后写入覆盖。

## `telegram.py`：出站 Telegram 通知

### `TelegramNotifier`

构造函数 `TelegramNotifier(bot_token: str, chat_id: str)` 保存目标 bot 和 chat；`notify(message: str) -> None` 是同步公开入口。

调用时机由 `MonitoringService.monitor_once()` 决定：仅当不是第一次 bootstrap、已处理的是新收盘 K 线且信号发生变化时，先构造 HTML 消息，再调用 notifier。通知失败只记录错误，不会使 `MonitorResult.notification_sent` 变成 `True`；当前实现不会因通知失败重试或回滚 Redis 信号状态。

发送流程：

1. `POST https://api.telegram.org/bot<token>/sendMessage`，JSON 包含 `chat_id`、`text`、`parse_mode="HTML"`，HTTP timeout 20 秒，并调用 `raise_for_status()`。
2. 只有 `httpx.HTTPStatusError` 才进入 fallback：用正则去掉 HTML 标签，附加“HTML 解析失败”提示和尽量提取的 Telegram error description，再以无 `parse_mode` 的纯文本重发。
3. fallback 仍失败则重新抛出最终异常；其他网络/HTTP 客户端异常直接记录并重新抛出，没有指数退避或异步版本。

`TelegramNotifier` 不负责 HTML 转义；当前上游 `MonitoringService._build_message()` 已对策略名、标的、周期、动作和参数做部分转义。日志会记录要发送的消息正文，调用方不要把凭据或不必要的敏感数据放入正文。

## `telegram_bot.py`：命令应用、状态面板和保留的交互路径

### 数据类

- `WatchConfig`：`symbol`、`strategy_name`、`interval`、`method`、`poll_seconds`。用于 job data 和 Redis 持久化。
- `RankConfig`：`symbol`、`strategy_name`、`method`、`intervals: list[str]`、`lookback_hours`。用于排名请求和最近排名缓存。

### `TelegramCommandBot.__init__()` 依赖与装配

构造函数需要 `bot_token`、`chat_id`、`TelegramResearchService`、`monitor_once_callback`，可选 `ConfigService` 和 Redis 客户端。

- CLI 在 `CommandContext` 中传入 `context.telegram_research()` 和 `context.monitor_once`；由于这是 bound method，bot 会探测到同一 owner 的 `monitor_once_async`，优先走异步 callback。
- `position_provider` 从 callback owner 的 `monitoring_service.order_executor` 取得；如果 callback 不是这种绑定方法，则状态面板没有持仓 provider。
- `chat_id` 强制转为字符串，并形成 `watch:{chat_id}`、`rank:last:{chat_id}` 等隔离 key。

### 当前实际注册的命令与回调

`build_application()` 只向 `Application` 注册以下入口：

| 入口 | 实际行为 | 是否触及交易 |
| --- | --- | --- |
| `/start` | HTML 说明页，说明 bot 只保留信号推送/状态查询，并提供“刷新状态”按钮 | 否 |
| `/status` | 构造 HTML 状态面板：最近 live status（最多 5 条）和当前 OKX/模拟持仓 | 否；只查询 |
| callback `m:status` | 编辑当前消息，刷新同一状态面板 | 否 |
| callback `stop` | 若被外部构造触发，只显示“入口已停用，由 quant_core 主循环负责监控和推送” | 不取消 job，也不下单 |

启动时 `post_init` 只设置 bot 命令菜单 `/start`、`/status`。`_allowed_chat()` 会把 `effective_user.id`、`effective_chat.id` 或消息 chat id 与配置的 `chat_id` 比较，支持字符串和数字比较；不匹配直接拒绝。它同时尝试通过 `ConfigService._load_overrides()` 重新加载覆盖配置。

### 状态面板读取与格式化

- `_load_live_statuses()` 优先读取 Redis hash `vntdr:live_statuses`；按 `heartbeat` 与当前时间比较，默认只接受 15 分钟内的状态，并按 `time` 字符串倒序，最多展示 5 条。只有 hash 完全没有 entry 时才 fallback 到单值 `vntdr:live_status`；hash 有但全部过期时不会再 fallback。
- `_load_current_positions()` 在 executor 所在的线程池中调用 `get_current_positions(None)`，异常全部转为空列表。因此模拟 executor 会显示“无持仓”，OKX 查询失败也会显示“无持仓”，而不是把查询错误展示给用户。
- `_build_status_panel()` 使用 Telegram HTML，展示 symbol、interval、LONG/SHORT/空仓、动作、收盘时间、心跳、`posSide`、数量、均价、标记价和 UPL；不展示总权益。
- `_format_signal()` 映射 `1/-1/0`，未知值原样字符串化；`_format_position_side()` 映射 `long/short/net`。

### 消息安全发送

`_send_safe()` 支持 Update、CallbackQuery、Message 或 chat id 字符串，可发送、编辑或通过 bot 发送。默认参数是 MarkdownV2，但调用方需自己负责转义；排名结果显式使用 `parse_mode=None`，状态面板/开始页使用 HTML。

当 Telegram 返回包含 `Can't parse entities` 的 `BadRequest` 时，它会去掉部分 `*`/反引号并以无 parse mode 重发；其他 `BadRequest` 重新抛出。没有可用的 target 方法或 bot 时可能静默不发送。`_escape_markdown_v2()` 和 `_escape_markdown_v2_code()` 是供格式化调用的静态辅助方法，不会自动套用到所有 `_send_safe()` 文本。

### 保留但当前未注册的旧命令/交互

源码仍在 `build_application()` 内定义 `rank_command`、`run_command`、`auto_command`、`stop_command`，以及 `config_entry`、`config_callback`、`config_fallback_text`、`cancel`；但当前没有添加对应的 `CommandHandler`/`ConversationHandler`。集成测试也明确验证以下命令未注册：

- `/rank`：原计划按多个周期调用 `TelegramResearchService.rank_intervals()`，每个周期同步公共 K 线并优化，缓存排名 7 天。
- `/run`：原计划调用 `_do_monitor()` 做一次监控。
- `/auto`：原计划排名后选择第一名周期，建立重复 job。
- `/stop`：原计划移除重复 job。
- `/config`：原计划通过 `ConfigService.list_all()/get()/set()` 修改配置。

对应的 `r:*`、`a:*`、`rr`、`cfg:*` callback 分支也没有被当前 `CallbackQueryHandler(pattern=r"^(m:status|stop)$")` 接收。不要把这些保留函数当作线上可用命令；当前配置修改入口是 Web UI/共享 override 文件，而不是 Telegram。

### 保留路径的调用细节

若未来重新注册这些 handler：

- `_execute_rank()` 将排名放到线程池，调用 `TelegramResearchService.rank_intervals()`，然后写入 `vntdr:rank:last:{chat_id}`、`context.user_data`，通过 `format_rankings()` 生成文本，并生成前 3 个周期的“运行/自动”按钮。
- `_do_monitor()` 从 `context.bot_data["default_order_size"]` 取 volume，缺失时默认为 `1.0`，再调用异步 `monitor_once_async` 或同步 callback。当前 CLI 装配没有显式给 Telegram application 的 `bot_data` 注入默认下单量，所以默认 fallback 实际可能为 `1.0`。
- `_replace_watch_job()` 先移除同名 job，再以 `poll_seconds` 调 `run_repeating(first=0)`，把 `asdict(WatchConfig)` 放进 job data 并保存 30 天；`_remove_watch_job()` 会同时删除 job、内存配置和 Redis 配置。
- `_build_watch_callback()` 每次先 reload override，执行一次 monitor；只有 `result.actions` 非空才发送格式化结果。callback 没有自己的异常兜底，异常交给 job queue 处理。
- `_load_watch_config()` 有从 Redis 恢复配置的能力，但 `on_startup()` 当前没有调用它或重建 job，因此进程重启不会自动恢复 watch job。

## 与 monitoring、portfolio、config 的交互

### CLI 装配与执行器选择

`cli.py::CommandContext.__init__()` 创建：

1. `TelegramNotifier`（使用 `Settings.telegram`）；
2. `RedisSignalStore(redis_client)`；
3. `MonitoringService(..., notifier=..., order_executor=..., signal_store=..., risk_manager=...)`；
4. `PortfolioRuntimeService(..., monitoring_service=...)`；
5. `TelegramResearchService`。

`CommandContext._build_order_executor()` 的唯一选择条件是 `settings.okx.trading_enabled`：

- `api_key`、`secret_key`、`passphrase` 任一缺失：`SimulatedOrderExecutor`；
- 三者齐全：`OkxOrderExecutor`，并把 `okx.demo_trading` 传给 OKX SDK。

`OkxSettings.trading_enabled` 只检查三项凭据是否存在，不会强制 `demo_trading=True`。`CommandContext.refresh_runtime_config()` 每次重新加载 override；OKX key、demo、域名、保证金模式、订单类型或重试参数变化时重建交易 executor 和公共历史 client。它不会把凭据写入本文档或日志内容。

### `MonitoringService` 的一次监控顺序

由 `CommandContext.monitor_once()` 或 bot 的 bound async callback 触发时，核心步骤是：

1. 从 `RedisSignalStore` 读前一信号和上次已处理收盘 bar。
2. 从 Postgres market repository 取 K 线，只用已完成的 bar；重复处理同一收盘 bar 时只写 skipped live status，不重复通知。
3. 调 `order_executor.get_account_equity()` 更新 `RiskManager` 回撤基线；查询失败只记录 warning，不阻断监控。
4. 缓存没有前一信号时调 `order_executor.get_current_positions(symbol=...)` 对账。对账失败切换到 monitoring-only fallback；首次 bootstrap 会抑制通知和动作。
5. 计算确认信号、数据质量门、最小持仓/冷却规则，经过 `RiskManager.filter_instructions()` 生成 `OrderInstruction` 列表。
6. 信号改变时调用 `TelegramNotifier.notify()`；通知异常被记录，监控仍继续更新状态。
7. 读取 `execution_mode` 仅用于日志/可观测性，然后明确记录“orders intentionally suppressed”；不调用 executor 的 `execute/execute_async`。
8. 写回信号、bar 时间、开仓时间和 `vntdr:live_status*`，返回 `MonitorResult`。

因此 `order_executor` 在当前路径主要承担“账户/持仓事实查询”，不是“订单执行入口”。`RiskManager` 仍会校验允许标的、最大订单量、最大回撤和 `allow_opening_trades`，但这些约束作用于计划动作/通知内容；不应被理解为当前已经有真实下单保护链。

### `PortfolioRuntimeService` / `PortfolioAllocator`

`PortfolioRuntimeService.run_enabled()` 对每个启用的策略实例调用 `MonitoringService.monitor_instance_once()`，把结果转换为 `StrategyDecision`，再交给 `PortfolioAllocator.allocate()` 形成 `PortfolioDecision`（目标权重、gross/net exposure、缩放原因）。它会隔离单实例异常，但 `PortfolioAllocator` 只计算权重，不调用 `order_executor`；当前 portfolio 路径同样是通知/审计目标，不是订单路由器。

### `ConfigService` 与共享配置

`ConfigService` 默认从 `Path.home() / ".vntdr" / "config_override.json"` 读取，覆盖值写回同一 JSON，并原地更新共享的 `Settings` 对象。可配置项包括研究默认策略/标的/周期/数量、寻优和费用参数、`research.execution_mode`、风险上限、`risk.allow_opening_trades` 以及 `okx.api_key`、`okx.secret_key`、`okx.passphrase`、`okx.demo_trading`。SecretStr 的实际值不应进入 Wiki、聊天或普通日志。

Compose 中 webapp 与 quant_core 共同挂载 `config_data` 到 `/root/.vntdr`；Web UI 写入 override 后，quant_core 的 `refresh_runtime_config()` 会重新读取并在 OKX 签名变化时重建客户端。Telegram bot 当前只在 `_allowed_chat()` 和 watch callback 中 reload override，因 `/config` 未注册，它不能通过当前线上命令修改配置。

## 真实交易安全开关与明确边界

要让代码“具备构造真实 OKX client 的条件”，至少需要：

1. 三项 OKX 交易凭据均非空，使 `Settings.okx.trading_enabled=True`；
2. `OKX_DEMO_TRADING=true` 时使用 demo (`flag="1"`)；只有明确改为 `false` 才构造 live (`flag="0"`) client；
3. `OKX_REST_BASE_URL`、保证金模式、订单类型和重试设置经过核对；
4. 风控允许标的/数量，且执行入口经过人工确认。

但当前版本还存在更高优先级的代码边界：`MonitoringService` 无论 `execution_mode` 是 `notify_only`、`paper` 还是 `live`，都不调用 `execute()`。因此：

- Telegram 通知是“策略信号/计划动作”边界，不是下单确认；
- `SimulatedOrderExecutor` 是无凭据 fallback，不会模拟余额或持仓；
- `OkxOrderExecutor` 是可直接调用的交易适配器，`demo_trading=False` + 完整凭据会指向实盘 API，但它不会自行阻止调用；
- 当前 quant_core 监控循环不会通过该适配器真实开仓/平仓；要恢复真实交易必须单独审查并修改 monitoring 执行闸门、幂等/对账和告警流程，不能只改环境变量。

`risk.allow_opening_trades=False`、最大订单量、最大回撤等只限制计划动作；它们与 `OKX_DEMO_TRADING`、凭据开关是不同层次的保护，不能互相替代。

## 测试映射

| 测试 | 覆盖的适配器事实 |
| --- | --- |
| `tests/unit/test_okx_order_executor.py` | 四种动作的 OKX 参数映射；成功/永久拒单；`50013` 瞬时重试；永久错误不重试；开仓失败中止后续；平仓失败继续但末尾聚合报错 |
| `tests/integration/test_monitoring.py` | 信号变化通知、换仓动作名称、同一收盘 bar 只处理一次、最小持仓规则；即使设置 `execution_mode="live"`，fake executor 的 `actions` 仍为空，证明当前 monitoring 不执行订单 |
| `tests/unit/test_notify_only.py` | `Settings.research.execution_mode` 默认是 `notify_only` |
| `tests/unit/test_monitoring_instance.py` | 持久化实例的 `execution_mode` 传递到 `monitor_once`，以及版本/辅助周期装配 |
| `tests/unit/test_telegram_bot_formatting.py` | 监控结果格式、版本/动作/参数展示、chat 访问判断、Redis chat key、排名文本不使用 Markdown parse mode |
| `tests/integration/test_telegram_bot_commands.py` | `/start`、`/status`、`m:status`；明确验证 `/rank`、`/run`、`/auto`、`/config`、`/stop` 未注册；live status 过期过滤；watch job 的替换/删除；持仓面板读取 |
| `tests/integration/test_cli.py` | `vntdr live --once` 只报告 signal/actions；ConfigService 修改 OKX 配置后，runtime client 会热重建且不会重复重建 |
| `tests/unit/test_config.py` | SecretStr/交易凭据判定、共享 Settings 原地更新、override reset |
| `tests/unit/test_portfolio_runtime.py` | 启用实例聚合为决策/目标组合并隔离实例错误；没有验证订单执行，因为 portfolio runtime 不执行订单 |

当前没有专门覆盖 `TelegramNotifier` 真实 HTTP fallback 或 `RedisSignalStore` Redis 异常/并发语义的单元测试；这些边界仍应通过注入 fake client 做回归验证。

## 已知限制清单

1. **真实下单链路未接通。** `OkxOrderExecutor` 的实现和单元测试存在，但 `MonitoringService` 当前抑制所有订单；`MonitorResult.actions` 只是计划动作。
2. **模拟执行器不是 paper 账本。** 持仓恒为空、权益恒为 `0.0`，无法提供成交、余额、手续费或滑点仿真。
3. **交易重试不覆盖所有失败。** 只有指定 OKX 返回码重试；SDK 网络异常不重试，且没有客户端订单 ID/幂等确认。
4. **平仓失败可能留下裸仓。** 适配器会继续执行其他腿并在末尾报错，但不会自动对账、补单或撤销开仓腿。
5. **Telegram 出站通知无重试。** 仅 HTML HTTP 状态失败有一次纯文本 fallback；没有异步发送、退避或失败队列。
6. **Telegram 命令面是简化版。** 当前线上只有 `/start`、`/status`；旧的排名、单次监控、自动监控和配置会话代码未注册。watch 配置也不会在 bot 启动时自动恢复。
7. **状态查询会吞掉持仓异常。** `/status` 查询持仓失败与确实无持仓都显示“无持仓”；它还不显示账户权益。
8. **访问控制只有单一 ID。** `_allowed_chat()` 将用户 ID、聊天 ID、消息聊天 ID 任一匹配即放行，没有角色、命令级权限或群组成员策略。
9. **Redis 状态不是事务。** `RedisSignalStore` 不设置 TTL、不做原子读改写；`live_statuses` 有 entry 但全部过期时不会 fallback 到单值状态，且 Telegram 15 分钟 TTL 与 Web UI 的在线阈值并不一致。
10. **线程池生命周期未显式关闭。** `OkxOrderExecutor`、`RedisSignalStore`、Telegram/历史服务均创建线程池，但 adapters 没有公开 shutdown 方法；长期重载/测试进程需留意资源回收。
