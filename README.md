# vntdr

Research-first quantitative trading foundation built around VeighNa, OKX, PostgreSQL, and Redis.

## AkShare A股资金流 MVP

项目现在使用 AkShare 作为 A 股研究数据源，不依赖 iFinD 账号。可以先用单标的验证公开接口：

```bash
uv run vntdr akshare-csi300-flow \
  --symbol 515180 --market sh \
  --from 2026-07-01 --to 2026-07-27 \
  --max-retries 5 --retry-backoff-seconds 1
```

不传 `--symbol` 时，命令会拉取当前沪深300成分并汇总主力、大单净流入趋势；MVP
小样本可增加 `--max-stocks 10`。结果会写入 `VNTDR_REPORT_DIR`（默认 `reports/`）的
日汇总 CSV、个股汇总 CSV 和 JSON 摘要。

该链路是研究用途：AkShare 封装的是公开网页数据接口，可能遭遇远端限流、连接中断、
字段变更或历史回补。输出中的 `source_note` 会保留这一限制，数据不进入 OKX 下单链路。
请求失败会自动使用指数退避和随机抖动重试；批量抓取默认每只标的间隔 0.8 秒，
可通过 `--request-interval`、`--max-retries`、`--retry-backoff-seconds` 和
`--retry-jitter-seconds` 调整。

### ETF 观察池定时入库

默认观察池为：`588200`、`510300`、`588170`、`588080`、`159845`、`159941`、`512400`。
一次采集会将最近约 120 个自然日的可用资金流幂等写入 PostgreSQL；可用
`VNTDR_ETF_WATCHLIST=588200:sh,510300:sh,159845:sz` 覆盖观察池。

手动执行一次：

```bash
uv run vntdr etf-flow-ingest --max-retries 5
```

启动 APScheduler 常驻任务（工作日 16:10，Asia/Shanghai）：

```bash
uv run vntdr etf-flow-scheduler
```

任务级失败会被标记为 `retryable`，默认 60 秒后重试，之后指数退避，最长等待 30 分钟，
直到整批观察池成功；这与单次 HTTP 请求的有限重试是两层机制。若只执行
`--run-once`，命令会在任务仍为 `retryable` 时返回非零退出码，不会在前台无限阻塞。

验证数据库、重试和写入链路但不启动常驻进程：

```bash
uv run vntdr etf-flow-scheduler --run-once --request-interval 0
```

原始规范化数据存放在 `etf_money_flow_daily`，每次任务的成功、失败和重试情况存放在
`etf_flow_ingestion_runs`。`available_at` 按交易日收盘后的发布时间建模，`fetched_at`
保留真实抓取时间，避免历史补采把未来数据泄漏进回测。生产环境启动前需要执行
`alembic upgrade head`。

Gradio 面板中的「📊 ETF资金流」标签页直接读取这两张表，提供观察池趋势摘要、主力净流入折线图、
日频明细和最近任务审计。点击「刷新数据库视图」不会访问外部数据；「立即采集并入库」只触发一次
有界采集，失败会在任务表中显示 `retryable`，后续仍由独立 `etf_ingest` APScheduler 服务继续重试。
因此 Web UI 不承担常驻调度职责，也不会把采集失败误显示成成功。

For stack deployments such as Portainer, the compose file expects a committed `stack.env` template next to `docker-compose.yml`. Fill or override those values in the deployment environment before starting `vntdr live`.

## TradingView 研究代理数据

项目包含一个基于 TradingView 网页 WebSocket 协议的非官方历史数据适配器，协议行为参考
MIT 许可的 [rongardF/tvdatafeed](https://github.com/rongardF/tvdatafeed)。该接口不是
TradingView 官方支持的数据 API，可能随网页协议、权限或限流策略变化，只应用于因子研究。

匿名拉取 OANDA 黄金 4 小时数据：

```bash
uv run vntdr sync-tradingview \
  --tv-symbol OANDA:XAUUSD \
  --output-symbol TV:XAUUSD \
  --interval 4h \
  --start 2024-01-01T00:00:00+00:00 \
  --end 2026-01-01T00:00:00+00:00
```

需要登录权限的品种可通过环境变量 `TRADINGVIEW_AUTH_TOKEN` 传入浏览器会话令牌。输出标的
强制使用 `TV:` 前缀，交易所固定为 `TRADINGVIEW`，不会与 OKX 可成交行情混用。

## 影子运行与版本审批

策略版本只能在通知模式下完成可审计的影子观察后才能审批。启动影子运行并定期记录标记权益：

```bash
uv run vntdr shadow-start --instance-id <INSTANCE_ID> --version-id <VERSION_ID>
uv run vntdr shadow-record-equity --shadow-run-id <SHADOW_RUN_ID> --equity 1.03
```

`shadow-finish --status passed` 会强制要求至少 28 天、至少一条权益观测且最大回撤不超过 10%。
审批还会核验影子运行绑定的是同一个实例与版本：

```bash
uv run vntdr strategy-approve \
  --instance-id <INSTANCE_ID> --version-id <VERSION_ID> --approved-by <REVIEWER> \
  --backtest-run-id <BACKTEST_RUN_ID> --walk-forward-run-id <WALK_FORWARD_RUN_ID> \
  --shadow-run-id <SHADOW_RUN_ID>
```

当前发布保持 `notify_only`：审批只允许策略进入通知/影子执行路径，不会提交真实订单。

## 因子消融与固定候选验证

消融在同一数据窗口执行明确变体，不重新寻优：

```bash
uv run vntdr ablate-strategy --strategy multi_factor --symbol TV:XAUUSD \
  --exchange TRADINGVIEW --interval 4h --from 2024-07-01 --to 2026-07-24 \
  --variant 'baseline={}' --variant 'no_trend={"trend_weight": 0}'
```

由消融发现的候选必须在走查中固定，而不是根据每折重新拟合：

```bash
uv run vntdr validate-strategy --strategy multi_factor --symbol TV:XAUUSD \
  --exchange TRADINGVIEW --interval 4h --from 2024-07-01 --to 2026-07-24 \
  --train-window 720 --test-window 240 --fixed-parameters-json '{"trend_weight": 0}'
```
