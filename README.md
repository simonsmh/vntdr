# vntdr

Research-first quantitative trading foundation built around VeighNa, OKX, PostgreSQL, and Redis.

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
