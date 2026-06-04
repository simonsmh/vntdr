# vntdr 项目总览

## 验收标准

使用CM\_MacD\_Ult\_MTF脚本回测，24小时内，XAU-USDT-SWAP寻找最优方案，并持续监控买点和卖点，通过Telegram通知用户，同时操作下单买入卖出多单和空单。

---

## 当前进度（2026-06-02）

### 已完成

**基础设施**
- Docker Compose 四容器栈：`db`(Postgres)、`cache`(Redis)、`quant_core`(监控)、`webapp`(Gradio)
- CI：push 到 main 自动构建发布到 `ghcr.io/simonsmh/vntdr:latest`
- nginx 反代：`https://aiapi.simonsmh.cc` → webapp:7860（与 `*.simonsmh.cc` 共用 ZeroSSL 通配符证书）
- `stack.env` 已修正：交易对 `XAU-USDT-SWAP`、周期 `4H`、Telegram 凭据已填入

**回测引擎** (`src/vntdr/services/research.py`)
- 单次回测、参数寻优（grid/ga）、走查回测三种模式
- 指标：总收益率、年化夏普、最大回撤、交易次数、胜率、盈亏比
- 手续费按 taker 费率 0.05% 计算（可在设置页切换 maker）

**Gradio 回测平台** (`src/vntdr/webapp.py`, `vntdr gradio`)
- 「回测」标签：K线蜡烛图 + 买卖信号 + MACD指标（上下双栏，Plotly）
- 「参数寻优」标签：网格/遗传搜索，Top结果表
- 「走查回测」标签：各折资金曲线
- 「设置」标签：研究参数 + 风控参数，可在线修改并持久化
- 顶部「从 OKX 拉取数据」：公共行情接口，无需 API key
- 全中文界面
- 端口 7860，nginx 绑定 `aiapi.simonsmh.cc`

**Telegram 机器人** (`src/vntdr/adapters/telegram_bot.py`)
- `/config` 命令：在线修改配置（与 Gradio 设置页共享同一个 `ConfigService`）
- `/rank` 命令：多周期排名扫描
- `/watch` 命令：自动监控最佳周期并通知
- 设置覆盖文件 `~/.vntdr/config_override.json` 通过 Docker volume `config_data` 在 webapp 和 quant_core 之间共享

**下单执行** (`src/vntdr/adapters/orders.py`)
- `OkxOrderExecutor`：支持 demo/live，瞬时错误自动重试（指数退避）
- `SimulatedOrderExecutor`：无 key 时的模拟执行器
- 持仓/权益对账（`monitoring.py` 中 `reconcile_positions`）

**数据管线** (`src/vntdr/services/history.py`)
- OKX 公共 K线接口拉取 → `clean_bars` 去重补缺 → 写入 Postgres
- 目前 DB 中有约 185 根 XAU-USDT-SWAP 4h K线（2026-05 至 2026-06）

---

### 策略调参结论

**问题**：默认参数 MACD(4,8,3,3) 在 4h 图上过快，181根K线产生62笔交易，其中38笔（61%）持仓 ≤2 根K线，属噪音交易。

**对比实验**（2026-05-01 ~ 2026-06-01，XAU-USDT-SWAP 4h）：

| 配置 | 交易 | 收益 | 回撤 | 夏普 | 噪音(≤2根) | 平均持仓 |
|---|---|---|---|---|---|---|
| 旧 (4,8,3,3) | 62 | 2.1% | 4.1% | 4.21 | 38笔 | 2.9根 |
| **新 (6,21,3,7)** | **26** | **2.7%** | **3.4%** | 3.29 | **6笔** | **6.0根** |

**结论**：MACD(6,21,3,7) 噪音降低84%，收益反而更高，回撤更低。已更新为默认参数，搜索空间调整为 `fast=[4,6,8], slow=[13,17,21], signal=[3,5,7], tw=[5,7]`。

---

### 待完成 / 已知问题

- **OKX 交易 key 失效**：`stack.env` 中 `OKX_API_KEY` 为空，quant_core 实际用 `SimulatedOrderExecutor`，不会真实下单。需要填入有效 key 并设置 `OKX_DEMO_TRADING=true` 才能开始真实交易。
- **数据量不足**：DB 仅约185根 4h K线（约1个月），走查回测折数有限。需要更长历史数据（建议至少6个月）来做可靠的参数寻优和走查验证。
- **`SimulatedOrderExecutor.get_current_positions()` 不接受 `symbol` 参数**：启动时 reconcile 会报 TypeError（已被 try/except 捕获，不影响运行，但需要修复接口）。
- **stack.env 里有真实 Telegram 凭据**：本地测试用，提交前应清空回模板值。
- **webapp 和 quant_core 的设置页共享**：已通过 `config_data` volume 实现，但 quant_core 的主循环每轮都会重新 `_load_overrides()`，所以设置页的改动会实时生效。

---

## 架构速查

```
aiapi.simonsmh.cc (nginx)
    ↓ proxy_pass
  webapp (vntdr gradio, :7860)
    ↓ 共享 DB + config_data volume
  quant_core (vntdr live)
    ↓
  Postgres (db:5432)  Redis (cache:6379)  OKX API
```

**策略文件**：`src/vntdr/strategies/cm_macd_ult_mtf.py`（MACD 多空）、`demo_momentum.py`（测试用）
**配置热更新**：`ConfigService` → `~/.vntdr/config_override.json`，webapp 设置页 / Telegram `/config` 均可修改
**回测数据流**：OKX 公共 K线 → `clean_bars` → Postgres `bars` 表 → `ResearchService.backtest()`

---

## 开发备忘

- `uv lock` 后需重新 `docker build`（`uv.lock` 是 frozen 模式）
- 容器内 DB 地址：`db:5432`（docker 网络），主机访问用容器桥接 IP `172.26.0.3`
- OKX 公共行情接口无需 API key，`flag="0"` 即可拉取
- `vnpy` 要求 `plotly>=6`，不要降级到 5
- 域名证书：ZeroSSL 通配符 `*.simonsmh.cc`，2026-08-06 到期
