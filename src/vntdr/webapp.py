"""Gradio backtest explorer for vntdr — 全中文界面，K线+MACD指标图。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import gradio as gr
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from vntdr.config import Settings
from vntdr.models import ResearchJobConfig
from vntdr.services.config_service import ConfigService
from vntdr.services.history import OkxHistoryClient, HistorySyncService
from vntdr.services.research import ResearchService
from vntdr.storage.database import Database
from vntdr.storage.repositories import MarketDataRepository, ResearchRunRepository


# ── 工具函数 ──────────────────────────────────────────────────────────

def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _default_dates(cs=None):
    end = _utcnow()
    lookback_hours = 720
    if cs is not None:
        try:
            val = cs.get("research.default_rank_lookback_hours")
            if val is not None:
                lookback_hours = int(val)
        except Exception:
            pass
    start = end - timedelta(hours=lookback_hours)
    return start, end


def _parse_datetime(val: Any, is_end: bool = False) -> datetime:
    if isinstance(val, datetime):
        dt = val.replace(tzinfo=None)
    elif not val:
        dt = _utcnow()
    else:
        val_str = str(val).strip()
        if " " in val_str:
            val_str = val_str.replace(" ", "T")
        try:
            dt = datetime.fromisoformat(val_str)
            dt = dt.replace(tzinfo=None)
        except Exception:
            from dateutil import parser
            dt = parser.parse(val_str)
            dt = dt.replace(tzinfo=None)

    if is_end:
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
            dt = dt.replace(hour=23, minute=59, second=59)
    return dt


def _parse_params(text: str) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for line in text.strip().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            try:
                params[key] = int(value)
            except ValueError:
                try:
                    params[key] = float(value)
                except ValueError:
                    params[key] = value
    return params


def _parse_space_value(v: Any) -> list[Any]:
    if not isinstance(v, str):
        return [v]
    
    val_str = v.strip()
    
    # 1. Parse range syntax like "2~10", "2-10", "2~10:2", "2~10 step 2"
    import re
    step = 1.0
    
    # Match step formats: "step X" or ":X" or "/X" at the end
    step_match = re.search(r'(?:step|:|\/)\s*([+-]?\d*(?:\.\d+)?)', val_str, re.IGNORECASE)
    if step_match:
        try:
            step = float(step_match.group(1))
            range_part = val_str[:step_match.start()].strip()
        except ValueError:
            range_part = val_str
    else:
        range_part = val_str
        
    parts = re.split(r'~|to|(?<=\d)-(?=\d)', range_part, flags=re.IGNORECASE)
    if len(parts) == 2:
        try:
            start = float(parts[0].strip())
            end = float(parts[1].strip())
            
            results = []
            current = start
            if step > 0:
                while current <= end + 1e-9:
                    results.append(int(current) if current.is_integer() else current)
                    current += step
            elif step < 0:
                while current >= end - 1e-9:
                    results.append(int(current) if current.is_integer() else current)
                    current += step
            if results:
                return results
        except ValueError:
            pass
            
    # 2. Fallback to comma split
    results = []
    for x in val_str.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            val = float(x)
            results.append(int(val) if val.is_integer() else val)
        except ValueError:
            results.append(x)
    return results


def _ema(values: list[float], length: int) -> list[float]:
    """EMA 计算（与 cm_macd_ult_mtf 策略保持一致）。"""
    alpha = 2.0 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


METRIC_ZH = {
    "Total Return":     "总收益率",
    "Sharpe Ratio":     "夏普比率",
    "Max Drawdown":     "最大回撤",
    "Trade Count":      "交易次数",
    "Win Rate":         "胜率",
    "Profit Factor":    "盈亏比",
}


def _metrics_df(metrics: dict[str, float]) -> pd.DataFrame:
    rows = []
    for k, v in metrics.items():
        label = k.replace("_", " ").title()
        label = METRIC_ZH.get(label, label)
        rows.append((label, v))
    return pd.DataFrame(rows, columns=["指标", "数值"])


def _params_df(params: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(params.items(), columns=["参数", "值"])


PARAM_LABELS = {
    "fast_length":   "快线周期",
    "slow_length":   "慢线周期",
    "signal_length": "信号线周期",
    "trend_window":  "趋势窗口",
    "lookback":      "回看周期",
}


# ── 策略参数定义 ──────────────────────────────────────────────────────

STRATEGY_PARAMS: dict[str, dict[str, Any]] = {
    "demo_momentum": {
        "defaults": {"lookback": 3},
        "space": {"lookback": "2~5"},
        "bounds": {"lookback": "1~20"},
    },
    "cm_macd_ult_mtf": {
        "defaults": {"fast_length": 6, "slow_length": 21, "signal_length": 3, "trend_window": 7},
        "space": {
            "fast_length":   "2~12:2",
            "slow_length":   "10~30:5",
            "signal_length": "3~9:2",
            "trend_window":  "3~9:2",
        },
        "bounds": {
            "fast_length":   "2~30",
            "slow_length":   "10~80",
            "signal_length": "2~25",
            "trend_window":  "2~40",
        },
    },
}


def _default_space_text(strategy_name: str) -> str:
    sp = STRATEGY_PARAMS.get(strategy_name, {}).get("space", {})
    lines = []
    for k, v in sp.items():
        if isinstance(v, list):
            lines.append(f"{k}={','.join(str(x) for x in v)}")
        else:
            lines.append(f"{k}={v}")
    return "\n".join(lines)


def _params_line(p: dict[str, Any]) -> str:
    return "  ".join(
        f"{PARAM_LABELS.get(k, k)}={v}" for k, v in p.items()
    )


# ── 服务初始化（模块级懒加载）────────────────────────────────────────

_RESEARCH: ResearchService | None = None
_HISTORY:  HistorySyncService | None = None
_MDR:      MarketDataRepository | None = None


def _init_services():
    cs = _get_config_service()
    settings = cs.settings
    database = Database(settings.database.dsn)
    database.create_schema()
    mdr = MarketDataRepository(database)
    rrr = ResearchRunRepository(database)
    research = ResearchService(
        settings=settings, market_data_repository=mdr,
        research_run_repository=rrr,
    )
    history = HistorySyncService(
        settings=settings,
        history_client=OkxHistoryClient(
            base_url=settings.okx.rest_base_url,
            demo_trading=settings.okx.demo_trading,
        ),
        market_data_repository=mdr, research_run_repository=rrr,
    )
    return research, history, mdr


def _get_services():
    global _RESEARCH, _HISTORY, _MDR
    if _RESEARCH is None:
        _RESEARCH, _HISTORY, _MDR = _init_services()
    return _RESEARCH, _HISTORY, _MDR


_CONFIG_SERVICE: ConfigService | None = None


def _get_config_service():
    global _CONFIG_SERVICE
    if _CONFIG_SERVICE is None:
        settings = Settings.from_env()
        _CONFIG_SERVICE = ConfigService(settings)
    return _CONFIG_SERVICE


def _get_targets_df_and_choices():
    cs = _get_config_service()
    targets = cs.get("research.monitored_targets") or []
    rows = []
    for idx, t in enumerate(targets):
        strat = t.get("strategy_name", "")
        current_params = cs.get("research.strategy_parameters") or {}
        p = current_params.get(strat, {})
        if not p:
            p = STRATEGY_PARAMS.get(strat, {}).get("defaults", {})
        params_str = ", ".join(f"{k}={v}" for k, v in p.items())
        rows.append([
            idx + 1,
            strat,
            t.get("symbol", ""),
            t.get("interval", ""),
            t.get("volume", 1.0),
            params_str
        ])
    df = pd.DataFrame(rows, columns=["序号", "策略", "交易对", "周期", "下单量", "策略参数"])
    choices = [f"{t['symbol']} ({t['interval']} - {t['strategy_name']})" for t in targets]
    val = choices[-1] if choices else None
    return df, choices, val


# ── K线 + MACD 指标图 ────────────────────────────────────────────────

def _build_kline_macd_chart(bars, signals, fast_length, slow_length, signal_length):
    """生成 K线蜡烛图 + 买卖信号 + MACD 指标（上下两栏）。"""
    dts    = [b.datetime for b in bars]
    opens  = [b.open    for b in bars]
    highs  = [b.high    for b in bars]
    lows   = [b.low     for b in bars]
    closes = [b.close   for b in bars]

    # MACD 计算（与 cm_macd_ult_mtf 策略保持一致）
    fast_ema  = _ema(closes, fast_length)
    slow_ema  = _ema(closes, slow_length)
    macd_line = [f - s for f, s in zip(fast_ema, slow_ema)]
    sig_line  = _ema(macd_line, signal_length)
    histogram = [m - s for m, s in zip(macd_line, sig_line)]

    # 买卖信号点
    buy_x, buy_y = [], []  # 开多
    sell_x, sell_y = [], []  # 开空
    close_long_x, close_long_y = [], []  # 平多
    close_short_x, close_short_y = [], []  # 平空
    
    prev_pos = 0
    for i, sig in enumerate(signals):
        if sig != prev_pos:
            if sig == 1:
                buy_x.append(bars[i].datetime)
                buy_y.append(bars[i].low)
            elif sig == -1:
                sell_x.append(bars[i].datetime)
                sell_y.append(bars[i].high)
            elif sig == 0:
                if prev_pos == 1:
                    close_long_x.append(bars[i].datetime)
                    close_long_y.append(bars[i].high)
                elif prev_pos == -1:
                    close_short_x.append(bars[i].datetime)
                    close_short_y.append(bars[i].low)
            prev_pos = sig

    # 创建上下两栏子图（K线占3份，MACD占1份）
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[3, 1], vertical_spacing=0.03,
    )

    # ── 上栏：K线蜡烛图 ────────────────────────────────────────
    fig.add_trace(
        go.Candlestick(
            x=dts, open=opens, high=highs, low=lows, close=closes,
            name="K线",
            increasing_line_color="#00C853",
            decreasing_line_color="#FF1744",
            increasing_fillcolor="#00C853",
            decreasing_fillcolor="#FF1744",
        ),
        row=1, col=1,
    )

    # 买卖信号标记（三角形/圆形，放在K线低/高点位置更醒目）
    if buy_x:
        fig.add_trace(
            go.Scatter(
                x=buy_x, y=buy_y, mode="markers", name="开多 📈",
                marker=dict(symbol="triangle-up", size=13, color="#00E676",
                            line=dict(width=1, color="#fff")),
            ),
            row=1, col=1,
        )
    if sell_x:
        fig.add_trace(
            go.Scatter(
                x=sell_x, y=sell_y, mode="markers", name="开空 📉",
                marker=dict(symbol="triangle-down", size=13, color="#FF5252",
                            line=dict(width=1, color="#fff")),
            ),
            row=1, col=1,
        )
    if close_long_x:
        fig.add_trace(
            go.Scatter(
                x=close_long_x, y=close_long_y, mode="markers", name="平多 ❌",
                marker=dict(symbol="circle", size=10, color="#E040FB",
                            line=dict(width=1, color="#fff")),
            ),
            row=1, col=1,
        )
    if close_short_x:
        fig.add_trace(
            go.Scatter(
                x=close_short_x, y=close_short_y, mode="markers", name="平空 ❌",
                marker=dict(symbol="circle", size=10, color="#00E5FF",
                            line=dict(width=1, color="#fff")),
            ),
            row=1, col=1,
        )

    # ── 下栏：MACD 柱状图 + MACD线 + 信号线 ─────────────────────
    hist_colors = ["#00C853" if v >= 0 else "#FF1744" for v in histogram]
    fig.add_trace(
        go.Bar(
            x=dts, y=histogram, name="MACD柱",
            marker_color=hist_colors, opacity=0.75,
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dts, y=macd_line, mode="lines", name="MACD",
            line=dict(width=1.2, color="#FF9800"),
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dts, y=sig_line, mode="lines", name="信号线",
            line=dict(width=1.2, color="#AB47BC"),
        ),
        row=2, col=1,
    )

    fig.update_layout(
        title="K线走势  ·  买卖信号  ·  MACD指标",
        height=680,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=55, r=20, t=60, b=30),
    )
    fig.update_yaxes(title_text="价格", row=1, col=1)
    fig.update_yaxes(title_text="MACD", row=2, col=1)
    return fig


# ── Gradio App ────────────────────────────────────────────────────────

def main(port: int = 7860) -> None:
    cs = _get_config_service()
    cs._load_overrides()
    default_start, default_end = _default_dates(cs)

    initial_strategy = cs.get("research.default_strategy") or "cm_macd_ult_mtf"
    initial_symbol = cs.get("research.default_symbol") or "XAU-USDT-SWAP"
    initial_interval = cs.get("research.default_interval") or "4h"
    if isinstance(initial_interval, str):
        initial_interval = initial_interval.lower()

    with gr.Blocks(
        title="vntdr 量化工作站",
        theme=gr.themes.Soft(),
    ) as app:

        gr.Markdown("# 📊 vntdr 量化工作站")

        main_tabs = gr.Tabs()
        with main_tabs:
            # ── Tab 1: 🔬 策略研究工作流 ───────────────────────
            with gr.Tab("🔬 策略研究工作流", id="tab_workflow"):
                with gr.Row():
                    # ── 左侧：主视图（总是展示图表与数据）──
                    with gr.Column(scale=3):
                        bt_chart = gr.Plot(label="K线 · 信号 · MACD指标")
                        
                        visual_tabs = gr.Tabs()
                        with visual_tabs:
                            with gr.Tab("📈 回测指标与交易记录", id="visual_backtest"):
                                with gr.Row():
                                    bt_metrics_table = gr.Dataframe(
                                        headers=["指标", "数值"], label="回测指标", interactive=False,
                                    )
                                    bt_params_table = gr.Dataframe(
                                        headers=["参数", "值"], label="使用参数", interactive=False,
                                    )
                                bt_trades_table = gr.Dataframe(
                                    headers=["时间", "价格", "动作", "仓位"],
                                    label="单次回测交易记录（买多/买空/平仓等）",
                                    interactive=False,
                                )
                                
                            with gr.Tab("⚡ 参数寻优结果 (辅助调参)", id="visual_optimize"):
                                with gr.Row():
                                    opt_select_combo = gr.Dropdown(
                                        label="选择要应用的寻优参数组合 (Top 5)",
                                        choices=[("暂无寻优结果，请先运行寻优", -1)],
                                        value=-1,
                                        interactive=True,
                                        scale=3,
                                    )
                                    opt_apply_btn = gr.Button("🎯 应用所选参数组合", variant="secondary", scale=1)
                                opt_top_table = gr.Dataframe(
                                    headers=["参数组合", "极值夏普", "最高收益"],
                                    label="Top 结果列表", interactive=False,
                                )
                                with gr.Row():
                                    opt_metrics_table = gr.Dataframe(
                                        headers=["指标", "数值"], label="最优指标", interactive=False,
                                    )
                                    opt_params_table = gr.Dataframe(
                                        headers=["参数", "值"], label="最优参数", interactive=False,
                                    )
                                opt_best_params = gr.State(value={})
                                opt_top_results = gr.State(value=[])
                                
                            with gr.Tab("🏁 走查测试结果", id="visual_walk_forward"):
                                wf_folds_plot = gr.Plot(label="样本外总收益曲线")
                                with gr.Row():
                                    wf_metrics_table = gr.Dataframe(
                                        headers=["指标", "数值"], label="样本外汇总指标", interactive=False,
                                    )
                                    wf_params_table = gr.Dataframe(
                                        headers=["参数", "值"], label="最新折参数", interactive=False,
                                    )
                                wf_folds_table = gr.Dataframe(
                                    headers=["折数", "夏普", "收益率", "参数"],
                                    label="各折参数明细", interactive=False,
                                )
                                wf_trades_table = gr.Dataframe(
                                    headers=["时间", "价格", "动作", "仓位"],
                                    label="样本外交易记录（买点卖点）", interactive=False,
                                )

                    # ── 右侧：控制与执行面板 ──
                    with gr.Column(scale=1):
                        gr.Markdown("### 🛠️ 市场与行情配置")
                        global_strategy = gr.Dropdown(
                            label="研究策略",
                            choices=list(STRATEGY_PARAMS.keys()),
                            value=initial_strategy,
                        )
                        global_symbol = gr.Dropdown(
                            label="交易对 (Symbol)",
                            choices=["XAU-USDT-SWAP", "QQQ-USDT-SWAP", "BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                            value=initial_symbol,
                            allow_custom_value=True,
                        )
                        global_interval = gr.Dropdown(
                            label="K线周期",
                            choices=["1m","3m","5m","15m","30m","1h","4h","1d"],
                            value=initial_interval,
                        )
                        with gr.Row():
                            global_start = gr.DateTime(label="开始时间", value=default_start, type="datetime", include_time=False, interactive=True)
                            global_end = gr.DateTime(label="结束时间", value=default_end, type="datetime", include_time=False, interactive=True)
                        with gr.Row():
                            global_sync_btn = gr.Button("🔄 同步 OKX 行情数据", variant="secondary")
                        global_sync_status = gr.Textbox(label="数据同步状态", interactive=False)

                        with gr.Accordion("⚙️ 策略参数与微调 (Backtest)", open=True):
                            bt_params_lookback = gr.Textbox(
                                label="demo_momentum 参数",
                                value="lookback=3",
                                visible=False,
                                lines=3,
                            )
                            bt_params_macd = gr.Textbox(
                                label="cm_macd_ult_mtf 参数",
                                value="fast_length=6\nslow_length=21\nsignal_length=3\ntrend_window=7",
                                visible=True,
                                lines=4,
                            )
                            bt_run_btn = gr.Button("▶️ 运行策略回测", variant="primary")
                            bt_status = gr.Textbox(label="回测状态", interactive=False)

                        with gr.Accordion("⚡ 辅助调参寻优 (Parameter Optimization)", open=False):
                            opt_auto_fit = gr.Checkbox(
                                label="自动范围拟合", value=False,
                            )
                            opt_space = gr.Textbox(
                                label="参数搜索空间（每行 key=val1,val2,val3）",
                                value=_default_space_text("cm_macd_ult_mtf"),
                                lines=5,
                            )
                            opt_run_btn = gr.Button("⚡ 运行参数寻优", variant="primary")
                            opt_apply_best_btn = gr.Button("🎯 将最优参数填入回测", variant="secondary")
                            opt_status = gr.Textbox(label="寻优状态", interactive=False)

                        with gr.Accordion("🏁 样本外走查测试 (Walk-forward)", open=False):
                            with gr.Row():
                                wf_train = gr.Number(label="训练窗口 (K线数)", value=60, precision=0)
                                wf_test  = gr.Number(label="测试窗口 (K线数)", value=20, precision=0)
                            wf_auto_fit = gr.Checkbox(label="走查自动范围拟合", value=False)
                            wf_run_btn = gr.Button("🏁 运行走查回测", variant="primary")
                            wf_status = gr.Textbox(label="走查状态", interactive=False)

                        with gr.Accordion("🚀 监控部署与管理 (CRUD)", open=True):
                            manage_table = gr.Dataframe(
                                headers=["序号", "策略", "交易对", "周期", "下单量", "策略参数"],
                                label="当前激活的监控列表",
                                interactive=False,
                            )
                            with gr.Row():
                                manage_select_target = gr.Dropdown(
                                    label="选择已有监控进行编辑/删除",
                                    choices=[],
                                    interactive=True,
                                    scale=3,
                                )
                                manage_autofill_btn = gr.Button("📋 填入当前回测配置", variant="secondary", scale=2)

                            with gr.Row():
                                manage_strategy = gr.Dropdown(
                                    label="策略 (Strategy)",
                                    choices=list(STRATEGY_PARAMS.keys()),
                                    value="cm_macd_ult_mtf",
                                    interactive=True,
                                )
                                manage_symbol = gr.Dropdown(
                                    label="交易对 (Symbol)",
                                    choices=["XAU-USDT-SWAP", "QQQ-USDT-SWAP", "BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                                    value="XAU-USDT-SWAP",
                                    allow_custom_value=True,
                                    interactive=True,
                                )
                            with gr.Row():
                                manage_interval = gr.Dropdown(
                                    label="K线周期 (Interval)",
                                    choices=["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"],
                                    value="4h",
                                    interactive=True,
                                )
                                manage_volume = gr.Number(
                                    label="下单量 (Volume)",
                                    value=1.0,
                                    interactive=True,
                                )
                            manage_params = gr.Textbox(
                                label="策略运行参数 (每行 key=val)",
                                value="fast_length=6\nslow_length=21\nsignal_length=3\ntrend_window=7",
                                lines=4,
                                interactive=True,
                            )
                            with gr.Row():
                                manage_add_btn = gr.Button("➕ 添加/部署监控", variant="primary")
                                manage_update_btn = gr.Button("✏️ 更新选中监控", variant="secondary")
                                manage_delete_btn = gr.Button("🗑️ 删除选中监控", variant="stop")
                            manage_status = gr.Textbox(label="操作状态", interactive=False)

            # ── Tab 2: 🟢 实盘监控看板 ─────────────────────────
            with gr.Tab("🟢 实盘监控看板", id="tab_live"):
                with gr.Row():
                    with gr.Column(scale=1):
                        live_health = gr.Markdown("### 🔍 正在获取监控状态...")
                        live_config = gr.Dataframe(
                            headers=["交易对", "周期", "策略", "最新信号", "上次信号", "最新动作", "运行状态", "最后更新时间"],
                            label="实时多币种监控中心",
                            interactive=False,
                        )
                    with gr.Column(scale=1):
                        live_account = gr.Markdown("### 🔍 正在获取账户资金...")
                        live_positions = gr.Dataframe(
                            headers=["合约/交易对", "方向", "持仓大小", "开仓均价", "未实现盈亏"],
                            label="当前持仓明细",
                            interactive=False,
                        )
                with gr.Row():
                    live_logs_table = gr.Dataframe(
                        headers=["时间", "策略", "交易对", "周期", "信号", "执行动作", "通知发送", "错误/状态"],
                        label="实盘通知与操作记录历史 (最新 50 条)",
                        interactive=False,
                    )
                with gr.Row():
                    live_refresh_btn = gr.Button("🔄 刷新监控状态", variant="primary")

                with gr.Accordion("🛠️ 管理监控目标 (Manage Monitored Targets)", open=True):
                    with gr.Row():
                        add_symbol = gr.Dropdown(
                            label="交易对 (Symbol)",
                            choices=["XAU-USDT-SWAP", "QQQ-USDT-SWAP", "BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                            value="QQQ-USDT-SWAP",
                            allow_custom_value=True,
                            scale=2,
                        )
                        add_interval = gr.Dropdown(
                            label="K线周期",
                            choices=["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"],
                            value="4h",
                            scale=1,
                        )
                        add_strategy = gr.Dropdown(
                            label="策略",
                            choices=list(STRATEGY_PARAMS.keys()),
                            value="cm_macd_ult_mtf",
                            scale=2,
                        )
                        add_volume = gr.Number(
                            label="下单量",
                            value=1.0,
                            scale=1,
                        )
                    with gr.Row():
                        btn_add_target = gr.Button("➕ 添加监控目标", variant="primary", scale=1)
                        remove_target_select = gr.Dropdown(
                            label="选择要移除的监控目标",
                            choices=[],
                            interactive=True,
                            scale=2,
                        )
                        btn_remove_target = gr.Button("🗑️ 移除选中的监控目标", variant="stop", scale=1)
                    manage_status = gr.Textbox(label="操作状态", interactive=False)

            # ── Tab 3: 设置 ────────────────────────────────────
            with gr.Tab("系统设置", id="tab_settings"):
                cfg_status = gr.Textbox(label="状态", interactive=False)

                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### ⚙️ 研究与默认参数")
                        cfg_strategy = gr.Dropdown(
                            label="默认策略",
                            choices=list(STRATEGY_PARAMS.keys()),
                        )
                        cfg_symbol = gr.Dropdown(
                            label="默认交易对",
                            choices=["XAU-USDT-SWAP", "QQQ-USDT-SWAP", "BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                            allow_custom_value=True,
                        )
                        cfg_interval = gr.Dropdown(
                            label="默认周期",
                            choices=["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"],
                        )
                        cfg_order_size = gr.Number(label="默认下单量",       precision=2)
                        cfg_rank_hours = gr.Number(label="回测默认回看小时数",   precision=0)
                        cfg_maker_fee = gr.Number(label="Maker 费率",       precision=6)
                        cfg_taker_fee = gr.Number(label="Taker 费率",       precision=6)
                        cfg_use_maker = gr.Checkbox(label="使用 Maker 费率")
                        cfg_optimize_target = gr.Dropdown(
                            label="寻优打分排序指标",
                            choices=[("夏普比率 (Sharpe)", "sharpe"), ("总收益率 (Return)", "return")],
                        )
                        cfg_trade_mode = gr.Dropdown(
                            label="交易模式 (多空/仅多/仅空)",
                            choices=[("多空双开 (Both)", "both"), ("只做多仓 (Long Only)", "long_only"), ("只做空仓 (Short Only)", "short_only")],
                        )

                    with gr.Column():
                        gr.Markdown("### 🛡️ 风控参数与限制")
                        cfg_max_capital = gr.Number(label="单策略最大资金", precision=4)
                        cfg_max_exposure = gr.Number(label="最大总敞口",     precision=4)
                        cfg_max_drawdown = gr.Number(label="最大回撤限制",   precision=4)
                        cfg_max_order = gr.Number(label="最大下单量",     precision=2)
                        cfg_allow_open = gr.Checkbox(label="允许开仓")

                        gr.Markdown("### 🔑 OKX API 配置")
                        cfg_okx_key = gr.Textbox(label="OKX API Key", type="password")
                        cfg_okx_secret = gr.Textbox(label="OKX Secret Key", type="password")
                        cfg_okx_passphrase = gr.Textbox(label="OKX Passphrase", type="password")
                        cfg_okx_demo = gr.Checkbox(label="Demo 模拟交易")

                with gr.Row():
                    cfg_save_btn  = gr.Button("💾 保存当前配置", variant="primary")
                    cfg_reload_btn = gr.Button("🔄 重新加载配置")
                    cfg_reset_btn = gr.Button("🗑️ 重置全部配置", variant="stop")

        # ── 事件处理 ──────────────────────────────────────────────

        def run_fetch_from_okx(symbol, interval, start, end):
            try:
                _, history, mdr = _get_services()
                if not start or not end:
                    return "请输入开始和结束日期"
                start_dt = _parse_datetime(start)
                end_dt   = _parse_datetime(end, is_end=True)
                if start_dt >= end_dt:
                    return "开始日期必须早于结束日期"
                history.sync(
                    symbol=symbol, interval=interval,
                    start=start_dt, end=end_dt, fill_missing=False,
                )
                count = len(mdr.fetch_bars(symbol, interval, start_dt, end_dt))
                return f"已同步 {count} 根K线 — {symbol} ({interval})"
            except Exception as e:
                return f"错误：{e}"

        def run_backtest(strategy_name, symbol, interval, start, end, params_text):
            try:
                ctx, _, _ = _get_services()
                if not start or not end:
                    return "请先输入日期", None, None, None, None
                parameters = _parse_params(params_text)
                config = ResearchJobConfig(
                    strategy_name=strategy_name, symbol=symbol,
                    interval=interval,
                    start=_parse_datetime(start),
                    end=_parse_datetime(end, is_end=True),
                    parameters=parameters,
                )
                bars = ctx._load_bars(config)
                if not bars:
                    return "没有K线数据，请先点击「从 OKX 拉取数据」", None, None, None, None
                report  = ctx.backtest(config)
                outcome = ctx._execute_backtest(bars, strategy_name, parameters)

                # MACD 参数（带默认值回退）
                defaults = STRATEGY_PARAMS.get(strategy_name, {}).get("defaults", {})
                fl = int(parameters.get("fast_length",   defaults.get("fast_length",   4)))
                sl = int(parameters.get("slow_length",   defaults.get("slow_length",   8)))
                sg = int(parameters.get("signal_length", defaults.get("signal_length", 3)))

                chart = _build_kline_macd_chart(
                    bars[: len(outcome.signals)],
                    outcome.signals, fl, sl, sg,
                )

                trades_records = []
                prev_pos = 0
                for idx, sig in enumerate(outcome.signals):
                    if sig != prev_pos:
                        action = ""
                        if sig == 1:
                            action = "买入开多" if prev_pos == 0 else "平空开多"
                        elif sig == -1:
                            action = "卖出开空" if prev_pos == 0 else "平多开空"
                        elif sig == 0:
                            action = "平多" if prev_pos == 1 else "平空"
                        
                        trades_records.append({
                            "时间": bars[idx].datetime.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bars[idx].datetime, "strftime") else str(bars[idx].datetime),
                            "价格": f"{bars[idx].close:.2f}",
                            "动作": action,
                            "仓位": f"{sig}",
                        })
                        prev_pos = sig
                trades_records.reverse()
                trades_df = pd.DataFrame(trades_records, columns=["时间", "价格", "动作", "仓位"])

                trades = int(report.metrics.get("trade_count", 0))
                return (
                    f"完成 — {len(bars)} 根K线，{trades} 笔交易",
                    _metrics_df(report.metrics),
                    _params_df({PARAM_LABELS.get(k, k): v for k, v in report.best_parameters.items()}),
                    chart,
                    trades_df,
                )
            except Exception as e:
                return f"错误：{e}", None, None, None, None

        def run_optimize(strategy_name, symbol, interval, start, end, space_text, auto_fit):
            try:
                ctx, _, _ = _get_services()
                if not start or not end:
                    return "请先输入日期", None, None, None, {}
                
                method = "ga"
                if auto_fit:
                    bounds = STRATEGY_PARAMS.get(strategy_name, {}).get("bounds", {})
                    parameter_space = {k: _parse_space_value(v) for k, v in bounds.items()}
                else:
                    space_raw = _parse_params(space_text)
                    parameter_space = {}
                    for k, v in space_raw.items():
                        parameter_space[k] = _parse_space_value(v)
                config = ResearchJobConfig(
                    strategy_name=strategy_name, symbol=symbol,
                    interval=interval,
                    start=_parse_datetime(start),
                    end=_parse_datetime(end, is_end=True),
                    mode="optimize",
                    parameter_space=parameter_space,
                    optimize_target=ctx.settings.research.optimize_target,
                )
                report = ctx.optimize(config, method=method)

                top_rows = [
                    [
                        _params_line(r),
                        r.get("sharpe_ratio", r.get("score", "") if ctx.settings.research.optimize_target != "return" else ""),
                        r.get("total_return", "")
                    ]
                    for r in report.top_results
                ]
                return (
                    f"最优夏普：{report.metrics.get('sharpe_ratio', 0):.4f}" if ctx.settings.research.optimize_target != "return" else f"最优收益率：{report.metrics.get('total_return', 0):.2%}",
                    _metrics_df(report.metrics),
                    _params_df({PARAM_LABELS.get(k, k): v for k, v in report.best_parameters.items()}),
                    pd.DataFrame(top_rows, columns=["参数组合", "夏普", "收益率"]),
                    report.best_parameters,
                    report.top_results,
                )
            except Exception as e:
                return f"错误：{e}", None, None, None, {}, []

        def run_walk_forward(
            strategy_name, symbol, interval, start, end,
            space_text, train_window, test_window, auto_fit,
        ):
            try:
                ctx, _, _ = _get_services()
                if not start or not end:
                    return "请先输入日期", None, None, None, None, None
                
                method = "ga"
                
                if auto_fit:
                    bounds = STRATEGY_PARAMS.get(strategy_name, {}).get("bounds", {})
                    parameter_space = {k: _parse_space_value(v) for k, v in bounds.items()}
                else:
                    space_raw = _parse_params(space_text)
                    parameter_space = {}
                    for k, v in space_raw.items():
                        parameter_space[k] = _parse_space_value(v)
                config = ResearchJobConfig(
                    strategy_name=strategy_name, symbol=symbol,
                    interval=interval,
                    start=_parse_datetime(start),
                    end=_parse_datetime(end, is_end=True),
                    mode="walk-forward",
                    method=method,
                    parameter_space=parameter_space,
                    train_window=int(train_window),
                    test_window=int(test_window),
                    optimize_target=ctx.settings.research.optimize_target,
                )
                bars = ctx._load_bars(config)
                if not bars:
                    return "没有K线数据", None, None, None, None, None
                report = ctx.walk_forward(config)

                params_df = (
                    _params_df({PARAM_LABELS.get(k, k): v for k, v in report.best_parameters.items()})
                    if report.best_parameters
                    else pd.DataFrame(columns=["参数", "值"])
                )

                fold_rows = [
                    [
                        f.fold_index,
                        f.metrics.get("sharpe_ratio", 0),
                        f.metrics.get("total_return", 0),
                        _params_line(f.parameters),
                    ]
                    for f in report.fold_results
                ]
                folds_df = pd.DataFrame(fold_rows, columns=["折数", "夏普", "收益率", "参数"])

                # ── 重建样本外连续执行数据 ──
                oos_bars = []
                oos_signals = []
                oos_equity = []
                current_equity_scale = 1.0
                step_returns = []

                for fold in report.fold_results:
                    fold_test_bars = [b for b in bars if fold.test_start <= b.datetime <= fold.test_end]
                    if not fold_test_bars:
                        continue
                    
                    outcome = ctx._execute_backtest(fold_test_bars, strategy_name, fold.parameters)
                    
                    oos_bars.extend(fold_test_bars[: len(outcome.signals)])
                    oos_signals.extend(outcome.signals)
                    
                    scaled_equity = [v * current_equity_scale for v in outcome.equity_curve]
                    if oos_equity:
                        oos_equity.extend(scaled_equity[1:])
                    else:
                        oos_equity.extend(scaled_equity)
                    current_equity_scale = oos_equity[-1]

                for idx in range(len(oos_equity) - 1):
                    step_returns.append((oos_equity[idx + 1] / oos_equity[idx]) - 1)

                trades_records = []
                prev_pos = 0
                trade_count = 0
                for idx in range(len(oos_signals)):
                    sig = oos_signals[idx]
                    if sig != prev_pos:
                        action = ""
                        if sig == 1:
                            action = "买入开多" if prev_pos == 0 else "平空开多"
                        elif sig == -1:
                            action = "卖出开空" if prev_pos == 0 else "平多开空"
                        elif sig == 0:
                            action = "平多" if prev_pos == 1 else "平空"
                        
                        trades_records.append({
                            "时间": oos_bars[idx].datetime.strftime("%Y-%m-%d %H:%M:%S"),
                            "价格": f"{oos_bars[idx].close:.2f}",
                            "动作": action,
                            "仓位": f"{sig}",
                        })
                        trade_count += 1
                        prev_pos = sig

                trades_df = pd.DataFrame(trades_records, columns=["时间", "价格", "动作", "仓位"])

                overall_metrics = ctx._metrics_from_returns(step_returns, oos_equity, trade_count, interval)
                metrics_df = _metrics_df(overall_metrics)

                fig = go.Figure()
                fig.update_layout(template="plotly_dark")
                if oos_bars and oos_equity:
                    oos_dts = [b.datetime for b in oos_bars[: len(oos_equity)]]
                    fig.add_trace(
                        go.Scatter(
                            x=oos_dts, y=oos_equity,
                            mode="lines", name="样本外总收益曲线",
                            line=dict(color="#00E676", width=2),
                        )
                    )
                fig.update_layout(
                    title="样本外总收益曲线 (Stitched Out-of-Sample)",
                    xaxis_title="日期", yaxis_title="权益",
                    height=400, template="plotly_dark",
                    margin=dict(l=50, r=20, t=50, b=30),
                )

                sharpe = overall_metrics.get("sharpe_ratio", 0)
                ret    = overall_metrics.get("total_return", 0)
                status = f"走查完成 — {len(report.fold_results)} 折，样本外总夏普={sharpe:.4f}，总收益率={ret:.2%}"
                return status, metrics_df, params_df, folds_df, fig, trades_df
            except Exception as e:
                return f"错误：{e}", None, None, None, None, None

        def fetch_live_status():
            import redis
            import json
            from datetime import datetime, timezone
            from vntdr.adapters.orders import OkxOrderExecutor, SimulatedOrderExecutor
            
            cs = _get_config_service()
            settings = cs.settings
            
            health_text = "### 🔴 监控离线 (Offline)\n\n未检测到监控心跳，`quant_core` 可能未启动或正在重启。"
            status_df = pd.DataFrame(columns=["交易对", "周期", "策略", "最新信号", "上次信号", "最新动作", "运行状态", "最后更新时间"])
            logs_df = pd.DataFrame(columns=["时间", "策略", "交易对", "周期", "信号", "执行动作", "通知发送", "错误/状态"])
            
            try:
                r_client = redis.from_url(settings.redis.url)
                raw_statuses = r_client.hgetall("vntdr:live_statuses")
                targets = settings.research.monitored_targets or []
                
                monitor_rows = []
                latest_heartbeat = 0.0
                latest_time_str = "无"
                
                for t in targets:
                    sym = t.get("symbol")
                    inv = t.get("interval")
                    strat = t.get("strategy_name")
                    
                    raw_entry = raw_statuses.get(f"{sym}:{inv}:{strat}".encode("utf-8"))
                    if not raw_entry:
                        raw_entry = raw_statuses.get(f"{sym}:{inv.lower()}:{strat}".encode("utf-8"))
                    if not raw_entry:
                        raw_entry = raw_statuses.get(f"{sym}:{inv.upper()}:{strat}".encode("utf-8"))
                        
                    if raw_entry:
                        entry = json.loads(raw_entry.decode("utf-8") if isinstance(raw_entry, bytes) else raw_entry)
                        sig_val = entry.get("signal", 0)
                        sig_text = "多头 📈" if sig_val == 1 else ("空头 📉" if sig_val == -1 else "空仓 💤")
                        prev_sig_val = entry.get("previous_signal", 0)
                        prev_sig_text = "多头" if prev_sig_val == 1 else ("空头" if prev_sig_val == -1 else "空仓" if prev_sig_val == 0 else "无")
                        
                        actions = entry.get("actions", [])
                        action_text = ", ".join(actions) if actions else "无"
                        
                        err = entry.get("error")
                        err_text = "✅ 正常" if not err else f"❌ 错误: {err}"
                        
                        time_str = entry.get("time", "")
                        hb = entry.get("heartbeat", 0.0)
                        if hb > latest_heartbeat:
                            latest_heartbeat = hb
                            latest_time_str = time_str
                            
                        monitor_rows.append([
                            sym, inv, strat, sig_text, prev_sig_text, action_text, err_text, time_str
                        ])
                    else:
                        monitor_rows.append([
                            sym, inv, strat, "未知 ❓", "未知 ❓", "无", "⏳ 暂无数据", "未运行/初始化中"
                        ])
                
                if monitor_rows:
                    status_df = pd.DataFrame(monitor_rows, columns=["交易对", "周期", "策略", "最新信号", "上次信号", "最新动作", "运行状态", "最后更新时间"])
                
                if latest_heartbeat > 0:
                    now_ts = datetime.now(timezone.utc).timestamp()
                    if now_ts - latest_heartbeat < 90:
                        health_text = f"### 🟢 监控在线 (Running)\n\n- **心跳状态**: 正常\n- **最后更新**: {latest_time_str}"
                    else:
                        health_text = f"### 🟡 监控无响应 (Stale)\n\n- **心跳延迟**: 已超时 ({(now_ts - latest_heartbeat):.1f} 秒前)\n- **最后活跃**: {latest_time_str}"
                
                raw_logs = r_client.lrange("vntdr:live_logs", 0, 49)
                log_rows = []
                for raw in raw_logs:
                    entry = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                    s_val = entry.get("signal", 0)
                    s_text = "多 (1)" if s_val == 1 else ("空 (-1)" if s_val == -1 else "空仓 (0)")
                    log_rows.append([
                        entry.get("time", ""),
                        entry.get("strategy_name", ""),
                        entry.get("symbol", ""),
                        entry.get("interval", ""),
                        s_text,
                        ", ".join(entry.get("actions", [])) or "无动作",
                        "✅ 成功" if entry.get("notification_sent") else "❌ 未发送",
                        entry.get("error") or "正常",
                    ])
                if log_rows:
                    logs_df = pd.DataFrame(log_rows, columns=["时间", "策略", "交易对", "周期", "信号", "执行动作", "通知发送", "错误/状态"])
            except Exception as e:
                health_text = f"### 🔴 Redis 状态读取失败\n\n- **错误原因**: {e}"
                
            account_text = "### 💳 OKX 资金与权益\n\n"
            positions_df = pd.DataFrame(columns=["合约/交易对", "方向", "持仓大小", "开仓均价", "未实现盈亏"])
            
            try:
                if settings.okx.api_key and settings.okx.secret_key:
                    executor = OkxOrderExecutor(
                        api_key=settings.okx.api_key.get_secret_value() if settings.okx.api_key else "",
                        secret_key=settings.okx.secret_key.get_secret_value() if settings.okx.secret_key else "",
                        passphrase=settings.okx.passphrase.get_secret_value() if settings.okx.passphrase else "",
                        demo_trading=settings.okx.demo_trading,
                    )
                    
                    try:
                        equity = executor.get_account_equity()
                        demo_label = " (Demo 模拟盘)" if settings.okx.demo_trading else " (Live 实盘)"
                        account_text += f"- **账户净权益**: **{equity:.2f} USDT** {demo_label}\n"
                    except Exception as eq_err:
                        err_str = str(eq_err)
                        if "50119" in err_str or "api key doesn't exist" in err_str.lower():
                            account_text += "- **账户权益**: ❌ 查询失败 (OKX API Key 不存在或已失效，请在系统设置中重新配置)\n"
                        else:
                            account_text += f"- **账户权益**: 查询失败 ({eq_err})\n"
                        
                    try:
                        raw_positions = executor.get_current_positions()
                        pos_rows = []
                        for p in raw_positions:
                            side_text = "多头 🟢" if p.get("posSide") == "long" else "空头 🔴"
                            pos_rows.append([
                                p.get("instId", ""),
                                side_text,
                                p.get("pos", "0"),
                                p.get("avgPx", "0"),
                                f"{float(p.get('upl', '0')):.2f} USDT",
                            ])
                        if pos_rows:
                            positions_df = pd.DataFrame(pos_rows, columns=["合约/交易对", "方向", "持仓大小", "开仓均价", "未实现盈亏"])
                        else:
                            account_text += "- **当前持仓**: 暂无持仓"
                    except Exception as pos_err:
                        err_str = str(pos_err)
                        if "50119" in err_str or "api key doesn't exist" in err_str.lower():
                            account_text += "- **当前持仓**: ❌ 查询失败 (OKX API Key 不存在或已失效，请在系统设置中重新配置)\n"
                        else:
                            account_text += f"- **当前持仓**: 查询失败 ({pos_err})\n"
                else:
                    account_text += "🔴 **OKX API 密钥未配置**\n\n系统目前在无 API 密钥的模拟测试模式下运行。"
            except Exception as e:
                account_text += f"🔴 **账户/持仓读取失败**: {e}"
                
            return health_text, status_df, account_text, positions_df, logs_df

        def update_space_text(strategy_name):
            return _default_space_text(strategy_name)

        def update_param_visibility(strategy_name):
            show_lookback = strategy_name == "demo_momentum"
            show_macd     = strategy_name == "cm_macd_ult_mtf"
            return gr.update(visible=show_lookback), gr.update(visible=show_macd)

        def toggle_space_visibility(auto_fit):
            return gr.update(visible=not auto_fit)

        # ── 绑定事件 ──────────────────────────────────────────────

        global_sync_btn.click(
            run_fetch_from_okx,
            inputs=[global_symbol, global_interval, global_start, global_end],
            outputs=[global_sync_status],
        )

        def run_backtest_dispatch(strategy_name, symbol, interval, start, end, params_lookback, params_macd):
            params_text = params_lookback if strategy_name == "demo_momentum" else params_macd
            status, metrics, params, chart, trades_df = run_backtest(strategy_name, symbol, interval, start, end, params_text)
            return status, metrics, params, chart, trades_df, gr.update(selected="visual_backtest")

        bt_run_btn.click(
            run_backtest_dispatch,
            inputs=[
                global_strategy, global_symbol, global_interval, global_start, global_end,
                bt_params_lookback, bt_params_macd,
            ],
            outputs=[bt_status, bt_metrics_table, bt_params_table, bt_chart, bt_trades_table, visual_tabs],
        )

        def manage_add_target(strategy, symbol, interval, volume, params_text):
            try:
                if not symbol or not interval or not strategy:
                    df, choices, val = _get_targets_df_and_choices()
                    return "⚠️ 请完整填写策略、交易对和周期！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                cs = _get_config_service()
                targets = cs.get("research.monitored_targets") or []
                
                # Check if duplicate
                for t in targets:
                    if t.get("symbol") == symbol and t.get("interval") == interval.lower() and t.get("strategy_name") == strategy:
                        df, choices, val = _get_targets_df_and_choices()
                        return f"⚠️ 监控目标 {symbol} ({interval} - {strategy}) 已存在！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                # Append target
                targets.append({
                    "strategy_name": strategy,
                    "symbol": symbol,
                    "interval": interval.lower(),
                    "volume": float(volume)
                })
                cs.set("research.monitored_targets", targets)
                
                # Save strategy parameters from manage_params
                params = {}
                for line in params_text.split("\n"):
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        if v.lower() == "true":
                            params[k] = True
                        elif v.lower() == "false":
                            params[k] = False
                        else:
                            try:
                                if "." in v:
                                    params[k] = float(v)
                                else:
                                    params[k] = int(v)
                            except ValueError:
                                params[k] = v
                
                current_params = cs.get("research.strategy_parameters") or {}
                if not isinstance(current_params, dict):
                    current_params = {}
                current_params[strategy] = params
                cs.set("research.strategy_parameters", current_params)
                
                df, choices, val = _get_targets_df_and_choices()
                return (
                    f"✅ 成功添加并部署监控目标 {symbol} ({interval})！",
                    df,
                    gr.update(choices=choices, value=val),
                    gr.update(choices=choices, value=val),
                )
            except Exception as e:
                df, choices, val = _get_targets_df_and_choices()
                return f"❌ 添加监控目标失败: {e}", df, gr.update(choices=choices), gr.update(choices=choices)

        def manage_update_target(selected_str, strategy, symbol, interval, volume, params_text):
            try:
                if not selected_str:
                    df, choices, val = _get_targets_df_and_choices()
                    return "⚠️ 请先选择一个要更新的监控目标！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                if not symbol or not interval or not strategy:
                    df, choices, val = _get_targets_df_and_choices()
                    return "⚠️ 策略、交易对和周期不能为空！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                cs = _get_config_service()
                targets = cs.get("research.monitored_targets") or []
                
                # Find the index of the selected target
                found_idx = -1
                for idx, t in enumerate(targets):
                    match_str = f"{t['symbol']} ({t['interval']} - {t['strategy_name']})"
                    if match_str == selected_str:
                        found_idx = idx
                        break
                
                if found_idx == -1:
                    df, choices, val = _get_targets_df_and_choices()
                    return f"⚠️ 未找到选中的监控目标 {selected_str}！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                # Check for duplicates if changing fields
                for idx, t in enumerate(targets):
                    if idx != found_idx:
                        if t.get("symbol") == symbol and t.get("interval") == interval.lower() and t.get("strategy_name") == strategy:
                            df, choices, val = _get_targets_df_and_choices()
                            return f"⚠️ 更新失败：另一个监控目标 {symbol} ({interval} - {strategy}) 已存在！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                # Update fields
                targets[found_idx] = {
                    "strategy_name": strategy,
                    "symbol": symbol,
                    "interval": interval.lower(),
                    "volume": float(volume)
                }
                cs.set("research.monitored_targets", targets)
                
                # Save strategy parameters from manage_params
                params = {}
                for line in params_text.split("\n"):
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        if v.lower() == "true":
                            params[k] = True
                        elif v.lower() == "false":
                            params[k] = False
                        else:
                            try:
                                if "." in v:
                                    params[k] = float(v)
                                else:
                                    params[k] = int(v)
                            except ValueError:
                                params[k] = v
                
                current_params = cs.get("research.strategy_parameters") or {}
                if not isinstance(current_params, dict):
                    current_params = {}
                current_params[strategy] = params
                cs.set("research.strategy_parameters", current_params)
                
                df, choices, val = _get_targets_df_and_choices()
                new_val = choices[found_idx] if found_idx < len(choices) else val
                return (
                    f"✅ 成功更新监控目标为 {symbol} ({interval})！",
                    df,
                    gr.update(choices=choices, value=new_val),
                    gr.update(choices=choices, value=new_val),
                )
            except Exception as e:
                df, choices, val = _get_targets_df_and_choices()
                return f"❌ 更新监控目标失败: {e}", df, gr.update(choices=choices), gr.update(choices=choices)

        def manage_delete_target(selected_str):
            try:
                if not selected_str:
                    df, choices, val = _get_targets_df_and_choices()
                    return "⚠️ 请先选择一个要删除的监控目标！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                cs = _get_config_service()
                targets = cs.get("research.monitored_targets") or []
                
                # Find the index of the selected target
                found_idx = -1
                for idx, t in enumerate(targets):
                    match_str = f"{t['symbol']} ({t['interval']} - {t['strategy_name']})"
                    if match_str == selected_str:
                        found_idx = idx
                        break
                
                if found_idx == -1:
                    df, choices, val = _get_targets_df_and_choices()
                    return f"⚠️ 未找到选中的监控目标 {selected_str}！", df, gr.update(choices=choices, value=val), gr.update(choices=choices, value=val)
                
                # Remove
                targets.pop(found_idx)
                cs.set("research.monitored_targets", targets)
                
                df, choices, val = _get_targets_df_and_choices()
                return (
                    f"✅ 成功删除监控目标 {selected_str}！",
                    df,
                    gr.update(choices=choices, value=val),
                    gr.update(choices=choices, value=val),
                )
            except Exception as e:
                df, choices, val = _get_targets_df_and_choices()
                return f"❌ 删除监控目标失败: {e}", df, gr.update(choices=choices), gr.update(choices=choices)

        def on_select_target_change(selected_str):
            if not selected_str:
                return gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
            try:
                cs = _get_config_service()
                targets = cs.get("research.monitored_targets") or []
                for t in targets:
                    match_str = f"{t['symbol']} ({t['interval']} - {t['strategy_name']})"
                    if match_str == selected_str:
                        strat = t.get("strategy_name")
                        current_params = cs.get("research.strategy_parameters") or {}
                        p = current_params.get(strat, {})
                        if not p:
                            p = STRATEGY_PARAMS.get(strat, {}).get("defaults", {})
                        params_text = "\n".join(f"{k}={v}" for k, v in p.items())
                        return (
                            gr.update(value=strat),
                            gr.update(value=t.get("symbol")),
                            gr.update(value=t.get("interval")),
                            gr.update(value=t.get("volume", 1.0)),
                            gr.update(value=params_text)
                        )
            except Exception as e:
                pass
            return gr.update(), gr.update(), gr.update(), gr.update(), gr.update()

        def autofill_from_backtest(strategy, symbol, interval, params_lookback, params_macd):
            params_text = params_lookback if strategy == "demo_momentum" else params_macd
            return (
                gr.update(value=strategy),
                gr.update(value=symbol),
                gr.update(value=interval),
                gr.update(value=1.0),
                gr.update(value=params_text)
            )

        def on_manage_strategy_change(strategy_name):
            try:
                cs = _get_config_service()
                current_params = cs.get("research.strategy_parameters") or {}
                p = current_params.get(strategy_name, {})
                if not p:
                    p = STRATEGY_PARAMS.get(strategy_name, {}).get("defaults", {})
                params_text = "\n".join(f"{k}={v}" for k, v in p.items())
                return gr.update(value=params_text)
            except Exception:
                return gr.update()

        manage_select_target.change(
            on_select_target_change,
            inputs=[manage_select_target],
            outputs=[manage_strategy, manage_symbol, manage_interval, manage_volume, manage_params]
        )

        manage_strategy.change(
            on_manage_strategy_change,
            inputs=[manage_strategy],
            outputs=[manage_params]
        )

        manage_autofill_btn.click(
            autofill_from_backtest,
            inputs=[global_strategy, global_symbol, global_interval, bt_params_lookback, bt_params_macd],
            outputs=[manage_strategy, manage_symbol, manage_interval, manage_volume, manage_params]
        )

        manage_add_btn.click(
            manage_add_target,
            inputs=[manage_strategy, manage_symbol, manage_interval, manage_volume, manage_params],
            outputs=[manage_status, manage_table, manage_select_target, remove_target_select]
        ).then(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        )

        manage_update_btn.click(
            manage_update_target,
            inputs=[manage_select_target, manage_strategy, manage_symbol, manage_interval, manage_volume, manage_params],
            outputs=[manage_status, manage_table, manage_select_target, remove_target_select]
        ).then(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        )

        manage_delete_btn.click(
            manage_delete_target,
            inputs=[manage_select_target],
            outputs=[manage_status, manage_table, manage_select_target, remove_target_select]
        ).then(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        )

        def run_optimize_dispatch(strategy_name, symbol, interval, start, end, space_text, auto_fit):
            status, metrics, params, top_table, best_params, top_results = run_optimize(
                strategy_name, symbol, interval, start, end, space_text, auto_fit
            )
            choices = []
            if top_results:
                for i, r in enumerate(top_results):
                    param_dict = {k: v for k, v in r.items() if k not in {"score", "total_return", "sharpe_ratio"}}
                    desc = f"第 {i+1} 名: {_params_line(param_dict)} (夏普: {r.get('sharpe_ratio', 0.0):.4f}, 收益: {r.get('total_return', 0.0):.2%})"
                    choices.append((desc, i))
            if not choices:
                choices = [("暂无寻优结果，请先运行寻优", -1)]
                val = -1
            else:
                val = 0
            return (
                status, metrics, params, top_table, best_params, top_results,
                gr.update(choices=choices, value=val),
                gr.update(selected="visual_optimize")
            )

        opt_run_btn.click(
            run_optimize_dispatch,
            inputs=[global_strategy, global_symbol, global_interval, global_start, global_end, opt_space, opt_auto_fit],
            outputs=[opt_status, opt_metrics_table, opt_params_table, opt_top_table, opt_best_params, opt_top_results, opt_select_combo, visual_tabs],
        )

        def apply_opt_params(strategy_name, selected_idx, top_results, best_params):
            params_to_apply = None
            if top_results and 0 <= selected_idx < len(top_results):
                r = top_results[selected_idx]
                params_to_apply = {k: v for k, v in r.items() if k not in {"score", "total_return", "sharpe_ratio"}}
            elif best_params:
                params_to_apply = best_params
                
            if not params_to_apply:
                return "未找到可选参数，请先运行寻优", gr.update(), gr.update(), gr.update(), gr.update()
            
            # Format parameters as key=value strings
            formatted = "\n".join(f"{k}={v}" for k, v in params_to_apply.items())
            
            # Decide which textbox to update and switch strategy/tab
            if strategy_name == "demo_momentum":
                return "参数已应用到回测", formatted, gr.update(), gr.update(selected="visual_backtest"), strategy_name
            else:
                return "参数已应用到回测", gr.update(), formatted, gr.update(selected="visual_backtest"), strategy_name

        opt_apply_btn.click(
            apply_opt_params,
            inputs=[global_strategy, opt_select_combo, opt_top_results, opt_best_params],
            outputs=[opt_status, bt_params_lookback, bt_params_macd, visual_tabs, global_strategy],
        )

        def apply_best_params_direct(strategy_name, best_params):
            if not best_params:
                return "未找到最优参数，请先运行寻优", gr.update(), gr.update()
            
            formatted = "\n".join(f"{k}={v}" for k, v in best_params.items())
            
            if strategy_name == "demo_momentum":
                return "最优参数已填入回测面板", formatted, gr.update()
            else:
                return "最优参数已填入回测面板", gr.update(), formatted

        opt_apply_best_btn.click(
            apply_best_params_direct,
            inputs=[global_strategy, opt_best_params],
            outputs=[opt_status, bt_params_lookback, bt_params_macd],
        )

        def run_walk_forward_dispatch(
            strategy_name, symbol, interval, start, end,
            space_text, train_window, test_window, auto_fit,
        ):
            status, metrics, params_df, folds_df, fig, trades_df = run_walk_forward(
                strategy_name, symbol, interval, start, end,
                space_text, train_window, test_window, auto_fit,
            )
            return status, metrics, params_df, folds_df, fig, trades_df, gr.update(selected="visual_walk_forward")

        wf_run_btn.click(
            run_walk_forward_dispatch,
            inputs=[
                global_strategy, global_symbol, global_interval, global_start, global_end,
                opt_space, wf_train, wf_test, wf_auto_fit,
            ],
            outputs=[wf_status, wf_metrics_table, wf_params_table, wf_folds_table, wf_folds_plot, wf_trades_table, visual_tabs],
        )

        def update_strategy_change(strategy_name):
            space_text = _default_space_text(strategy_name)
            show_lookback = strategy_name == "demo_momentum"
            show_macd     = strategy_name == "cm_macd_ult_mtf"
            return (
                space_text,
                gr.update(visible=show_lookback),
                gr.update(visible=show_macd),
            )

        global_strategy.change(
            update_strategy_change,
            inputs=[global_strategy],
            outputs=[opt_space, bt_params_lookback, bt_params_macd],
        )
        opt_auto_fit.change(toggle_space_visibility, inputs=[opt_auto_fit], outputs=[opt_space])

        # ── 设置标签页事件 ──────────────────────────────────────────

        CFG_KEYS = [
            "research.default_strategy",
            "research.default_symbol",
            "research.default_interval",
            "research.default_order_size",
            "research.default_rank_lookback_hours",
            "research.maker_fee_rate",
            "research.taker_fee_rate",
            "research.use_maker_fee",
            "research.optimize_target",
            "research.trade_mode",
            "risk.max_strategy_capital",
            "risk.max_total_exposure",
            "risk.max_drawdown",
            "risk.max_order_size",
            "risk.allow_opening_trades",
            "okx.api_key",
            "okx.secret_key",
            "okx.passphrase",
            "okx.demo_trading",
        ]

        cfg_outputs = [
            cfg_status,
            cfg_strategy, cfg_symbol, cfg_interval,
            cfg_order_size, cfg_rank_hours,
            cfg_maker_fee, cfg_taker_fee, cfg_use_maker, cfg_optimize_target, cfg_trade_mode,
            cfg_max_capital, cfg_max_exposure, cfg_max_drawdown,
            cfg_max_order, cfg_allow_open,
            cfg_okx_key, cfg_okx_secret, cfg_okx_passphrase, cfg_okx_demo,
            global_strategy, global_symbol, global_interval,
            global_start, global_end,
            bt_params_lookback, bt_params_macd,
            remove_target_select,
            manage_table,
            manage_select_target,
        ]

        def load_settings():
            cs = _get_config_service()
            cs._load_overrides()
            vals = []
            from pydantic import SecretStr
            for k in CFG_KEYS:
                val = cs.get(k)
                if isinstance(val, SecretStr):
                    val = val.get_secret_value()
                if k == "research.default_interval" and isinstance(val, str):
                    val = val.lower()
                vals.append(val)

            g_strat = cs.get("research.default_strategy") or "cm_macd_ult_mtf"
            g_sym = cs.get("research.default_symbol") or "XAU-USDT-SWAP"
            g_int = cs.get("research.default_interval") or "4h"
            if isinstance(g_int, str):
                g_int = g_int.lower()
            g_start, g_end = _default_dates(cs)

            current_params = cs.get("research.strategy_parameters") or {}
            demo_p = current_params.get("demo_momentum", {})
            if not demo_p:
                demo_p = STRATEGY_PARAMS.get("demo_momentum", {}).get("defaults", {})
            demo_p_text = "\n".join(f"{k}={v}" for k, v in demo_p.items())

            macd_p = current_params.get("cm_macd_ult_mtf", {})
            if not macd_p:
                macd_p = STRATEGY_PARAMS.get("cm_macd_ult_mtf", {}).get("defaults", {})
            macd_p_text = "\n".join(f"{k}={v}" for k, v in macd_p.items())

            df, choices, val = _get_targets_df_and_choices()
            return ["当前配置已加载"] + vals + [
                g_strat, g_sym, g_int, g_start, g_end, demo_p_text, macd_p_text,
                gr.update(choices=choices, value=val),
                df,
                gr.update(choices=choices, value=val),
            ]

        def save_settings(
            strategy, symbol, interval,
            order_size, rank_hours,
            maker_fee, taker_fee, use_maker, optimize_target, trade_mode,
            max_capital, max_exposure, max_drawdown,
            max_order, allow_open,
            api_key, secret_key, passphrase, demo_trading,
        ):
            cs = _get_config_service()
            if isinstance(interval, str):
                interval = interval.lower()
            values = [
                strategy, symbol, interval,
                order_size, rank_hours,
                maker_fee, taker_fee, use_maker, optimize_target, trade_mode,
                max_capital, max_exposure, max_drawdown,
                max_order, allow_open,
                api_key, secret_key, passphrase, demo_trading,
            ]
            saved = []
            for key, val in zip(CFG_KEYS, values):
                try:
                    ok = cs.set(key, val)
                    if ok:
                        saved.append(ConfigService.CONFIG_LABELS.get(key, key))
                except Exception:
                    pass
            
            vals = []
            for k in CFG_KEYS:
                val = cs.get(k)
                if k == "research.default_interval" and isinstance(val, str):
                    val = val.lower()
                vals.append(val)

            g_strat = cs.get("research.default_strategy") or "cm_macd_ult_mtf"
            g_sym = cs.get("research.default_symbol") or "XAU-USDT-SWAP"
            g_int = cs.get("research.default_interval") or "4h"
            if isinstance(g_int, str):
                g_int = g_int.lower()
            g_start, g_end = _default_dates(cs)

            current_params = cs.get("research.strategy_parameters") or {}
            demo_p = current_params.get("demo_momentum", {})
            if not demo_p:
                demo_p = STRATEGY_PARAMS.get("demo_momentum", {}).get("defaults", {})
            demo_p_text = "\n".join(f"{k}={v}" for k, v in demo_p.items())

            macd_p = current_params.get("cm_macd_ult_mtf", {})
            if not macd_p:
                macd_p = STRATEGY_PARAMS.get("cm_macd_ult_mtf", {}).get("defaults", {})
            macd_p_text = "\n".join(f"{k}={v}" for k, v in macd_p.items())

            df, choices, val = _get_targets_df_and_choices()
            return [f"已保存 {len(saved)} 项配置并同步到全局回测配置"] + vals + [
                g_strat, g_sym, g_int, g_start, g_end, demo_p_text, macd_p_text,
                gr.update(choices=choices, value=val),
                df,
                gr.update(choices=choices, value=val),
            ]

        def reset_settings():
            cs = _get_config_service()
            cs.reset_all()
            cs._load_overrides()
            vals = []
            for k in CFG_KEYS:
                val = cs.get(k)
                if k == "research.default_interval" and isinstance(val, str):
                    val = val.lower()
                vals.append(val)

            g_strat = cs.get("research.default_strategy") or "cm_macd_ult_mtf"
            g_sym = cs.get("research.default_symbol") or "XAU-USDT-SWAP"
            g_int = cs.get("research.default_interval") or "4h"
            if isinstance(g_int, str):
                g_int = g_int.lower()
            g_start, g_end = _default_dates(cs)

            current_params = cs.get("research.strategy_parameters") or {}
            demo_p = current_params.get("demo_momentum", {})
            if not demo_p:
                demo_p = STRATEGY_PARAMS.get("demo_momentum", {}).get("defaults", {})
            demo_p_text = "\n".join(f"{k}={v}" for k, v in demo_p.items())

            macd_p = current_params.get("cm_macd_ult_mtf", {})
            if not macd_p:
                macd_p = STRATEGY_PARAMS.get("cm_macd_ult_mtf", {}).get("defaults", {})
            macd_p_text = "\n".join(f"{k}={v}" for k, v in macd_p.items())

            df, choices, val = _get_targets_df_and_choices()
            return ["已重置全部配置且同步到全局回测配置"] + vals + [
                g_strat, g_sym, g_int, g_start, g_end, demo_p_text, macd_p_text,
                gr.update(choices=choices, value=val),
                df,
                gr.update(choices=choices, value=val),
            ]

        cfg_save_btn.click(
            save_settings,
            inputs=[
                cfg_strategy, cfg_symbol, cfg_interval,
                cfg_order_size, cfg_rank_hours,
                cfg_maker_fee, cfg_taker_fee, cfg_use_maker, cfg_optimize_target, cfg_trade_mode,
                cfg_max_capital, cfg_max_exposure, cfg_max_drawdown,
                cfg_max_order, cfg_allow_open,
                cfg_okx_key, cfg_okx_secret, cfg_okx_passphrase, cfg_okx_demo,
            ],
            outputs=cfg_outputs,
        )
        cfg_reset_btn.click(reset_settings, outputs=cfg_outputs)
        cfg_reload_btn.click(load_settings, outputs=cfg_outputs)
        
        live_refresh_btn.click(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        )

        def add_monitored_target(symbol, interval, strategy, volume):
            try:
                cs = _get_config_service()
                targets = cs.get("research.monitored_targets") or []
                # Check if it already exists
                for t in targets:
                    if t.get("symbol") == symbol and t.get("interval") == interval and t.get("strategy_name") == strategy:
                        df, choices, val = _get_targets_df_and_choices()
                        return f"⚠️ 该监控目标 {symbol} ({interval}) 已经存在于列表中！", gr.update(choices=choices, value=val), df, gr.update(choices=choices, value=val)
                
                # Append new target
                targets.append({
                    "strategy_name": strategy,
                    "symbol": symbol,
                    "interval": interval,
                    "volume": float(volume)
                })
                cs.set("research.monitored_targets", targets)
                
                df, choices, val = _get_targets_df_and_choices()
                return f"✅ 成功添加监控目标 {symbol} ({interval})！", gr.update(choices=choices, value=val), df, gr.update(choices=choices, value=val)
            except Exception as e:
                df, choices, val = _get_targets_df_and_choices()
                return f"❌ 添加监控目标失败: {e}", gr.update(), df, gr.update()

        def remove_monitored_target(target_str):
            try:
                if not target_str:
                    df, choices, val = _get_targets_df_and_choices()
                    return "⚠️ 请先选择要移除的监控目标！", gr.update(), df, gr.update()
                cs = _get_config_service()
                targets = cs.get("research.monitored_targets") or []
                new_targets = []
                for t in targets:
                    match_str = f"{t['symbol']} ({t['interval']} - {t['strategy_name']})"
                    if match_str != target_str:
                        new_targets.append(t)
                
                cs.set("research.monitored_targets", new_targets)
                df, choices, val = _get_targets_df_and_choices()
                return f"✅ 成功移除监控目标 {target_str}！", gr.update(choices=choices, value=val), df, gr.update(choices=choices, value=val)
            except Exception as e:
                df, choices, val = _get_targets_df_and_choices()
                return f"❌ 移除监控目标失败: {e}", gr.update(), df, gr.update()

        btn_add_target.click(
            add_monitored_target,
            inputs=[add_symbol, add_interval, add_strategy, add_volume],
            outputs=[manage_status, remove_target_select, manage_table, manage_select_target]
        ).then(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        )

        btn_remove_target.click(
            remove_monitored_target,
            inputs=[remove_target_select],
            outputs=[manage_status, remove_target_select, manage_table, manage_select_target]
        ).then(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        )
        
        # Load settings automatically when the page is opened, then fetch live status, then auto-run backtest
        app.load(
            load_settings,
            outputs=cfg_outputs
        ).then(
            fetch_live_status,
            outputs=[live_health, live_config, live_account, live_positions, live_logs_table]
        ).then(
            run_backtest_dispatch,
            inputs=[
                global_strategy, global_symbol, global_interval, global_start, global_end,
                bt_params_lookback, bt_params_macd,
            ],
            outputs=[bt_status, bt_metrics_table, bt_params_table, bt_chart, bt_trades_table, visual_tabs],
        )

    app.launch(server_name="0.0.0.0", server_port=port)
