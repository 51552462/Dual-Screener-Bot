import os
import re
import time
import threading

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pandas as pd


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHART_FOLDER = os.path.join(BASE_DIR, "charts")
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)
chart_lock = threading.Lock()


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9가-힣._-]", "_", s)


def get_daily_theme():
    theme_idx = time.localtime().tm_mday % 5
    themes = [
        {"bg": "#0B0E14", "grid": "#1A202C", "text": "#FFFFFF", "up": "#F6465D", "down": "#0ECB81"},
        {"bg": "#FFFFFF", "grid": "#F0F0F0", "text": "#131722", "up": "#E0294A", "down": "#2EBD85"},
        {"bg": "#131722", "grid": "#2A2E39", "text": "#D1D4DC", "up": "#26A69A", "down": "#EF5350"},
        {"bg": "#000000", "grid": "#111111", "text": "#00FFA3", "up": "#00FFA3", "down": "#FF3366"},
        {"bg": "#F8F9FA", "grid": "#E9ECEF", "text": "#212529", "up": "#FF4757", "down": "#2ED573"},
    ]
    return themes[theme_idx]


def save_chart(df: pd.DataFrame, symbol: str, rank: int, show_volume=False, is_promo=False, side="LONG"):
    with chart_lock:
        try:
            plt.rcParams["font.family"] = "NanumGothic"
            plt.rcParams["axes.unicode_minus"] = False

            timestamp_ms = int(time.time() * 1000)
            vol_suffix = "promo" if is_promo else ("wVol" if show_volume else "noVol")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(symbol)}_{timestamp_ms}_{vol_suffix}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            df_cut.dropna(subset=["Open", "High", "Low", "Close", "Volume"], inplace=True)
            if df_cut.empty or len(df_cut) < 5:
                return None

            c = df_cut["Close"].iloc[-1]
            prev_c = df_cut["Close"].iloc[-2] if len(df_cut) > 1 else c
            diff = c - prev_c
            diff_pct = (diff / prev_c) * 100 if prev_c != 0 else 0
            sign = "▲" if diff > 0 else ("▼" if diff < 0 else "-")

            if is_promo:
                theme = get_daily_theme()
                bg_color, grid_color, text_main = theme["bg"], theme["grid"], theme["text"]
                color_up, color_down = theme["up"], theme["down"]
                text_sub = text_main
                custom_figsize = (9, 9)
            else:
                bg_color, grid_color, text_main, text_sub = "#131722", "#2A2E39", "#FFFFFF", "#8A91A5"
                color_up, color_down = "#FF3B69", "#00B4D8"
                custom_figsize = (11, 6.5) if show_volume else (9, 9)

            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)
            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut["High"].max() - df_cut["Low"].min()) * 0.04
            side_u = str(side or "LONG").upper()
            if side_u == "SHORT":
                signal_marker.iloc[-1] = df_cut["High"].iloc[-1] + y_offset
                marker_style = "v"
                marker_color = "#EF5350"
            else:
                signal_marker.iloc[-1] = df_cut["Low"].iloc[-1] - y_offset
                marker_style = "^"
                marker_color = "#FFD700"
            ap = mpf.make_addplot(
                signal_marker,
                type="scatter",
                markersize=400 if is_promo else 300,
                marker=marker_style,
                color=marker_color,
                alpha=1.0,
            )

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge="inherit", wick="inherit", volume="inherit")
            style = mpf.make_mpf_style(
                marketcolors=mc,
                facecolor=bg_color,
                edgecolor=bg_color,
                figcolor=bg_color,
                gridcolor=grid_color,
                gridstyle="--",
                y_on_right=True,
                rc={"font.family": plt.rcParams["font.family"], "text.color": text_main, "axes.labelcolor": text_sub, "xtick.color": text_sub, "ytick.color": text_sub},
            )

            fig, _ = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=style, figsize=custom_figsize, tight_layout=False, returnfig=True)
            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            fig.text(0.05, 0.94, symbol, fontsize=24 if is_promo else 22, fontweight="bold", color=text_main, ha="left")
            right_text = f"{sign} {abs(diff_pct):.2f}%" if is_promo else f"Close: {c:,.4f} ({sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, 0.94, right_text, fontsize=22 if is_promo else 18, fontweight="bold", color=color_diff, ha="right")
            fig.text(0.05, 0.03, "Bitget Quant Signal", fontsize=10, color=text_sub, ha="left", style="italic")
            fig.savefig(path, dpi=250 if is_promo else 200, bbox_inches="tight", facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception:
            return None
