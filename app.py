import os
import re
from datetime import datetime

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# -----------------------------
# Page config & title
# -----------------------------
st.set_page_config(page_title="Anna Vissi — Total Streams (MMV)", layout="wide")
st.title("Anna Vissi — Total Streams")
st.caption("Personal tool • Source: MusicMetricsVault.com (estimates)")

# -----------------------------
# Load daily totals CSV
# -----------------------------
@st.cache_data(ttl=600)
def load_totals_csv(path: str = "mmv_total_streams.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df

try:
    df_totals = load_totals_csv()
except FileNotFoundError:
    st.error("File `mmv_total_streams.csv` not found in the same folder as this app.")
    st.stop()
except Exception as e:
    st.error(f"Failed to read totals CSV: {e}")
    st.stop()

if df_totals.empty:
    st.warning("The `mmv_total_streams.csv` file is empty.")
else:
    latest = df_totals.iloc[-1]
    total = int(latest["total_plays"])
    delta = latest.get("daily_delta", "")

    # Metrics (top cards)
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Streams", f"{total:,}")

    if delta != "" and pd.notna(delta):
        sign = "+" if float(delta) >= 0 else ""
        c2.metric("Daily Streams (Δ)", f"{sign}{int(delta):,}")
    else:
        c2.metric("Daily Streams (Δ)", "—")

    last_date_str = pd.to_datetime(latest["date"]).strftime("%d/%m/%Y")
    c3.metric("Last Update", last_date_str)

    # Chart (compact)
    fig, ax = plt.subplots(figsize=(5, 2.5))
    ax.plot(df_totals["date"], df_totals["total_plays"], linewidth=2)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Streams")
    ax.set_title("Total Streams Over Time")
    st.pyplot(fig, use_container_width=False)

    # Daily totals table (history)
    st.subheader("Daily Total History")
    st.dataframe(df_totals.sort_values("date", ascending=False), use_container_width=True)

    # -----------------------------
    # ALL TRACKS — today list + daily change + sorting options
    # -----------------------------
    st.subheader("All Songs — Total & Daily Performance")

    TRACKS_DIR = "mmv_tracks_daily"

    def _parse_date_from_name(name: str):
        # Accepts: mmv_track_streams_YYYY-MM-DD.csv or mmv_track_streams_YYYY_MM_DD.csv
        m = re.search(r"mmv_track_streams_(\d{4}[-_]\d{2}[-_]\d{2})\.csv$", name)
        if not m:
            return None
        raw = m.group(1).replace("_", "-")
        return datetime.strptime(raw, "%Y-%m-%d").date()

    # Find the latest (and previous) daily snapshot files
    daily_files = []
    if os.path.isdir(TRACKS_DIR):
        for fn in os.listdir(TRACKS_DIR):
            if fn.startswith("mmv_track_streams_") and fn.endswith(".csv"):
                d = _parse_date_from_name(fn)
                if d:
                    daily_files.append((d, os.path.join(TRACKS_DIR, fn)))

    if not daily_files:
        st.info("No daily files found in `mmv_tracks_daily/`.")
    else:
        daily_files.sort(key=lambda x: x[0])  # by date
        d_today, path_today = daily_files[-1]
        df_today = pd.read_csv(path_today)

        # Normalize column names & ensure required columns exist
        df_today.columns = [c.strip().lower() for c in df_today.columns]
        for need in ["title", "plays", "release_date", "duration", "isrc"]:
            if need not in df_today.columns:
                df_today[need] = None

        # Try to compare with previous day; else set ΔToday = 0
        have_prev = len(daily_files) >= 2
        if have_prev:
            d_prev, path_prev = daily_files[-2]
            df_prev = pd.read_csv(path_prev)
            df_prev.columns = [c.strip().lower() for c in df_prev.columns]
            for need in ["title", "plays", "isrc"]:
                if need not in df_prev.columns:
                    df_prev[need] = None

            # Prefer join by ISRC if present; fallback to Title
            key = ["isrc"] if (df_today["isrc"].notna().any() and df_prev["isrc"].notna().any()) else ["title"]
            merged = pd.merge(df_today, df_prev, on=key, how="left", suffixes=("_today", "_prev"))
            plays_col = merged["plays_today"].fillna(0)
            delta_col = (merged["plays_today"] - merged["plays_prev"]).fillna(0)
            titles = merged["title_today"].fillna(merged["title_prev"]).fillna("—")
        else:
            plays_col = df_today["plays"].fillna(0)
            delta_col = pd.Series([0] * len(df_today))
            titles = df_today["title"].fillna("—")

        # Numeric base for proper sorting
        out_raw = pd.DataFrame({
            "Title": titles,
            "plays_num": plays_col.astype(int),
            "delta_num": delta_col.astype(int)
        })

        # Sorting selector
        sort_choice = st.radio(
            "Sort by:",
            ["Total Streams (All-time)", "Daily Streams (Change vs. Yesterday)"],
            index=0,
            horizontal=True
        )

        if sort_choice.startswith("Total"):
            out_raw = out_raw.sort_values(["plays_num", "delta_num"], ascending=[False, False]).reset_index(drop=True)
        else:
            out_raw = out_raw.sort_values(["delta_num", "plays_num"], ascending=[False, False]).reset_index(drop=True)

        # Rank column
        out_raw.index += 1
        out_raw.insert(0, "Rank", out_raw.index)

        # Display-friendly formatting
        out_disp = out_raw.copy()
        out_disp["Total Streams"] = out_disp["plays_num"].map(lambda x: f"{int(x):,}")
        out_disp["Daily Streams"] = out_disp["delta_num"].map(lambda x: f"{int(x):+,}")
        out_disp = out_disp[["Rank", "Title", "Total Streams", "Daily Streams"]]

        if have_prev:
            st.caption(f"Snapshot: {d_today} vs {d_prev}")
        else:
            st.caption(f"Snapshot: {d_today} — first day (daily streams show as +0)")
        st.dataframe(out_disp, use_container_width=True)