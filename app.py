# app.py  — Anna Vissi Streams dashboard (robust)
import os, glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st

st.set_page_config(page_title="Anna Vissi — Total Streams", layout="wide")
st.title("Anna Vissi — Total Streams")
st.caption("Personal tool • Source: MusicMetricsVault.com (estimates)")

TOTALS_CSV = "mmv_total_streams.csv"
TRACKS_DIR = "mmv_tracks_daily"

# ----------------- Helpers -----------------
@st.cache_data
def load_totals_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Coerce expected columns / tolerate weird files
    if "date" not in df.columns:
        # try set column names if missing
        if df.shape[1] >= 3:
            df.columns = ["date", "total_plays", "daily_delta"] + list(df.columns[3:])
        else:
            return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if "total_plays" in df.columns:
        df["total_plays"] = pd.to_numeric(df["total_plays"], errors="coerce").fillna(0).astype(int)
    if "daily_delta" in df.columns:
        df["daily_delta"] = pd.to_numeric(df["daily_delta"], errors="coerce").fillna(0).astype(int)
    return df.sort_values("date")

@st.cache_data
def _list_track_files(track_dir: str):
    # prefer deduped; fall back to raw
    files = sorted(glob.glob(os.path.join(track_dir, "mmv_track_streams_*_deduped.csv")))
    raw   = sorted(glob.glob(os.path.join(track_dir, "mmv_track_streams_*.csv")))
    if files: 
        return files
    return raw

@st.cache_data
def load_tracks_today_prev(track_dir: str):
    files = _list_track_files(track_dir)
    if not files:
        return pd.DataFrame(), pd.DataFrame(), None, None
    today_path = files[-1]
    prev_path  = files[-2] if len(files) >= 2 else None

    def _read(p):
        if p is None or not os.path.exists(p):
            return pd.DataFrame()
        df = pd.read_csv(p)
        # normalize column names
        df = df.rename(columns={"track":"title"})
        if "title" not in df.columns: return pd.DataFrame()
        if "plays" in df.columns:
            df["plays"] = pd.to_numeric(df["plays"], errors="coerce").fillna(0).astype(int)
        # keep helpful columns only
        keep = [c for c in ["title","plays","duration","release_date"] if c in df.columns]
        return df[keep]

    df_today = _read(today_path)
    df_prev  = _read(prev_path)
    return df_today, df_prev, today_path, prev_path

def add_daily_change(df_today, df_prev):
    if df_today.empty:
        return df_today.assign(daily_change=np.nan)
    if df_prev.empty:
        return df_today.assign(daily_change=0)

    # merge by (normalized) title + duration if available
    t = df_today.copy()
    p = df_prev.copy()

    def keyify(d):
        k = d["title"].str.strip().str.lower()
        k = k.str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")
        if "duration" in d.columns:
            return k + "|" + d["duration"].fillna("").astype(str)
        return k

    t["_k"] = keyify(t)
    p["_k"] = keyify(p)

    merged = t.merge(p[["_k","plays"]].rename(columns={"plays":"plays_prev"}), on="_k", how="left")
    merged["plays_prev"] = pd.to_numeric(merged["plays_prev"], errors="coerce").fillna(0).astype(int)
    merged["daily_change"] = (merged["plays"] - merged["plays_prev"]).astype(int)
    return merged.drop(columns=["_k"])

# ----------------- Load data -----------------
# totals
try:
    totals = load_totals_csv(TOTALS_CSV)
except Exception as e:
    st.error(f"Failed to read totals CSV: {e}")
    totals = pd.DataFrame()

# tracks
tracks_today, tracks_prev, f_today, f_prev = load_tracks_today_prev(TRACKS_DIR)
tracks = add_daily_change(tracks_today, tracks_prev)

# ----------------- UI: metrics -----------------
if totals.empty:
    st.warning("The mmv_total_streams.csv file is empty.")
else:
    latest = totals.iloc[-1]
    total = int(latest.get("total_plays", 0))
    delta = int(latest.get("daily_delta", 0))
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Streams", f"{total:,}")
    c2.metric("Daily Streams (Δ)", f"{delta:+,}")
    c3.metric("Last update", latest["date"].date().strftime("%d/%m/%Y"))

    # plot
    fig, ax = plt.subplots(figsize=(5, 2.5))
    ax.plot(totals["date"], totals["total_plays"], linewidth=2)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.set_xlabel("Date")
    ax.set_ylabel("Total Streams")
    ax.set_title("Total Streams Over Time")
    st.pyplot(fig)

# ----------------- UI: track table -----------------
st.subheader("Track Performance")

if tracks.empty:
    st.info("No track file found yet in the folder 'mmv_tracks_daily'.")
else:
    sort_by = st.radio(
        "Sort by",
        ["Total Streams", "Daily Streams (Δ)"],
        horizontal=True,
        index=0
    )
    view = tracks.copy()
    # default sort total desc
    if sort_by == "Daily Streams (Δ)":
        view = view.sort_values(["daily_change","plays","title"], ascending=[False, False, True])
    else:
        view = view.sort_values(["plays","title"], ascending=[False, True])

    # show clean columns with indexing
    nice = view.rename(columns={"title":"Title","plays":"Total Streams","daily_change":"Daily Streams (Δ)"})
    nice.insert(0, "#", range(1, len(nice)+1))
    st.dataframe(nice, height=520)

    # file hints
    ft = os.path.basename(f_today) if f_today else "—"
    fp = os.path.basename(f_prev) if f_prev else "—"
    st.caption(f"Using: **{ft}**  ·  Previous for Δ: **{fp}**")