# app.py — fix duplicate numbering (No already exists)

import os, glob, pandas as pd, matplotlib.pyplot as plt, streamlit as st

TOTALS_CSV = "mmv_total_streams.csv"
TRACKS_DIR = "mmv_tracks_daily"

st.set_page_config(page_title="Anna Vissi — Total Streams", layout="wide")
st.markdown("# Anna Vissi — Total Streams")
st.caption("Personal tool · Source: MusicMetricsVault.com (estimates)")

@st.cache_data
def load_totals_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date","total_plays","daily_delta","source"])
    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.columns[0].lower().startswith("﻿date"):
        df.rename(columns={df.columns[0]: "date"}, inplace=True)
    df = df[df["date"].astype(str).str.match(r"\d{4}-\d{2}-\d{2}", na=False)]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    df["total_plays"] = pd.to_numeric(df["total_plays"], errors="coerce").fillna(0).astype(int)
    if "daily_delta" in df.columns:
        df["daily_delta"] = pd.to_numeric(df["daily_delta"], errors="coerce").fillna(0).astype(int)
    else:
        df["daily_delta"] = 0
    return df

@st.cache_data
def load_latest_tracks(dirpath: str) -> pd.DataFrame:
    if not os.path.isdir(dirpath):
        return pd.DataFrame(columns=["title","plays","cover_url","release_date","daily_delta"])
    files = sorted(glob.glob(os.path.join(dirpath, "mmv_track_streams_*_deduped.csv")))
    if not files:
        return pd.DataFrame(columns=["title","plays","cover_url","release_date","daily_delta"])
    df = pd.read_csv(files[-1], encoding="utf-8-sig")
    if "plays" not in df and "total" in df:
        df.rename(columns={"total":"plays"}, inplace=True)
    df["plays"] = pd.to_numeric(df.get("plays", 0), errors="coerce").fillna(0).astype(int)
    if "daily_delta" not in df.columns:
        df["daily_delta"] = 0
    df["title"] = df.get("title").astype(str)
    df["cover_url"] = df.get("cover_url")
    return df

totals = load_totals_csv(TOTALS_CSV)

if totals.empty:
    st.warning("The **mmv_total_streams.csv** file is empty.")
else:
    latest = totals.iloc[-1]
    c1, c2, c3 = st.columns([1,1,1])
    c1.metric("Total Streams", f"{int(latest['total_plays']):,}")
    c2.metric("Daily Streams (Δ)", f"{int(latest.get('daily_delta',0)):+,}")
    c3.metric("Last Update", latest["date"].strftime("%d/%m/%Y"))

    # smaller chart: 50% (από 6×3 -> 3×1.5)
    fig, ax = plt.subplots(figsize=(3, 1.5))
    ax.plot(totals["date"], totals["total_plays"], linewidth=2)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.set_xlabel("Date"); ax.set_ylabel("Total Streams")
    ax.set_title("Total Streams Over Time")
    st.pyplot(fig, use_container_width=False)

st.markdown("## Track Performance")

tracks = load_latest_tracks(TRACKS_DIR)

# επιλογή sort
sort_by = st.radio("Sort by", ["Total Streams", "Daily"], horizontal=True, label_visibility="collapsed")
if sort_by == "Daily":
    tracks = tracks.sort_values("daily_delta", ascending=False).reset_index(drop=True)
else:
    tracks = tracks.sort_values("plays", ascending=False).reset_index(drop=True)

# numbering 1..N (μόνο αν δεν υπάρχει ήδη)
if "No" not in tracks.columns:
    tracks.insert(0, "No", tracks.index + 1)
else:
    tracks["No"] = range(1, len(tracks) + 1)

# view (με κόμματα)
view = pd.DataFrame({
    "No": tracks["No"],
    "Cover": tracks.get("cover_url"),
    "Title": tracks["title"],
    "Total Streams": tracks["plays"].map(lambda x: f"{x:,}"),
    "Daily (Δ)": tracks["daily_delta"].map(lambda x: f"{x:+,}")
})

st.dataframe(
    view,
    column_order=["No","Cover","Title","Total Streams","Daily (Δ)"],
    hide_index=True,
    use_container_width=True,
    column_config={
        "No": st.column_config.NumberColumn("No", width="small"),
        "Cover": st.column_config.ImageColumn("Cover", help="Album art", width="small"),
        "Title": st.column_config.TextColumn("Title", width="large"),
        "Total Streams": st.column_config.TextColumn("Total Streams", width="medium"),
        "Daily (Δ)": st.column_config.TextColumn("Daily (Δ)", width="small"),
    },
)