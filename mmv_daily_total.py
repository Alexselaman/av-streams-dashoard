# mmv_daily_total.py — Scrape MMV, dedupe, EXCLUDE "Mouri", update totals CSV

import os, re, time, unicodedata
import datetime as dt
from typing import Optional
import requests
import pandas as pd
from bs4 import BeautifulSoup

ARTIST_URL = "https://www.musicmetricsvault.com/artists/anna-vissi/3qg78gggwp04ytv0zqmsxl"
OUT_TOTAL_CSV = "mmv_total_streams.csv"
OUT_TRACKS_DIR = "mmv_tracks_daily"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# αφαιρούμε το "Mouri" (και ελληνικές γραφές)
EXCLUDE_PATTERNS = [r"\bmouri\b", r"\bμουρη\b", r"\bμούρη\b"]

def fetch(url: str, retries: int = 3, wait: int = 2) -> str:
    last = None
    for i in range(retries):
        print(f"[fetch] GET {url} (try {i+1}/{retries})")
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            print("[fetch] status=", r.status_code)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(wait)
    raise RuntimeError(f"Fetch failed: {last}")

def parse_human_int(s: str) -> Optional[int]:
    if s is None: return None
    s = str(s).strip().replace(",", "")
    return int(s) if s.isdigit() else None

def strip_accents(x: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", x) if unicodedata.category(c) != "Mn")

def norm_title(t: str) -> str:
    t = str(t or "").strip().lower()
    t = strip_accents(t)
    t = re.sub(r"\s+", " ", t)
    return t

def should_exclude(title: str) -> bool:
    nt = norm_title(title)
    return any(re.search(p, nt) for p in EXCLUDE_PATTERNS)

def parse_duration_to_seconds(s: str) -> Optional[int]:
    if s is None: return None
    s = str(s).strip()
    m = re.match(r"^(\d+):(\d{1,2})$", s)
    if m: return int(m.group(1))*60 + int(m.group(2))
    try: return int(round(float(s)))
    except Exception: return None

def make_key(title: str, duration: str) -> str:
    return f"{norm_title(title)}|{parse_duration_to_seconds(duration)}"

def find_tracks_table(soup: BeautifulSoup):
    for idx, tbl in enumerate(soup.find_all("table"), start=1):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        print(f"[table] candidate {idx} headers={headers}")
        if {"track","plays","duration","release date"}.issubset(set(headers)):
            print(f"[table] chosen #{idx}")
            return tbl
    return None

def table_to_df(tbl: BeautifulSoup) -> pd.DataFrame:
    headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if not tds or len(tds) < len(headers):
            continue
        rows.append([td.get_text(" ", strip=True) for td in tds[:len(headers)]])
    df = pd.DataFrame(rows, columns=[h.strip().lower().replace(" ", "_") for h in headers])
    df = df.rename(columns={"track":"title","plays":"plays","duration":"duration","release_date":"release_date"})
    df["plays"] = df["plays"].apply(parse_human_int)
    df = df.dropna(subset=["title","plays"])
    print(f"[parse] rows={len(df)}")
    return df

def update_totals_csv(today: str, total: int, prev_total: Optional[int]):
    header = "date,total_plays,daily_delta,source\n"
    line   = f"{today},{total},{0 if prev_total is None else total - prev_total},MusicMetricsVault.com (personal use)\n"

    if not os.path.exists(OUT_TOTAL_CSV) or os.path.getsize(OUT_TOTAL_CSV)==0:
        with open(OUT_TOTAL_CSV, "w", encoding="utf-8") as f:
            f.write(header + line)
        print("[save] created totals with today")
        return

    with open(OUT_TOTAL_CSV, "r", encoding="utf-8") as f:
        lines = [ln for ln in f.readlines() if ln.strip()]

    # βρες prev_total αν δεν δόθηκε
    if prev_total is None:
        for ln in reversed(lines):
            if ln.startswith("date,") or ln.startswith(today + ","):
                continue
            try:
                prev_total = int(ln.split(",")[1])
                break
            except Exception:
                pass
        # ανανέωσε τη γραμμή με σωστό delta
        line = f"{today},{total},{0 if prev_total is None else total - prev_total},MusicMetricsVault.com (personal use)\n"

    # αντικατάσταση ή προσθήκη
    replaced = False
    for i in range(len(lines)-1, -1, -1):
        if lines[i].startswith(today + ","):
            lines[i] = line
            replaced = True
            break
    if not replaced:
        lines.append(line)

    with open(OUT_TOTAL_CSV, "w", encoding="utf-8") as f:
        if not lines[0].startswith("date,"):
            f.write(header)
        f.writelines([ln if ln.endswith("\n") else ln + "\n" for ln in lines])
    print("[save] totals updated")

def main():
    print("▶ START SCRAPE")
    html = fetch(ARTIST_URL)
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    tbl = find_tracks_table(soup)
    if not tbl:
        raise RuntimeError("Tracks table not found")

    df = table_to_df(tbl)

    # exclude τίτλους (π.χ. Mouri)
    before = len(df)
    df = df[~df["title"].apply(should_exclude)]
    print(f"[exclude] removed={before-len(df)}")

    # dedupe: ίδιος normalised τίτλος + ίδια διάρκεια ⇒ κρατάμε το max plays
    df["_key"] = df.apply(lambda r: make_key(r.get("title"), r.get("duration")), axis=1)
    df_dedup = (df.sort_values("plays", ascending=False)
                  .groupby("_key", as_index=False)
                  .agg(title=("title","first"),
                       plays=("plays","max"),
                       duration=("duration","first"),
                       release_date=("release_date","first")))

    today = dt.date.today().strftime("%Y-%m-%d")
    os.makedirs(OUT_TRACKS_DIR, exist_ok=True)
    raw_path   = os.path.join(OUT_TRACKS_DIR, f"mmv_track_streams_{today}.csv")
    dedup_path = os.path.join(OUT_TRACKS_DIR, f"mmv_track_streams_{today}_deduped.csv")
    df.to_csv(raw_path, index=False, encoding="utf-8-sig")
    df_dedup.to_csv(dedup_path, index=False, encoding="utf-8-sig")
    print(f"[save] RAW -> {raw_path}   DEDUP -> {dedup_path}")

    total = int(df_dedup["plays"].fillna(0).astype(int).sum())
    print(f"[total] {total:,}")

    # previous total αν υπάρχει τελευταίο διαφορετικής μέρας
    prev_total = None
    if os.path.exists(OUT_TOTAL_CSV) and os.path.getsize(OUT_TOTAL_CSV)>0:
        try:
            old = pd.read_csv(OUT_TOTAL_CSV)
            if "date" in old.columns and "total_plays" in old.columns:
                old["date"] = pd.to_datetime(old["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                prev_rows = old[old["date"] < today]
                if not prev_rows.empty:
                    prev_total = int(prev_rows.iloc[-1]["total_plays"])
        except Exception:
            prev_total = None

    update_totals_csv(today, total, prev_total)
    print("✅ DONE")

if __name__ == "__main__":
    main()