# mmv_daily_total.py  — SAFE upgrade (μόνο requests + bs4)
# Source: MusicMetricsVault.com (personal use only)

import os, re, csv, time, datetime as dt
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup

ARTIST_URL = "https://www.musicmetricsvault.com/artists/anna-vissi/3qg78GGGWP04yTv0ZQMsXl"

# Αρχεία εξόδου
OUT_TOTAL_CSV = "mmv_total_streams.csv"      # date,total_plays,daily_delta,source
OUT_TRACKS_DIR = "mmv_tracks_daily"          # per-day ανάλυση

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def fetch(url: str, retries: int = 3, wait: int = 2) -> str:
    last = None
    for _ in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(wait)
    raise RuntimeError(f"Fetch failed: {last}")

def parse_human_number(s: str) -> Optional[int]:
    if not s: return None
    s = s.strip().lower().replace(",", "")
    m = re.match(r"^([\d\.]+)\s*([kmb])?$", s)
    if m:
        val = float(m.group(1)); suf = (m.group(2) or "")
        mult = {"k":1_000, "m":1_000_000, "b":1_000_000_000}.get(suf, 1)
        return int(round(val * mult))
    if re.fullmatch(r"\d+", s): return int(s)
    return None

def find_table_and_columns(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], dict]:
    for tbl in soup.find_all("table"):
        ths = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not ths: 
            continue
        col = {"title":None, "plays":None, "duration":None, "date":None, "isrc":None}
        for i, name in enumerate(ths):
            if any(k in name for k in ["track","title","song"]): col["title"] = i
            if ("play" in name) or ("stream" in name):         col["plays"] = i   # <— Plays support
            if "duration" in name or "length" in name:         col["duration"] = i
            if "release date" in name or name == "date":       col["date"] = i
            if "isrc" in name:                                 col["isrc"] = i
        if col["title"] is not None and col["plays"] is not None:
            return tbl, col
    return None, {}

def read_last_total(path: str) -> Optional[int]:
    if not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if len(rows) < 2: return None
    try:
        return int(rows[-1][1])  # total_plays της τελευταίας γραμμής
    except Exception:
        return None

def main():
    html = fetch(ARTIST_URL)
    soup = BeautifulSoup(html, "html.parser")

    table, col = find_table_and_columns(soup)
    if not table:
        print("❌ Δεν βρέθηκε πίνακας με στήλες Track/Plays.")
        input("\nΠάτα Enter για έξοδο...")
        return

    rows = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if not tds: 
            continue
        def safe(idx): 
            return tds[idx].get_text(strip=True) if idx is not None and idx < len(tds) else ""
        title    = safe(col["title"])
        plays    = parse_human_number(safe(col["plays"]))
        duration = safe(col.get("duration"))
        rel_date = safe(col.get("date"))
        isrc     = safe(col.get("isrc"))
        if title and plays is not None:
            rows.append({"title":title,"plays":plays,"duration":duration,"release_date":rel_date,"isrc":isrc})

    if not rows:
        print("❌ Δεν διαβάστηκαν σειρές με τίτλο + plays.")
        input("\nΠάτα Enter για έξοδο...")
        return

    total = sum(r["plays"] for r in rows)
    today = dt.datetime.now().date().isoformat()
    prev  = read_last_total(OUT_TOTAL_CSV)
    delta = None if prev is None else (total - prev)

    # — Σύνολο (append)
    write_header = not os.path.exists(OUT_TOTAL_CSV)
    with open(OUT_TOTAL_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["date","total_plays","daily_delta","source"])
        w.writerow([today, total, "" if delta is None else delta, "MusicMetricsVault.com (personal use)"])

    # — Αναλυτικά ανά ημέρα
    os.makedirs(OUT_TRACKS_DIR, exist_ok=True)
    daily_path = os.path.join(OUT_TRACKS_DIR, f"mmv_track_streams_{today}.csv")
    with open(daily_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title","plays","duration","release_date","isrc","source"])
        for r in rows:
            w.writerow([r["title"], r["plays"], r["duration"], r["release_date"], r["isrc"], "MusicMetricsVault.com"])

    print("✅ OK")
    print(f"Σύνολο σήμερα: {total:,} plays")
    if delta is not None:
        sign = "+" if delta >= 0 else ""
        print(f"Ημερήσια μεταβολή: {sign}{delta:,} plays")
    print(f"Saved: {OUT_TOTAL_CSV}")
    print(f"Saved: {daily_path}")

    input("\nΠάτα Enter για έξοδο...")

if __name__ == "__main__":
    main()