# mmv_daily_total.py — scrape MMV, dedupe, exclude specific tracks, save covers, update totals
# (Excludes: "Mouri" + "Sta Hronia Tis Ipomonis - Remastered 2005")

import os, re, time, unicodedata
import datetime as dt
from typing import Optional
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- CONFIG ----------
ARTIST_URL = "https://www.musicmetricsvault.com/artists/anna-vissi/3qg78gggwp04ytv0zqmsxl"
OUT_TOTAL_CSV = "mmv_total_streams.csv"
OUT_TRACKS_DIR = "mmv_tracks_daily"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ---------- EXCLUSIONS ----------
# κανονικοποιούμε τίτλους (lower + χωρίς τόνους) πριν το matching
EXCLUDE_PATTERNS = [
    r"\bmouri\b",
    r"\bμουρη\b",
    r"\bμούρη\b",
    r"\bsta hronia tis ipomonis\s*-\s*remastered\s*2005\b",  # ακριβώς αυτός ο τίτλος/έκδοση
]

# ---------- HELPERS ----------
def fetch(url: str, retries: int = 3, wait: int = 2) -> str:
    last_err = None
    for i in range(retries):
        print(f"[fetch] GET {url} (try {i+1}/{retries})")
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            print(f"[fetch] status={r.status_code}")
            if r.status_code == 200:
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(wait)
    raise RuntimeError(f"Fetch failed: {last_err}")

def parse_human_int(s: str) -> Optional[int]:
    if s is None:
        return None
    s = str(s).strip().replace(",", "")
    return int(s) if s.isdigit() else None

def parse_duration_to_seconds(s: str) -> Optional[int]:
    if s is None: return None
    s = str(s).strip()
    m = re.match(r"^(\d+):(\d{1,2})$", s)
    if m: return int(m.group(1)) * 60 + int(m.group(2))
    try: return int(round(float(s)))
    except Exception: return None

def strip_accents(x: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", x) if unicodedata.category(c) != "Mn")

def norm_title_preserve_version(t: str) -> str:
    t = str(t or "").strip().lower()
    t = strip_accents(t)
    t = re.sub(r"\s+", " ", t)
    return t

def make_dedupe_key(title: str, duration: str) -> str:
    return f"{norm_title_preserve_version(title)}|{parse_duration_to_seconds(duration)}"

def should_exclude(title: str) -> bool:
    nt = norm_title_preserve_version(title)  # lowercase + χωρίς τόνους
    return any(re.search(p, nt, flags=re.IGNORECASE) for p in EXCLUDE_PATTERNS)

def find_tracks_table(soup: BeautifulSoup):
    for idx, tbl in enumerate(soup.find_all("table"), start=1):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if {"track", "plays", "duration", "release date"}.issubset(set(headers)):
            return tbl
    return None

def table_to_dataframe(tbl: BeautifulSoup) -> pd.DataFrame:
    # helper: πιάσε URL εικόνας και από lazy attrs
    def extract_img_url(td):
        img = td.find("img")
        if not img:
            return None
        for attr in ("src", "data-src", "data-lazy", "data-original"):
            val = img.get(attr)
            if val and isinstance(val, str) and val.strip():
                if val.startswith("//"):  # protocol-relative
                    val = "https:" + val
                return val
        return None

    headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if not tds or len(tds) < len(headers):
            continue

        cells = [td.get_text(" ", strip=True) for td in tds[:len(headers)]]
        cover_url = extract_img_url(tds[0]) if tds else None
        rows.append(cells + [cover_url])

    cols = [h.strip().lower().replace(" ", "_") for h in headers] + ["cover_url"]
    df = pd.DataFrame(rows, columns=cols)
    df = df.rename(columns={
        "track": "title",
        "plays": "plays",
        "duration": "duration",
        "release_date": "release_date"
    })
    df["plays"] = df["plays"].apply(parse_human_int)
    df = df.dropna(subset=["title", "plays"])
    return df

# ---------- MAIN ----------
def main():
    print("▶ START mmv_daily_total.py")
    html = fetch(ARTIST_URL)

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    tbl = find_tracks_table(soup)
    if not tbl:
        print("✗ No valid table found.")
        return

    df = table_to_dataframe(tbl)

    # 1) Exclude συγκεκριμένους τίτλους (Mouri + Remastered 2005)
    before = len(df)
    df = df[~df["title"].apply(should_exclude)]
    removed = before - len(df)
    print(f"[exclude] removed={removed}")

    # 2) Save RAW της ημέρας
    today_str = dt.date.today().strftime("%Y-%m-%d")
    os.makedirs(OUT_TRACKS_DIR, exist_ok=True)
    raw_path = os.path.join(OUT_TRACKS_DIR, f"mmv_track_streams_{today_str}.csv")
    df.to_csv(raw_path, index=False, encoding="utf-8-sig")
    print(f"[save] RAW -> {raw_path} (rows={len(df)})")

    # 3) DEDUPE: ίδιος normalized τίτλος + ίδια διάρκεια => κρατάμε max plays
    df["_key"] = df.apply(lambda r: make_dedupe_key(r.get("title"), r.get("duration")), axis=1)
    df_dedup = (
        df.sort_values("plays", ascending=False)
          .groupby("_key", as_index=False)
          .agg(
              title=("title", "first"),
              plays=("plays", "max"),
              duration=("duration", "first"),
              release_date=("release_date", "first"),
              cover_url=("cover_url", "first"),
          )
    )

    # 4) (προαιρετική) αρίθμηση για ευκολότερο display στο app
    df_dedup.insert(0, "No", range(1, len(df_dedup) + 1))

    dedup_path = os.path.join(OUT_TRACKS_DIR, f"mmv_track_streams_{today_str}_deduped.csv")
    df_dedup.to_csv(dedup_path, index=False, encoding="utf-8-sig")
    print(f"[save] DEDUPED -> {dedup_path} (rows={len(df_dedup)})")

    # 5) Σύνολο (χωρίς τα excluded + χωρίς duplicates)
    deduped_total = int(df_dedup["plays"].fillna(0).astype(int).sum())
    print(f"[total] deduped_total={deduped_total:,}")

    # 6) Ενημέρωση/αντικατάσταση σημερινής γραμμής στο totals CSV
    header = "date,total_plays,daily_delta,source\n"
    lines = []
    prev_total = None

    if os.path.exists(OUT_TOTAL_CSV):
        with open(OUT_TOTAL_CSV, "r", encoding="utf-8-sig") as f:
            lines = [ln for ln in f.readlines() if ln.strip()]
        for ln in reversed(lines):
            if not ln.startswith("date,") and not ln.startswith(today_str + ","):
                try:
                    prev_total = int(ln.split(",")[1])
                    break
                except Exception:
                    pass

    daily_delta = 0 if prev_total is None else (deduped_total - prev_total)
    today_line = f"{today_str},{deduped_total},{daily_delta},MusicMetricsVault.com (personal use)\n"

    if not lines:
        with open(OUT_TOTAL_CSV, "w", encoding="utf-8-sig") as f:
            f.write(header + today_line)
    else:
        replaced = False
        for i in range(len(lines) - 1, -1, -1):
            if lines[i].startswith(today_str + ","):
                lines[i] = today_line
                replaced = True
                break
        if not replaced:
            lines.append(today_line)
        with open(OUT_TOTAL_CSV, "w", encoding="utf-8-sig") as f:
            if not lines[0].startswith("date,"):
                f.write(header)
            f.writelines([ln if ln.endswith("\n") else (ln + "\n") for ln in lines])

    print("✅ DONE")

if __name__ == "__main__":
    main()