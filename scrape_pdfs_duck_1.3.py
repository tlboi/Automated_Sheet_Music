#!/usr/bin/env python3
"""
scrape_pdfs_duck8_dedupe.py

Reads a CSV sight‐reading list (Level,Composer,Title), then for each piece:
  1) Tries DuckDuckGo (via duckduckgo-search’s DDGS, falls back to HTML scrape)
  2) Tries IMSLP’s built-in search page
  3) Tries Internet Archive’s API
  4) Tries Mutopia Project’s search page

If none yield PDFs, it then shortens the title one word at a time
(from the end) and retries the full cascade, until it either finds
up to MAX_PDFS_PER_PIECE **unique‐size** PDFs or the title is one word.

Downloads into a `pdfs/` folder naming:
  Level - Title - Composer - oldfilename.pdf

Also reports, in real time, how many PDFs were downloaded via each source,
and if after all fallbacks a piece still has fewer than MAX_PDFS_PER_PIECE
PDFs, moves its CSV entry to the bottom for the next run.
"""

import csv
import os
import re
import time
import glob
import requests
from requests.exceptions import RequestException, ReadTimeout
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
from urllib.parse import quote, urlparse, parse_qs, unquote

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------
INPUT_CSV            = 'sightreading_list.csv'
OUTPUT_DIR           = 'pdfs'
MAX_PDFS_PER_PIECE   = 3
RESULTS_PER_QUERY    = 30
DOWNLOAD_DELAY       = 1     # seconds between downloads
ENTRY_DELAY          = 1     # seconds between each piece
DDGS_PAUSE           = 1     # seconds before each DDGS call
HTML_ENDPOINT        = 'https://duckduckgo.com/html/'
HEADERS              = {
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://duckduckgo.com/html/'
}

# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------
def sanitize_filename(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", s)

def extract_real_url(ddg_href: str) -> str:
    p = urlparse(ddg_href)
    qs = parse_qs(p.query)
    return unquote(qs.get('uddg', [ddg_href])[0])

def is_relevant_pdf(url: str, title: str, composer: str) -> bool:
    """
    Very basic filter: the URL must contain
      • at least half the words from the title, OR
      • the composer's name, OR
      • one of the generic sheet-music keywords.
    """
    text = unquote(url).lower()

    # 1) Title words
    words = re.findall(r'\w+', title.lower())
    match_count = sum(1 for w in words if w in text)
    if match_count >= len(words) / 2:
        return True

    # 2) Composer (no spaces)
    comp = composer.lower().replace(" ", "")
    if comp in text:
        return True

    # 3) Generic keywords
    for kw in ("piano", "arranged", "composer", "score", "sheet", "music"):
        if kw in text:
            return True

    return False

def ddg_html_search(query: str, max_results: int = RESULTS_PER_QUERY):
    try:
        resp = requests.get(
            HTML_ENDPOINT,
            params={'q': query},
            headers=HEADERS,
            timeout=2
        )
        resp.raise_for_status()
    except (ReadTimeout, RequestException) as e:
        print(f"      ⚠ DDG HTML failed ({e!r}), skipping")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    pdfs = []
    for a in soup.find_all('a', class_='result__a', href=True):
        real = extract_real_url(a['href'])
        if real.lower().endswith('.pdf'):
            pdfs.append(real)
            if len(pdfs) >= max_results:
                break
    return pdfs

class DDGSTimeout(DDGS):
    def __enter__(self):
        super().__enter__()
        orig = self.session.request
        def timed_request(method, url, **kwargs):
            kwargs.setdefault("timeout", 2)
            return orig(method, url, **kwargs)
        self.session.request = timed_request
        return self

def ddg_search_pdf_urls(query: str, max_results: int = RESULTS_PER_QUERY):
    time.sleep(DDGS_PAUSE)
    try:
        pdfs = []
        with DDGSTimeout() as ddgs:
            for res in ddgs.text(query, max_results=max_results):
                href = res.get('href','')
                if href.lower().endswith('.pdf'):
                    pdfs.append(href)
                    if len(pdfs) >= max_results:
                        break
        if pdfs:
            return pdfs
    except Exception:
        pass
    return ddg_html_search(query, max_results)

def get_remote_pdf_size(url: str) -> int | None:
    try:
        r = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        cl = r.headers.get('Content-Length')
        return int(cl) if cl and cl.isdigit() else None
    except Exception:
        return None

def maybe_download_pdf(url: str, dest_path: str, seen_sizes: set[int]) -> bool:
    size = get_remote_pdf_size(url)
    if size is not None and size in seen_sizes:
        print(f"      ↳ Skipping duplicate (size={size})")
        return False

    try:
        r = requests.get(url, stream=True, timeout=15, headers=HEADERS)
        ctype = r.headers.get('Content-Type','').lower()
        if r.status_code == 200 and 'pdf' in ctype:
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            if size is None:
                size = os.path.getsize(dest_path)
            if size in seen_sizes:
                os.remove(dest_path)
                print(f"      ↳ Removed duplicate after download (size={size})")
                return False
            seen_sizes.add(size)
            print(f"      ✅ Saved (size={size}): {os.path.basename(dest_path)}")
            return True
    except Exception:
        pass

    print("      ❌ Failed download")
    return False

# ------------------------------------------------------------------
# DEBUGGED FALLBACKS
# ------------------------------------------------------------------
import re as _re
from urllib.parse import quote as _quote

def search_imslp(title: str, composer: str, max_results: int = RESULTS_PER_QUERY):
    def clean(q):
        return _quote(_re.sub(r"[^\w\s]", "", q))
    def do_search(qstr):
        url = f"https://imslp.org/index.php?title=Special:Search&search={clean(qstr)}"
        print(f"[DEBUG][IMSLP] searching → {url}")
        r = requests.get(url, headers=HEADERS, timeout=10); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        divs = soup.find_all("div", class_="mw-search-result-heading")
        return [div.find("a")["href"] for div in divs if div.find("a")]
    hits = do_search(f"{title} {composer}")
    if not hits:
        print("[DEBUG][IMSLP] no hits with composer, retrying title-only")
        hits = do_search(title)
    pdfs = []
    for rel in hits[:max_results]:
        page_url = "https://imslp.org" + rel
        print(f"[DEBUG][IMSLP] visiting piece page → {page_url}")
        pr = requests.get(page_url, headers=HEADERS, timeout=10)
        ps = BeautifulSoup(pr.text, "html.parser")
        fp = ps.find("a", href=_re.compile(r"/index\.php\?title=Special:FilePath/.+\.pdf"))
        if fp:
            pdfs.append("https://imslp.org" + fp["href"])
        if len(pdfs) >= max_results:
            break
    print(f"[DEBUG][IMSLP] total PDFs found: {len(pdfs)}")
    return pdfs

def search_archive_org(title: str, composer: str, max_results: int = 10):
    endpoint = "https://archive.org/advancedsearch.php"
    q = f'"{title}" "{composer}" AND mediatype:texts'
    params = {'q': q, 'fl[]': 'identifier', 'rows': max_results, 'output': 'json'}
    print(f"[DEBUG][ARCHIVE] GET {endpoint} params={params}")
    r = requests.get(endpoint, params=params, headers=HEADERS, timeout=10); r.raise_for_status()
    docs = r.json().get('response', {}).get('docs', [])
    print(f"[DEBUG][ARCHIVE] docs returned: {len(docs)}")
    pdfs = []
    for doc in docs:
        ident = doc.get('identifier')
        if ident:
            pdfs.append(f"https://archive.org/download/{ident}/{ident}_text.pdf")
        if len(pdfs) >= max_results:
            break
    print(f"[DEBUG][ARCHIVE] PDFs built: {len(pdfs)}")
    return pdfs

def search_mutopia(title: str, composer: str, max_results: int = RESULTS_PER_QUERY):
    url = f"http://www.mutopiaproject.org/cgibin/piece-info.cgi?searchtext={_quote(title)}"
    print(f"[DEBUG][MUTOPIA] GET {url}")
    r = requests.get(url, headers=HEADERS, timeout=10); r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    pdfs = []
    for a in soup.select('a[href$=".pdf"]'):
        href = a['href']
        pdfs.append(href if href.startswith('http') else 'http://www.mutopiaproject.org'+href)
        if len(pdfs) >= max_results:
            break
    print(f"[DEBUG][MUTOPIA] found {len(pdfs)}")
    return pdfs

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # counters for real-time report
    counters = {
        'DDG':       0,
        'IMSLP':     0,
        'Archive':   0,
        'Mutopia':   0
    }

    templates = [
       # English
       # English
    "{title} {composer} sheet music pdf",
    "{title} {composer} piano sheet music",
    "{title} {composer} piano sheet music pdf",
"{title} {composer} score pdf download",
"{title} {composer} full score pdf",
"{title} {composer} piano score pdf",
"{title} {composer} pdf",
"{title} {composer} piano solo pdf",
"{title} {composer} piano pdf",
"{title} pdf sheet music",
"{title} {composer} pdf partition",
"{title} {composer} partition piano",
"{title} {composer} piano partition pdf",
"{title} {composer} partituras pdf download",
"{title} {composer} partituras pdf",
"{title} {composer} piano notenblätter pdf",
### Spanish
"{title} {composer} partituras piano pdf",
"{title} {composer} partitura piano pdf",
### French
"{title} {composer} partition pdf",
"{title} {composer} partition piano pdf",
### Italian
"{title} {composer} spartito pdf",
"{title} {composer} spartito pianoforte pdf",
### Portuguese
"{title} {composer} partitura pdf",
"{title} {composer} partitura piano pdf",
### German
"{title} {composer} notenblätter pdf",
"{title} {composer} notenblatt pdf",
### Russian
"{title} {composer} ноты pdf",
"{title} {composer} ноты пианино pdf",
### Chinese
"{title} {composer} 乐谱 pdf",
"{title} {composer} 钢琴 乐谱 pdf",
### Japanese
"{title} {composer} 楽譜 pdf",
"{title} {composer} ピアノ 乐谱 pdf",
#
## Arabic
"{title} {composer} نوتة موسيقية pdf"
"{title} {composer} pdf",
    ]

    # read all entries (no header row)
    with open(INPUT_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    complete_rows = []
    incomplete_rows = []

    for row in rows:
        level, composer, title = row[0].strip(), row[1].strip(), row[2].strip()

        # Resume: skip or pick up where left off
        pattern = f"{level} - {sanitize_filename(title)} - {sanitize_filename(composer)} - *.pdf"
        existing = glob.glob(os.path.join(OUTPUT_DIR, pattern))
        seen_sizes = set()
        for fp in existing:
            try:
                seen_sizes.add(os.path.getsize(fp))
            except Exception:
                pass
        saved = len(existing)
        if saved >= MAX_PDFS_PER_PIECE:
            print(f"\n▶ Skipping “{title}” — already have {saved} PDFs")
            complete_rows.append(row)
            continue

        print(f"\n▶ Processing: {title} — {composer} (Level {level})")

        def try_search(curr_title):
            nonlocal saved, counters

            # 1) DuckDuckGo
            for tmpl in templates:
                if saved >= MAX_PDFS_PER_PIECE:
                    return
                q = tmpl.format(title=curr_title, composer=composer)
                print(f"  • DDG query: {q}")
                for url in ddg_search_pdf_urls(q):
                    if not is_relevant_pdf(url, curr_title, composer):
                        print(f"      ↳ Skipping irrelevant PDF: {url}")
                        continue
                    if saved >= MAX_PDFS_PER_PIECE:
                        return
                    remote_name = sanitize_filename(os.path.basename(urlparse(url).path))
                    fn = f"{level} - {sanitize_filename(title)} - {sanitize_filename(composer)} - {remote_name}"
                    path = os.path.join(OUTPUT_DIR, fn)
                    if maybe_download_pdf(url, path, seen_sizes):
                        saved += 1
                        counters['DDG'] += 1
                        print(f"      [REPORT] DuckDuckGo count = {counters['DDG']}")
                    time.sleep(DOWNLOAD_DELAY)

            # 2) IMSLP
            if saved < MAX_PDFS_PER_PIECE:
                print("  • Falling fallback to IMSLP")
                for url in search_imslp(curr_title, composer):
                    if not is_relevant_pdf(url, curr_title, composer):
                        print(f"      ↳ Skipping irrelevant PDF: {url}")
                        continue
                    if saved >= MAX_PDFS_PER_PIECE:
                        return
                    remote_name = sanitize_filename(os.path.basename(urlparse(url).path))
                    fn = f"{level} - {sanitize_filename(title)} - {sanitize_filename(composer)} - {remote_name}"
                    path = os.path.join(OUTPUT_DIR, fn)
                    if maybe_download_pdf(url, path, seen_sizes):
                        saved += 1
                        counters['IMSLP'] += 1
                        print(f"      [REPORT] IMSLP count = {counters['IMSLP']}")
                    time.sleep(DOWNLOAD_DELAY)

            # 3) Internet Archive
            if saved < MAX_PDFS_PER_PIECE:
                print("  • Falling fallback to Internet Archive")
                for url in search_archive_org(curr_title, composer):
                    if not is_relevant_pdf(url, curr_title, composer):
                        print(f"      ↳ Skipping irrelevant PDF: {url}")
                        continue
                    if saved >= MAX_PDFS_PER_PIECE:
                        return
                    remote_name = sanitize_filename(os.path.basename(urlparse(url).path))
                    fn = f"{level} - {sanitize_filename(title)} - {sanitize_filename(composer)} - {remote_name}"
                    path = os.path.join(OUTPUT_DIR, fn)
                    if maybe_download_pdf(url, path, seen_sizes):
                        saved += 1
                        counters['Archive'] += 1
                        print(f"      [REPORT] Archive count = {counters['Archive']}")
                    time.sleep(DOWNLOAD_DELAY)

            # 4) Mutopia
            if saved < MAX_PDFS_PER_PIECE:
                print("  • Falling fallback to Mutopia")
                for url in search_mutopia(curr_title, composer):
                    if not is_relevant_pdf(url, curr_title, composer):
                        print(f"      ↳ Skipping irrelevant PDF: {url}")
                        continue
                    if saved >= MAX_PDFS_PER_PIECE:
                        return
                    remote_name = sanitize_filename(os.path.basename(urlparse(url).path))
                    fn = f"{level} - {sanitize_filename(title)} - {sanitize_filename(composer)} - {remote_name}"
                    path = os.path.join(OUTPUT_DIR, fn)
                    if maybe_download_pdf(url, path, seen_sizes):
                        saved += 1
                        counters['Mutopia'] += 1
                        print(f"      [REPORT] Mutopia count = {counters['Mutopia']}")
                    time.sleep(DOWNLOAD_DELAY)

        try:
            # first pass: full title
            try_search(title)

            # then progressively trim
            words = title.split()
            while saved < MAX_PDFS_PER_PIECE and len(words) > 1:
                words.pop()
                shortened = " ".join(words)
                print(f"  • Falling back with shortened title: {shortened!r}")
                try_search(shortened)

            print(f"  ✔ {saved} unique PDF(s) saved for “{title}”")
        except Exception as e:
            print(f"  ❌ Unhandled error for “{title}”: {e!r}")

        # after all attempts, categorize row
        if saved < MAX_PDFS_PER_PIECE:
            incomplete_rows.append(row)
        else:
            complete_rows.append(row)

        time.sleep(ENTRY_DELAY)

    # rewrite CSV so incomplete entries go last
    with open(INPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for r in complete_rows + incomplete_rows:
            writer.writerow(r)

if __name__ == '__main__':
    main()
