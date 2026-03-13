import streamlit as st
import pandas as pd
import time
import re
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests as req

from scraper import scrape_url

st.set_page_config(page_title="URL Checker — UKL", layout="wide", page_icon="🔍")

# ── Session state init ──────────────────────────────────────────────────────

def init():
    defaults = {
        'urls': [],
        'results': [],
        'status': 'idle',       # idle | running | paused | stopped | done
        'current_index': 0,
        'crawl_end': 0,
        'delay': 0.0,
        'batch_size': 100,
        'workers': 10,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init()

# ── Helpers ─────────────────────────────────────────────────────────────────

def load_csv(file) -> list:
    try:
        df = pd.read_csv(file, header=None)
        return df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        return []

def load_gsheet(url: str) -> list:
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    if not match:
        st.error("Invalid Google Sheet URL — make sure you paste the full share link.")
        return []
    sheet_id = match.group(1)

    # Try to detect gid (tab) param
    gid_match = re.search(r'gid=(\d+)', url)
    gid = gid_match.group(1) if gid_match else '0'

    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    try:
        r = req.get(csv_url, timeout=20)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text), header=None)
        return df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
    except Exception as e:
        st.error(f"Could not load Google Sheet: {e}. Make sure sharing is set to 'Anyone with the link can view'.")
        return []

def results_to_df() -> pd.DataFrame:
    if not st.session_state.results:
        return pd.DataFrame(columns=['URL', 'Status Code', 'Word Count', 'Extract'])
    return pd.DataFrame(
        st.session_state.results,
        columns=['URL', 'Status Code', 'Word Count', 'Extract']
    )

def set_urls(urls: list):
    st.session_state.urls = urls
    st.session_state.results = []
    st.session_state.current_index = 0
    st.session_state.crawl_end = len(urls)
    st.session_state.status = 'idle'

# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🔍 URL Checker")
    st.caption("UKL — Site List Validator")
    st.divider()

    # Input
    st.subheader("1. Load URLs")
    input_method = st.radio("Source", ["Upload CSV", "Google Sheet URL", "Enter Manually"], horizontal=False)

    if input_method == "Upload CSV":
        uploaded = st.file_uploader("CSV file (URLs in column A)", type=["csv"])
        if uploaded:
            if st.button("Load CSV", use_container_width=True):
                urls = load_csv(uploaded)
                if urls:
                    set_urls(urls)
                    st.success(f"Loaded {len(urls):,} URLs")

    elif input_method == "Google Sheet URL":
        gsheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")
        st.caption("Sheet must be shared: *Anyone with the link can view*")
        if st.button("Load Sheet", use_container_width=True):
            if gsheet_url:
                urls = load_gsheet(gsheet_url)
                if urls:
                    set_urls(urls)
                    st.success(f"Loaded {len(urls):,} URLs")

    else:
        manual_input = st.text_area(
            "Paste URLs — one per line or copied direct from Google Sheets",
            placeholder="https://example.com\nhttps://another-site.co.uk\nhttps://third-site.com\n...",
            height=250
        )

        # Live count as user pastes
        if manual_input:
            preview_urls = []
            for line in manual_input.splitlines():
                # Take first cell only if pasted from multi-column sheet (tab separated)
                cell = line.split('\t')[0].strip()
                if cell:
                    preview_urls.append(cell)
            st.caption(f"{len(preview_urls):,} URLs detected")

        if st.button("Load URLs", use_container_width=True):
            urls = []
            for line in manual_input.splitlines():
                cell = line.split('\t')[0].strip()
                if cell:
                    urls.append(cell)
            if urls:
                set_urls(urls)
                st.success(f"Loaded {len(urls):,} URLs")
            else:
                st.warning("No URLs found — paste one URL per line.")

    total = len(st.session_state.urls)

    st.divider()

    # Crawl settings
    st.subheader("2. Crawl Settings")

    crawl_mode = st.selectbox("Mode", ["All URLs", "First N URLs", "Row Range"])

    crawl_start = 0
    crawl_end = total

    if crawl_mode == "First N URLs":
        n = st.number_input("Number of URLs", min_value=1, max_value=max(total, 1), value=min(100, max(total, 1)), step=1)
        crawl_start = 0
        crawl_end = int(n)
    elif crawl_mode == "Row Range":
        c1, c2 = st.columns(2)
        with c1:
            crawl_start = st.number_input("From", min_value=1, max_value=max(total, 1), value=1, step=1) - 1
        with c2:
            crawl_end = st.number_input("To", min_value=1, max_value=max(total, 1), value=min(500, max(total, 1)), step=1)

    workers = st.select_slider("Concurrent workers", options=[1, 5, 10, 20, 25, 50], value=10,
        help="How many URLs to check simultaneously. 10-25 is a good balance for large lists.")
    batch_size = st.select_slider("Batch size", options=[25, 50, 100, 250, 500], value=100,
        help="URLs processed per UI refresh. Higher = faster, less frequent progress updates.")
    delay = st.slider("Delay between batches (s)", 0.0, 5.0, 0.0, 0.1,
        help="With concurrent workers, delay between requests is less important. Keep at 0 for speed.")
    use_playwright = st.toggle(
        "JS rendering (Playwright)",
        value=False,
        help="Enable for JS-heavy sites like Screwfix. Slower but extracts full page content. Auto-fallback is always on for sites returning <50 words."
    )

    st.divider()

    # Controls
    st.subheader("3. Controls")

    c1, c2 = st.columns(2)
    with c1:
        status = st.session_state.status
        if status in ('idle', 'stopped', 'done'):
            if st.button("▶ Start", use_container_width=True, type="primary", disabled=(total == 0)):
                st.session_state.results = []
                st.session_state.current_index = crawl_start
                st.session_state.crawl_end = crawl_end
                st.session_state.delay = delay
                st.session_state.batch_size = batch_size
                st.session_state.workers = workers
                st.session_state.use_playwright = use_playwright
                st.session_state.status = 'running'
                st.rerun()
        elif status == 'paused':
            if st.button("▶ Continue", use_container_width=True, type="primary"):
                st.session_state.status = 'running'
                st.rerun()
        elif status == 'running':
            if st.button("⏸ Pause", use_container_width=True):
                st.session_state.status = 'paused'
                st.rerun()

    with c2:
        if st.button("⏹ Stop", use_container_width=True, disabled=(st.session_state.status == 'idle')):
            st.session_state.status = 'stopped'
            st.rerun()

    if total > 0:
        st.caption(f"{total:,} URLs loaded")

# ── Main area ────────────────────────────────────────────────────────────────

STATUS_LABELS = {
    'idle':    '⚪ Ready — configure settings and press Start',
    'running': '🟢 Running...',
    'paused':  '🟡 Paused',
    'stopped': '🔴 Stopped',
    'done':    '✅ Complete',
}

completed = len(st.session_state.results)
crawl_end_idx = st.session_state.crawl_end
crawl_span = max(crawl_end_idx - st.session_state.get('current_index', 0) + completed, 1)

if total > 0:
    st.subheader("Progress")
    progress_val = min(completed / max(crawl_end_idx - (crawl_end_idx - completed - (st.session_state.current_index - completed)), 1), 1.0)

    # Simpler progress calc
    target = crawl_end_idx
    start_offset = target - (completed + max(0, crawl_end_idx - st.session_state.current_index))
    progress_pct = completed / max(target - start_offset, 1)

    st.progress(min(progress_pct, 1.0))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Status", STATUS_LABELS.get(st.session_state.status, ''))
    col2.metric("Completed", f"{completed:,}")
    col3.metric("Remaining", f"{max(0, crawl_end_idx - st.session_state.current_index):,}")
    col4.metric("Total loaded", f"{total:,}")
else:
    st.info("Load a CSV or Google Sheet from the sidebar to get started.")

# ── Crawl loop ───────────────────────────────────────────────────────────────

if st.session_state.status == 'running':
    end_idx = st.session_state.crawl_end
    batch = st.session_state.batch_size
    wait = st.session_state.delay
    pw = st.session_state.get('use_playwright', False)
    max_workers = st.session_state.get('workers', 10)

    # Grab next batch of URLs
    start = st.session_state.current_index
    end = min(start + batch, end_idx)
    batch_urls = st.session_state.urls[start:end]

    # Fetch concurrently
    batch_results = [None] * len(batch_urls)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(scrape_url, url, 10, pw): i
            for i, url in enumerate(batch_urls)
        }
        for future in as_completed(future_to_idx):
            i = future_to_idx[future]
            result = future.result()
            batch_results[i] = [
                result['url'],
                result['status_code'],
                result['word_count'],
                result['extract'],
            ]

    st.session_state.results.extend(batch_results)
    st.session_state.current_index = end

    if wait > 0:
        time.sleep(wait)

    if st.session_state.current_index >= end_idx:
        st.session_state.status = 'done'

    st.rerun()

# ── Results table ─────────────────────────────────────────────────────────────

if st.session_state.results:
    df = results_to_df()

    st.divider()
    st.subheader(f"Results — {len(df):,} URLs checked")

    # Export
    col1, col2 = st.columns([1, 6])
    with col1:
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        st.download_button("⬇ Export CSV", csv_bytes, "ukl_url_check.csv", "text/csv", use_container_width=True)

    # Status filter
    all_statuses = sorted(df['Status Code'].astype(str).unique().tolist())
    selected = st.multiselect("Filter by Status Code", options=all_statuses, default=all_statuses)
    filtered = df[df['Status Code'].astype(str).isin(selected)]

    # Summary counts
    summary = df['Status Code'].astype(str).value_counts().reset_index()
    summary.columns = ['Status Code', 'Count']
    with st.expander("Status Code Summary"):
        st.dataframe(summary, use_container_width=True, hide_index=True)

    st.dataframe(
        filtered,
        use_container_width=True,
        height=500,
        column_config={
            'URL': st.column_config.LinkColumn('URL'),
            'Status Code': st.column_config.TextColumn('Status Code', width='small'),
            'Word Count': st.column_config.NumberColumn('Word Count', width='small'),
            'Extract': st.column_config.TextColumn('Extract', width='large'),
        }
    )
