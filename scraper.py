import time
import re
import unicodedata
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# Realistic Chrome headers
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]

# Status codes that indicate bot-blocking rather than genuinely dead pages
BOT_BLOCK_CODES = {403, 429, 503, 406, 999}


def clean_text(text: str) -> str:
    """
    Normalise encoding artifacts and strip non-content symbol garbage.

    Handles: NFKC normalisation (smart quotes, ligatures, fullwidth chars),
    icon-font private-use-area characters (Font Awesome etc.), runs of
    punctuation symbols, and stray control characters.
    """
    # Normalise unicode: smart quotes → straight, ligatures → letters, etc.
    text = unicodedata.normalize('NFKC', text)
    # Strip private use area characters used by icon fonts (Font Awesome, etc.)
    text = re.sub(r'[\uE000-\uF8FF]', '', text)
    # Strip sequences of 3+ symbols that aren't useful prose punctuation
    text = re.sub(r'[^\w\s\'\-\.,:;!?()\[\]@#%&]{3,}', ' ', text)
    # Strip control characters (except normal whitespace)
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    # Collapse multiple spaces
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def extract_text(html: str | bytes) -> tuple[int, str]:
    """
    Parse HTML and return (word_count, clean_extract).

    Accepts bytes (preferred — lets lxml auto-detect encoding from the
    <meta charset> declaration) or an already-decoded string.

    Strategy:
    1. Strip all known noise tags
    2. Remove common noisy elements by class/id patterns (cookie banners, nav, etc.)
    3. Target the main content area first before falling back to full body
    4. Filter out short / nav-like text fragments from the result
    """
    soup = BeautifulSoup(html, 'lxml')

    # Remove noise tags entirely
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside',
                     'noscript', 'iframe', 'form', 'svg', 'figure', 'picture',
                     'button', 'select', 'input', 'textarea']):
        tag.decompose()

    # Remove common noise elements by class or id pattern
    noise_re = re.compile(
        r'cookie|consent|gdpr|banner|popup|modal|overlay|notification|alert|'
        r'chat|widget|breadcrumb|pagination|social|share|related|comment|'
        r'search[-_]?form|skip[-_]?link|sr[-_]?only|screen[-_]?reader|'
        r'sidebar|ad[-_]?unit|advertisement|promo|signup|subscribe|newsletter|'
        r'menu|topbar|toolbar|ribbon|sticky',
        re.I,
    )
    for el in soup.find_all(True):
        classes = ' '.join(el.get('class', []))
        el_id = el.get('id', '')
        if noise_re.search(classes) or noise_re.search(el_id):
            el.decompose()

    # Find the most relevant content container
    content = (
        soup.find('main') or
        soup.find(attrs={'role': 'main'}) or
        soup.find('article') or
        soup.find(id=re.compile(r'^(content|main|primary|body-content|page-content)$', re.I)) or
        soup.find(class_=re.compile(
            r'\b(entry-content|post-content|page-content|article-content|'
            r'main-content|site-content|body-content)\b', re.I
        )) or
        soup.body or
        soup
    )

    # Get text as newline-separated lines and filter for quality
    raw = content.get_text(separator='\n', strip=True)

    lines = []
    for line in raw.splitlines():
        line = clean_text(re.sub(r'\s+', ' ', line).strip())
        words = line.split()

        # Skip short fragments (likely labels, nav items, or single words)
        if len(words) < 4:
            continue
        # Skip pipe-separated navigation strings
        if line.count('|') >= 2:
            continue
        # Skip breadcrumb-style lines
        if re.search(r'[»›»>]\s', line):
            continue
        # Skip lines that are mostly numbers/special characters (junk encoding artefacts)
        alpha_ratio = sum(c.isalpha() for c in line) / max(len(line), 1)
        if alpha_ratio < 0.6:
            continue

        lines.append(line)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for line in lines:
        key = line.lower()
        if key not in seen:
            seen.add(key)
            unique.append(line)

    full_text = ' '.join(unique)
    word_count = len(full_text.split())
    extract = full_text[:700] if len(full_text) > 700 else full_text
    return word_count, extract


def _redirect_domain(original_url: str, final_url: str) -> str:
    """
    Return the final domain if the redirect landed on a different domain,
    otherwise return an empty string.
    """
    orig = urlparse(original_url).netloc.lower().removeprefix('www.')
    final = urlparse(final_url).netloc.lower().removeprefix('www.')
    if final and orig != final:
        return urlparse(final_url).netloc.lower()
    return ''


def scrape_with_requests(url: str, timeout: int = 10) -> dict:
    """
    Standard requests-based fetch.
    Rotates user agents with short back-off on bot-blocking status codes.
    Uses response.content (bytes) so lxml can auto-detect page encoding,
    which avoids garbled text from mis-declared charsets.
    """
    session = requests.Session()

    last_status = None
    for attempt, ua in enumerate(USER_AGENTS):
        session.headers.update({**HEADERS, 'User-Agent': ua})
        try:
            response = session.get(url, timeout=timeout, allow_redirects=True)
            status = response.status_code
            last_status = status
            final_url = response.url
            redir = _redirect_domain(url, final_url)

            if status == 200:
                # Use .content (bytes) — lxml reads the charset declaration
                # directly, which prevents symbol garbage from encoding mismatches.
                word_count, extract = extract_text(response.content)
                return {
                    'url': url,
                    'status_code': status,
                    'redirect_domain': redir,
                    'word_count': word_count,
                    'extract': extract,
                    'js_rendered': False,
                    'verified': True,
                }

            if status in BOT_BLOCK_CODES and attempt < len(USER_AGENTS) - 1:
                # Back off before trying next UA
                time.sleep(0.5 * (attempt + 1))
                continue

            # Non-blocking error (404, 410, 301, etc.) — return immediately
            return {
                'url': url,
                'status_code': status,
                'redirect_domain': redir,
                'word_count': 0,
                'extract': '',
                'js_rendered': False,
                'verified': False,
            }

        except requests.exceptions.Timeout:
            return {'url': url, 'status_code': 'TIMEOUT', 'redirect_domain': '', 'word_count': 0, 'extract': '', 'js_rendered': False, 'verified': False}
        except requests.exceptions.ConnectionError:
            return {'url': url, 'status_code': 'CONNECTION_ERROR', 'redirect_domain': '', 'word_count': 0, 'extract': '', 'js_rendered': False, 'verified': False}
        except requests.exceptions.TooManyRedirects:
            return {'url': url, 'status_code': 'TOO_MANY_REDIRECTS', 'redirect_domain': '', 'word_count': 0, 'extract': '', 'js_rendered': False, 'verified': False}
        except Exception as e:
            return {'url': url, 'status_code': 'ERROR', 'redirect_domain': '', 'word_count': 0, 'extract': str(e)[:120], 'js_rendered': False, 'verified': False}

    # All UAs exhausted and still blocked
    return {
        'url': url,
        'status_code': last_status or 'BLOCKED',
        'redirect_domain': '',
        'word_count': 0,
        'extract': '',
        'js_rendered': False,
        'verified': False,
    }


def scrape_with_playwright(url: str, timeout: int = 20) -> dict | None:
    """
    Headless Chrome fetch with anti-detection measures.
    Returns None if Playwright is not installed.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1366,768',
                ],
            )
            context = browser.new_context(
                user_agent=USER_AGENTS[0],
                locale='en-GB',
                timezone_id='Europe/London',
                viewport={'width': 1366, 'height': 768},
                java_script_enabled=True,
                extra_http_headers={
                    'Accept-Language': 'en-GB,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'DNT': '1',
                },
            )

            # Remove webdriver fingerprint signals
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en'] });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()

            # Track actual HTTP status via response events
            status_holder = {'code': None}

            def on_response(response):
                if response.url == page.url or response.request.is_navigation_request():
                    if status_holder['code'] is None:
                        status_holder['code'] = response.status

            page.on('response', on_response)

            try:
                page.goto(url, timeout=timeout * 1000, wait_until='domcontentloaded')
            except PWTimeout:
                browser.close()
                return {'url': url, 'status_code': 'TIMEOUT', 'redirect_domain': '', 'word_count': 0, 'extract': '', 'js_rendered': True, 'verified': False}

            # Wait for content to settle + trigger lazy-loaded elements
            page.wait_for_timeout(1500)
            page.mouse.wheel(0, 600)
            page.wait_for_timeout(600)

            html = page.content()
            final_url = page.url
            actual_status = status_holder['code'] or 200
            redir = _redirect_domain(url, final_url)
            browser.close()

        word_count, extract = extract_text(html)
        return {
            'url': url,
            'status_code': actual_status,
            'redirect_domain': redir,
            'word_count': word_count,
            'extract': extract,
            'js_rendered': True,
            'verified': True,
        }

    except Exception as e:
        return {
            'url': url,
            'status_code': 'PLAYWRIGHT_ERROR',
            'redirect_domain': '',
            'word_count': 0,
            'extract': str(e)[:120],
            'js_rendered': True,
            'verified': False,
        }


def classify(status_code, word_count: int) -> str:
    """Plain-English classification of a URL result."""
    if isinstance(status_code, int):
        if status_code == 200:
            if word_count >= 150:
                return 'LIVE'
            elif word_count >= 30:
                return 'THIN'
            else:
                return 'PARKED'
        if status_code in (301, 302, 307, 308):
            return 'REDIRECT'
        if status_code in (404, 410):
            return 'NOT FOUND'
        if status_code in BOT_BLOCK_CODES:
            return 'BLOCKED'
        if status_code >= 500:
            return 'SERVER ERROR'
        return f'HTTP {status_code}'

    code_str = str(status_code)
    if code_str == 'TIMEOUT':
        return 'TIMEOUT'
    if code_str == 'CONNECTION_ERROR':
        return 'DOWN'
    if code_str in ('TOO_MANY_REDIRECTS',):
        return 'REDIRECT LOOP'
    return 'ERROR'


def scrape_url(url: str, timeout: int = 10, use_playwright: bool = False) -> dict:
    """
    Fetch a URL and return status code, word count, text extract, and classification.

    Flow:
    1. If use_playwright=True, go straight to Playwright.
    2. Otherwise, try requests first (fast path).
    3. If requests returns a bot-blocking code (403/429/503 etc.) or very low word count,
       automatically retry with Playwright to verify before marking as blocked/dead.
    4. Attach a plain-English classification to every result.
    """
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    if use_playwright:
        result = scrape_with_playwright(url, timeout + 10)
        if result:
            result['classification'] = classify(result['status_code'], result['word_count'])
            return result

    # Fast path: requests
    result = scrape_with_requests(url, timeout)

    # Determine if Playwright should verify the result.
    # Covers:
    #   - Known bot-blocking codes (403, 429, 503, 406, 999)
    #   - Any other 4xx that isn't a definitive not-found (404, 410) —
    #     e.g. 401, 406, 451 can all be bot-triggered on live sites
    #   - 200 responses with suspiciously low word counts (parked / JS-only pages)
    status = result['status_code']
    is_ambiguous_4xx = (
        isinstance(status, int) and
        400 <= status < 500 and
        status not in (404, 410)
    )
    needs_verification = (
        is_ambiguous_4xx or
        (status == 200 and result['word_count'] < 50)
    )

    if needs_verification:
        pw_result = scrape_with_playwright(url, timeout + 10)
        if pw_result:
            # Prefer Playwright if it gets a real 200 with more content,
            # or if requests was blocked and Playwright got through at all.
            if (
                pw_result['status_code'] == 200 and pw_result['word_count'] > result['word_count']
            ) or (
                is_ambiguous_4xx and pw_result['status_code'] == 200
            ):
                pw_result['classification'] = classify(pw_result['status_code'], pw_result['word_count'])
                return pw_result

    result['classification'] = classify(result['status_code'], result['word_count'])
    return result
