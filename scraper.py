import requests
from bs4 import BeautifulSoup
import re

# Realistic browser headers to avoid 403 blocks
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
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
}

# Fallback user agents to rotate on 403
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
]


def extract_text(html: str) -> tuple[int, str]:
    """Parse HTML and return (word_count, extract)."""
    soup = BeautifulSoup(html, 'lxml')

    # Remove non-content tags
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript', 'iframe']):
        tag.decompose()

    text = soup.get_text(separator=' ', strip=True)
    text = re.sub(r'\s+', ' ', text).strip()

    word_count = len(text.split())
    extract = text[:600] if len(text) > 600 else text
    return word_count, extract


def scrape_with_requests(url: str, timeout: int = 10) -> dict:
    """Standard requests-based fetch with header rotation on 403."""
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
        status_code = response.status_code

        # Retry with rotated user agents on 403
        if status_code == 403:
            for ua in USER_AGENTS[1:]:
                session.headers.update({'User-Agent': ua})
                response = session.get(url, timeout=timeout, allow_redirects=True)
                if response.status_code != 403:
                    status_code = response.status_code
                    break

        if status_code == 200:
            word_count, extract = extract_text(response.text)
            js_rendered = word_count < 50
            return {
                'url': url,
                'status_code': status_code,
                'word_count': word_count,
                'extract': extract,
                'js_rendered': js_rendered,
            }
        else:
            return {'url': url, 'status_code': status_code, 'word_count': 0, 'extract': '', 'js_rendered': False}

    except requests.exceptions.Timeout:
        return {'url': url, 'status_code': 'TIMEOUT', 'word_count': 0, 'extract': '', 'js_rendered': False}
    except requests.exceptions.ConnectionError:
        return {'url': url, 'status_code': 'CONNECTION_ERROR', 'word_count': 0, 'extract': '', 'js_rendered': False}
    except requests.exceptions.TooManyRedirects:
        return {'url': url, 'status_code': 'TOO_MANY_REDIRECTS', 'word_count': 0, 'extract': '', 'js_rendered': False}
    except Exception as e:
        return {'url': url, 'status_code': 'ERROR', 'word_count': 0, 'extract': str(e)[:100], 'js_rendered': False}


def scrape_with_playwright(url: str, timeout: int = 15) -> dict:
    """Headless browser fetch mimicking a real Chrome user to bypass bot detection."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None  # Playwright not installed

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                ]
            )
            context = browser.new_context(
                user_agent=USER_AGENTS[0],
                locale='en-GB',
                timezone_id='Europe/London',
                viewport={'width': 1366, 'height': 768},
                java_script_enabled=True,
                # Mimic real browser capabilities
                extra_http_headers={
                    'Accept-Language': 'en-GB,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                }
            )

            # Remove webdriver fingerprint
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en'] });
            """)

            page = context.new_page()
            page.goto(url, timeout=timeout * 1000, wait_until='domcontentloaded')

            # Human-like: wait a moment for content to settle
            page.wait_for_timeout(1500)

            # Scroll down slightly to trigger lazy-loaded content
            page.mouse.wheel(0, 500)
            page.wait_for_timeout(800)

            html = page.content()
            browser.close()

        word_count, extract = extract_text(html)
        return {
            'url': url,
            'status_code': 200,
            'word_count': word_count,
            'extract': extract,
            'js_rendered': True,
        }
    except Exception as e:
        return {'url': url, 'status_code': 'PLAYWRIGHT_ERROR', 'word_count': 0, 'extract': str(e)[:100], 'js_rendered': True}


def scrape_url(url: str, timeout: int = 10, use_playwright: bool = False) -> dict:
    """
    Fetch a URL and return status code, word count, and text extract.
    If use_playwright is True, uses headless Chrome for JS-rendered sites.
    Otherwise uses requests with header rotation for 403 handling.
    Falls back to Playwright automatically if word count < 50 and Playwright is available.
    """
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    if use_playwright:
        result = scrape_with_playwright(url, timeout)
        if result:
            return result

    result = scrape_with_requests(url, timeout)

    # Auto-fallback to Playwright if word count suspiciously low (JS-rendered site)
    if result['status_code'] == 200 and result['js_rendered'] and not use_playwright:
        pw_result = scrape_with_playwright(url, timeout)
        if pw_result and pw_result['word_count'] > result['word_count']:
            return pw_result

    return result
