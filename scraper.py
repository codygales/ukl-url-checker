import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def scrape_url(url: str, timeout: int = 10) -> dict:
    """Fetch a URL and return status code, word count, and text extract."""
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        status_code = response.status_code

        if status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove non-content tags
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript', 'iframe']):
                tag.decompose()

            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text).strip()

            word_count = len(text.split())
            extract = text[:600] if len(text) > 600 else text
        else:
            word_count = 0
            extract = ''

        return {
            'url': url,
            'status_code': status_code,
            'word_count': word_count,
            'extract': extract,
        }

    except requests.exceptions.Timeout:
        return {'url': url, 'status_code': 'TIMEOUT', 'word_count': 0, 'extract': ''}
    except requests.exceptions.ConnectionError:
        return {'url': url, 'status_code': 'CONNECTION_ERROR', 'word_count': 0, 'extract': ''}
    except requests.exceptions.TooManyRedirects:
        return {'url': url, 'status_code': 'TOO_MANY_REDIRECTS', 'word_count': 0, 'extract': ''}
    except Exception as e:
        return {'url': url, 'status_code': f'ERROR', 'word_count': 0, 'extract': str(e)[:100]}
