# URL Checker — UKL

Streamlit app for bulk URL validation against a 20k+ site list.
Checks status codes, word counts, and extracts page copy to identify live vs dead/parked/sold sites.

## Setup

```bash
cd UKL/url-checker
pip install -r requirements.txt
streamlit run app.py
```

## Usage
1. Upload a CSV (URLs in column A) or paste a Google Sheet URL
   - Google Sheet must be set to "Anyone with the link can view"
2. Choose crawl mode: All / First N / Row Range
3. Set delay (0.5s default — polite crawling)
4. Press Start — pause, continue, or stop at any time
5. Export results to CSV when done

## Output Columns
| Column | Description |
|--------|-------------|
| URL | The URL checked |
| Status Code | 200, 301, 404, TIMEOUT, CONNECTION_ERROR, etc. |
| Word Count | Number of visible words on the page |
| Extract | First 600 characters of visible page text |

## Interpreting Results
- **200 + high word count** → live, active site
- **200 + low word count (<100)** → parked, for sale, or thin page
- **301/302** → redirecting (may have moved)
- **404/410** → removed or dead
- **CONNECTION_ERROR / TIMEOUT** → site down or blocked
