# bfg-crawl

A simple web crawler that fetches pages and stores them in SQLite.

## Features

- Multi-threaded crawling
- Rate limiting support
- SQLite database storage

## Install

```bash
pip install bfg-crawl
```

## Usage

```python
from crawl import Crawler, RateLimitingLoader

loader = ALoaderForThisWebsite()
crawler = Crawler("pages.db", RateLimitingLoader(loader), concurrency=5)

crawler.run()
```

The crawler reads URLs from a SQLite database and saves the page content back to the database.
