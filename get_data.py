import random
import requests
from pathlib import Path
from time import sleep
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
    "Referer": "https://www.google.com",
    "Accept-Language": "es-ES,es;q=0.9",
}

proxies = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}

years_lim = (1980, 2026)
data_path = Path("data")

for year in range(*years_lim):
    filepath = data_path / f"mvp_{year}.csv"
    url = f"https://www.basketball-reference.com/awards/awards_{year}.html"
    response = requests.get(url, headers=headers, proxies=proxies)
    assert response.status_code == 200, f"Error - { response.status_code = }"
    df = pd.read_html(response.text, header=1)[0].head(10)
    df["Year"] = year
    df.to_csv(filepath, index=False)
    sleep(random.uniform(3, 7))
