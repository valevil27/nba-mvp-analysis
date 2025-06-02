import random
import sys
from io import StringIO
from pathlib import Path
from time import sleep

import pandas as pd
import requests

from map_teams import map_teams_short

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
    "Referer": "https://www.google.com",
    "Accept-Language": "es-ES,es;q=0.9",
}

proxies = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}


def clean_team_name(s: str) -> str:
    index = s.find("*")
    if index == -1:
        index = s.find("(") - 1
        if index == -2:
            return s
    return s[:index]


def preprocess_season(df: pd.DataFrame, mapper=map_teams_short) -> pd.DataFrame:
    df = df.copy()
    conf = "East" if df.columns[0].lower().startswith("east") else "West"
    reset_idx = df[~df["W"].astype(str).str.isnumeric()].index.to_list()
    rank = 1
    for i, r in df.iterrows():
        if i in reset_idx:
            rank = 1
            continue
        df.loc[i, "Rank"] = rank  # type: ignore
        rank += 1
    df = df[df["W"].astype(str).str.isnumeric()]
    df["Rank"] = df["Rank"].astype(int)
    old_cols = df.columns.tolist()
    old_cols[0] = "Team"
    df.columns = old_cols
    df["Team"] = df["Team"].apply(clean_team_name)
    df["Team"] = df["Team"].map(mapper)
    df["Conference"] = conf
    df = df.drop(columns=["W", "L", "GB", "SRS"])
    df = df.rename(columns={"W/L%": "WinRatio", "PS/G": "PPG", "PA/G": "PAG"})
    for c in ["WinRatio", "PPG", "PAG"]:
        df[c] = df[c].astype(float)
    return df


def get_mvp_data(path: Path, years: tuple[int, int]):
    if not path.exists():
        path.mkdir(parents=True)

    for year in range(*years):
        filepath = path / f"mvp_{year}.csv"
        url = f"https://www.basketball-reference.com/awards/awards_{year}.html"
        response = requests.get(url, headers=headers, proxies=proxies)
        assert response.status_code == 200, (
            f"Error - { response.status_code = }"
        )
        df = pd.read_html(response.text, header=1)[0].head(10)
        df["Year"] = year
        df.to_csv(filepath, index=False)
        sleep(random.uniform(3, 7))


def get_seasons(path: Path, years: tuple[int, int]):
    if not path.exists():
        path.mkdir(parents=True)

    for year in range(*years):
        filepath = path / f"season_{year}.csv"
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}.html"
        response = requests.get(url, headers=headers, proxies=proxies)
        assert response.status_code == 200, (
            f"Error - { response.status_code = }"
        )
        dfs = pd.read_html(StringIO(response.text))[:2]
        df = pd.concat(
            [
                preprocess_season(dfs[0]),
                preprocess_season(dfs[1]),
            ]
        )
        df["Year"] = year
        df.to_csv(filepath, index=False)
        print(f"- Archivo para el año {year}: {filepath}")
        print(f"\tConferencias: {df['Conference'].unique()}\n")
        sleep(random.uniform(3, 7))


commands = ["all", "seasons", "mvps"]


def main():
    args = sys.argv
    assert len(args) == 2 and args[1] in commands, (
        f"Expected one argument of: {', '.join(commands)}"
    )
    data_path = Path("data")
    mvp_path = data_path / "mvp"
    teams_path = data_path / "season"
    years = (1980, 2026)
    match args[1]:
        case "all":
            get_mvp_data(mvp_path, years)
        case "mvps":
            get_mvp_data(mvp_path, years)
        case "seasons":
            get_seasons(teams_path, years)


if __name__ == "__main__":
    main()
