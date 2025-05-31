from io import StringIO
import random
import sys
import requests
from pathlib import Path
from time import sleep
import pandas as pd

map_teams = {
    "LAL": "Los Angeles Lakers",
    "PHI": "Philadelphia 76ers",
    "SAS": "San Antonio Spurs",
    "BOS": "Boston Celtics",
    "SEA": "Seattle SuperSonics",
    "ATL": "Atlanta Hawks",
    "HOU": "Houston Rockets",
    "MIL": "Milwaukee Bucks",
    "PHO": "Phoenix Suns",
    "DEN": "Denver Nuggets",
    "NJN": "New Jersey Nets",
    "NYK": "New York Knicks",
    "DET": "Detroit Pistons",
    "UTA": "Utah Jazz",
    "WSB": "Washington Bullets",
    "CHI": "Chicago Bulls",
    "POR": "Portland Trail Blazers",
    "CLE": "Cleveland Cavaliers",
    "GSW": "Golden State Warriors",
    "ORL": "Orlando Magic",
    "MIA": "Miami Heat",
    "CHH": "Charlotte Hornets",
    "SAC": "Sacramento Kings",
    "MIN": "Minnesota Timberwolves",
    "TOR": "Toronto Raptors",
    "DAL": "Dallas Mavericks",
    "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",
    "WAS": "Washington Wizards",
    "NOH": "New Orleans Hornets",
    "TOT": "Total",
    "OKC": "Oklahoma City Thunder",
    "CHA": "Charlotte Bobcats",
    "NOP": "New Orleans Pelicans",
    "MEM": "Memphis Grizzlies",
    "KCK": "Kansas City Kings",
    "SDC": "San Diego Clippers",
}
map_teams_short = {v: k for k, v in map_teams.items()}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
    "Referer": "https://www.google.com",
    "Accept-Language": "es-ES,es;q=0.9",
}

proxies = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}


def preprocess_league(
    df: pd.DataFrame, mapper=map_teams_short
) -> pd.DataFrame:
    df = df.copy()
    conf = "East" if df.columns[0].lower().startswith("east") else "West"
    df = df[df["W"].str.isnumeric()]
    old_cols = df.columns.tolist()
    old_cols[0] = "Team"
    df.columns = old_cols
    df["Team"] = df["Team"].apply(lambda x: x[:-1] if x.endswith("*") else x)
    df["Team"] = df["Team"].map(mapper)
    df["Conference"] = conf
    df["Rank"] = df.index
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
        assert response.status_code == 200, f"Error - { response.status_code = }"
        df = pd.read_html(response.text, header=1)[0].head(10)
        df["Year"] = year
        df.to_csv(filepath, index=False)
        sleep(random.uniform(3, 7))


def get_teams(path: Path, years: tuple[int, int]):
    if not path.exists():
        path.mkdir(parents=True)

    for year in range(*years):
        filepath = path / f"season_{year}.csv"
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}.html"
        response = requests.get(url, headers=headers, proxies=proxies)
        assert response.status_code == 200, f"Error - { response.status_code = }"
        dfs = pd.read_html(StringIO(response.text))[:2]
        df = pd.concat(
            [
                preprocess_league(dfs[0]),
                preprocess_league(dfs[1]),
            ]
        )
        df["Year"] = year
        df.to_csv(filepath, index=False)
        print(f"- Archivo para el año {year}: {filepath}")
        print(f"\tConferencias: {df["Conference"].unique()}\n")
        sleep(random.uniform(3, 7))

commands = ["all", "teams", "players"]

def main():
    args = sys.argv
    assert len(args) == 2 and args[1] in commands, f"Expected one argument of: {", ".join(commands)}"
    data_path = Path("data")
    mvp_path = data_path / "mvp"
    teams_path = data_path / "season"
    years = (1980, 2026)
    match args[1]:
        case "all":
            get_mvp_data(mvp_path, years)
        case "players":
            get_mvp_data(mvp_path, years)
        case "teams":
            get_teams(teams_path,years)


if __name__ == "__main__":
    main()
