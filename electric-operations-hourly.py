import requests
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

def extract(api_key, start_year=2020):
    base_url = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data"
    params = {
        "frequency": "hourly",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
        "api_key": api_key
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        json_response = response.json()
        print("API response:", json_response)
        data = json_response.get("response", {}).get("data", [])
        print(f"Fetched {len(data)} rows")
        return data
    except requests.RequestException as e:
        print(f"API error: {e}")
        print(f"Response text: {response.text}")
        return []

def transform(raw_data):
    if not raw_data:
        print("No data to transform")
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)

    # Cleaning
    df["period"] = pd.to_datetime(df["period"], format="%Y-%m-%dT%H", errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["period", "value"])

    # Rename columns
    df = df.rename(columns={
        "period": "datetime",
        "subba-name": "subba_name",
        "parent-name": "parent_name",
        "value": "value_mw",
        "value-units": "value_units"
    })

    print(f"Transformed {len(df)} rows")
    return df[["datetime", "subba_name", "parent_name", "value_mw", "value_units"]]

def load(df, db_path="eia_rto_data.db"):
    if df.empty:
        print("No data to load")
        return

    conn = sqlite3.connect(db_path)
    df.to_sql("region_sub_ba_data", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data loaded to {db_path}")

def visualize(df):
    if df.empty:
        print("No data to visualize")
        return

    plt.figure(figsize=(10, 6))
    df.groupby("datetime")["value_mw"].mean().plot()
    plt.title("Average Power Value Over Time")
    plt.xlabel("Datetime")
    plt.ylabel("Power (MW)")
    plt.grid(True)
    plt.savefig("power_trend.png")
    plt.show()
    print("Saved plot to power_trend.png")

def run_etl(api_key):
    raw_data = extract(api_key)
    cleaned_df = transform(raw_data)
    load(cleaned_df)
    visualize(cleaned_df)
    return cleaned_df

if __name__ == "__main__":
    API_KEY = "C818Lw6Kjhe39QmHPrgyRQU0Uc8lGTgvUlBRO0Oq"
    if not API_KEY:
        print("Error: Please set a valid EIA API key")
    else:
        run_etl(API_KEY)