import requests
import pandas as pd
import sqlalchemy as create_engine
import sqlite3
import matplotlib.pyplot as plt

def extract(api_key, start_year=2020):
    base_url = "https://api.eia.gov/v2/electricity/retail-sales/data"
    params = {
        "frequency": "monthly",
        "data[0]": "customers",
        "data[1]": "price",
        "data[2]": "revenue",
        "data[3]": "sales",
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
        print("API response:", json_response)  # Debug: Print full response
        data = json_response.get("response", {}).get("data", [])
        print(f"Fetched {len(data)} rows")
        return data
    except requests.RequestException as e:
        print(f"API error: {e}")
        print(f"Response text: {response.text}")  # Debug: Print raw response
        return []
    


def transform(raw_data):
    if not raw_data:
        print("No data to transform")
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    
    # Basic cleaning
    df["period"] = pd.to_datetime(df["period"], format="%Y-%m", errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["customers"] = pd.to_numeric(df["customers"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df = df.dropna(subset=["period", "price", "customers", "revenue", "sales"])
    
    # Rename columns
    df = df.rename(columns={
        "period": "date",
        "stateid": "state",
        "sectorid": "sector",
        "price": "price_cents_kwh",
        "customers": "customers_count",
        "revenue": "revenue_thousand_dollars",
        "sales": "sales_mwh"
    })
    
    print(f"Transformed {len(df)} rows")
    return df[["date", "state", "sector", "customers_count", "price_cents_kwh",
            "revenue_thousand_dollars", "sales_mwh"]]

# Step 3: Load - Save to SQLite database
def load(df, db_path="eia_data.db"):
    if df.empty:
        print("No data to load")
        return
    
    conn = sqlite3.connect(db_path)
    df.to_sql("electricity_retail_sales", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data loaded to {db_path}")

# Step 4: Visualize - Plot average price over time (portfolio bonus)
def visualize(df):
    if df.empty:
        print("No data to visualize")
        return
    
    plt.figure(figsize=(10, 6))
    df.groupby("date")["price_cents_kwh"].mean().plot()
    plt.title("Average Electricity Price Over Time")
    plt.xlabel("Date")
    plt.ylabel("Price (cents/kWh)")
    plt.grid(True)
    plt.savefig("price_trend.png")  # Save for portfolio
    plt.show()
    print("Saved plot to price_trend.png")

# Main function
def run_etl(api_key):
    raw_data = extract(api_key)
    cleaned_df = transform(raw_data)
    load(cleaned_df)
    visualize(cleaned_df)
    return cleaned_df

# Run the script
if __name__ == "__main__":
    API_KEY = "C818Lw6Kjhe39QmHPrgyRQU0Uc8lGTgvUlBRO0Oq"  # Replace with your EIA API key
    if API_KEY == "C818Lw6Kjhe39QmHPrgyRQU0Uc8lGTgvUlBRO0Oq":
        print("Error: Please set a valid EIA API key")
    else:
        df = run_etl(API_KEY)
        print(df.head())
