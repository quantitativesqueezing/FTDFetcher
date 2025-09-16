import pandas as pd
import zipfile, io, requests
import datetime as dt
import requests
import sys
import argparse

def build_url(year, month, half):
    if half == "a":  # first half
        start = f"{year}{month:02d}a"
    else:  # second half
        start = f"{year}{month:02d}b"
    return f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{start}.zip"

def get_latest_url():
    today = dt.date.today()
    y, m, d = today.year, today.month, today.day

    if d <= 15:
        # Use previous month’s second half
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1
        return build_url(y, m, "a")
    else:
        # Current month’s first half
        return build_url(y, m, "b")

def fetch_top_ftds(num_results=200, export=True):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"
        ),
        "Referer": "https://www.sec.gov/",
    }

    today = dt.date.today()
    month = today.strftime("%Y%m")
    last_month_date = today.replace(day=1) - dt.timedelta(days=1)
    last_month = last_month_date.strftime("%Y%m")

    candidates = [
        f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{month}a.zip",
        f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{month}b.zip",
        f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{last_month}b.zip",
        f"https://www.sec.gov/files/data/fails-deliver-data/cnsfails{last_month}a.zip",
    ]

    resp = None
    for potential in candidates:
        print(f"Trying ➤ {potential}")
        try:
            resp = requests.get(potential, headers=headers, timeout=30)
            resp.raise_for_status()
            print(f"✅ Downloading from: {potential}")
            url = potential
            break
        except requests.HTTPError as err:
            print(f"  ❌ Request failed (status {err.response.status_code}), trying next…")
            resp = None
    else:
        raise RuntimeError(f"No FTD files accessible within: {candidates}")

    # Save the ZIP file locally
    zip_filename = "latest_ftd.zip"
    with open(zip_filename, "wb") as fzip:
        fzip.write(resp.content)

    z = zipfile.ZipFile(io.BytesIO(resp.content))
    fname = z.namelist()[0]
    # Read the file, decode as latin1 to handle possible encoding
    with z.open(fname) as f:
        raw_data = f.read().decode("latin1")
    from io import StringIO
    df = pd.read_csv(StringIO(raw_data), sep="|", header=0)
    expected_cols = ["SettlementDate","CUSIP","Symbol","QuantityFails","Company","Price"]
    if len(df.columns) >= len(expected_cols):
        df.columns = expected_cols + list(df.columns[len(expected_cols):])
    else:
        df.columns = expected_cols[:len(df.columns)]

    df.dropna(subset=["QuantityFails","Price"], inplace=True)
    df["QuantityFails"] = pd.to_numeric(df["QuantityFails"], errors="coerce")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["FTD_Value"] = df["QuantityFails"] * df["Price"]

    latest_date = df["SettlementDate"].max()
    df_recent = df[df["SettlementDate"] == latest_date]

    WHITELIST = {"SPY", "QQQ", "USO", "LQD"}
    fundish_substrings = [
        # Common ETF/ETN/fund families
        "etf", "etn", "spdr", "ishares", "vanguard", "invesco", "proshares",
        "global x", "direxion", "wisdomtree", "xtrackers", "vaneck", "pacer",
        "ark", "first trust", "schwab", "select sector", "index",
        # Generic fund terms
        "fund", "trust unit", "unit investment trust", "closed end", "open end",
        # Wealth/private equity terms
        "private equity", "wealth fund", "family office", "sovereign wealth",
        # Bond/fixed income keywords (to exclude bond funds/ETFs/notes)
        "bond", "treasury", "muni", "municipal", "note", "preferred", "fixed income",
        # Other structures often not single operating companies
        "depositary receipt", "adr", "ads", "unit trust", "capital trust", "income trust",
        "reit", "real estate", "partnership", " lp ", " llp ", " mlp ", " etp "
    ]

    def is_single_stock(symbol: str, company: str) -> bool:
        # Normalize symbol safely (guard against NaN/float/None)
        if symbol is None or (isinstance(symbol, float) and pd.isna(symbol)):
            sym = ""
        else:
            sym = str(symbol)
        sym = sym.upper().strip()
        if sym in WHITELIST:
            return True

        # Normalize company safely
        if company is None or (isinstance(company, float) and pd.isna(company)):
            name = ""
        else:
            name = str(company)
        name = name.lower()
        name_spaced = f" {name} "
        return not any(term in name_spaced for term in fundish_substrings)

    df_recent = df_recent.copy()
    df_recent["Symbol"] = df_recent["Symbol"].astype(str)
    df_recent["Company"] = df_recent["Company"].astype(str)
    df_recent = df_recent[df_recent.apply(lambda r: is_single_stock(r.get("Symbol"), r.get("Company")), axis=1)]
    top_results = (df_recent.sort_values("FTD_Value", ascending=False)
                    .head(num_results)
                    .reset_index(drop=True))

    # Format QuantityFails with thousands separators for display/export
    try:
        # Keep a numeric backup if needed later
        top_results["QuantityFails_numeric"] = top_results["QuantityFails"].astype("Int64")
        top_results["QuantityFails"] = top_results["QuantityFails_numeric"].map(lambda x: f"{x:,}" if pd.notna(x) else "")
    except Exception:
        # Fallback formatting in case of unexpected types
        top_results["QuantityFails"] = top_results["QuantityFails"].apply(lambda x: f"{int(float(x)):,}" if pd.notna(x) else "")

    top_results['FTD_Value'] = top_results['FTD_Value'].map(lambda x: f"${x:,.2f}")

    # Reorder columns for display and export
    top_results = top_results[['SettlementDate','Symbol','Company','CUSIP','Price','QuantityFails','FTD_Value']]

    print(f"\n🎯 Top {num_results} NYSE/NASDAQ by FTD Value on {latest_date}:\n")
    print(top_results[['SettlementDate','Symbol','Company','CUSIP','Price','QuantityFails','FTD_Value']].to_string(index=False))

    # Export to Excel format (.xlsx)
    excel_name = f"FTD_Top{num_results}_{latest_date}.xlsx"
    top_results.to_excel(excel_name, index=False, engine="openpyxl")
    print(f"\n✅ Exported Excel (XLSX) file: {excel_name}")

    if export:
        fname_base = f"FTD_Top{num_results}_{latest_date}"
        top_results.to_csv(fname_base + ".csv", index=False)
        top_results.to_excel(fname_base + ".xlsx", index=False, engine="openpyxl")
        print(f"\n✅ Exported: {fname_base}.csv and {fname_base}.xlsx")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and export top FTD data")
    parser.add_argument("num_results", type=int, help="Number of top results to fetch (must be > 0)")
    parser.add_argument("--no-export", action="store_true", help="Skip file export")

    args = parser.parse_args()

    if args.num_results <= 0:
        print("Error: Number of results must be greater than 0")
        sys.exit(1)

    fetch_top_ftds(num_results=args.num_results, export=not args.no_export)