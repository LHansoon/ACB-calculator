import pandas
import requests
from decimal import Decimal
import pandas as pd

def get_exchange_rate(start_date, end_date, target_currency_code="USDCAD"):
    code = f"FX{target_currency_code}"
    url = f"https://www.bankofcanada.ca/valet/observations/{code}/json?start_date={start_date}&end_date={end_date}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        observations = data.get("observations", [])

        if observations:
            result = {}
            for each_record in observations:
                result[each_record.get("d")] = each_record.get(code).get("v")
                result[each_record.get("d")] = Decimal(result[each_record.get("d")]) if result[each_record.get("d")] != "" else 0
            return result
        else:
            print("No exchange rate data available for the given date range.")
    else:
        print(f"Error fetching data: {response.status_code}")


DTYPE_MAPPING = {
    "security"            : "string",
    "type"                : "string",
    "total_or_share"      : "string",
    "amount"              : "Float64",
    "shares"              : "Float64",
    "commission"          : "Float64",
    "fx"                  : "Float64",
    "is_price_fx"         : "boolean",
    "is_commission_fx"    : "boolean",
    "currency"            : "string",
    "in_day_id"           : "string"
}


def sanitize_wealthsimple(file_path):
    MAPPING = {
        "transaction": "type",
        "symbol": "security",
        "share": "shares"
    }

    # rename cols
    df = pd.read_csv(file_path)
    df = df.rename(columns=MAPPING)

    # remove and add columns
    col_to_keep = ["date", "type", "amount", "currency", "security", "shares"]
    col_to_add  = {"commission": 0,
                   "total_or_share": "Total",
                   "fx": pandas.NA,
                   "is_price_fx": pandas.NA,
                   "is_commission_fx": True,
                   "account": "wealthsimple",
                   "in_day_id": pandas.NA
                   }
    df = df[col_to_keep]
    df = df.assign(**col_to_add)

    # set datatype
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.astype(DTYPE_MAPPING)

    # filter out base on transaction type
    types = ["BUYTOOPEN", "SELLTOCLOSE", "CONT", "FEE", "REFER", "TRFIN", "TRFOUT"]
    df = df[~df["type"].isin(types)]

    # adding in day incremental id, start from 0
    df["in_day_id"] = df.groupby("date").cumcount()

    return df


def sanitize_questrade(file_path):
    MAPPING = {
        "Settlement Date": "date",
        "Action": "type",
        "Symbol": "security",
        "Quantity": "shares",
        "Gross Amount": "amount",
        "Commission": "commission",
        "Currency": "currency"
    }
    df = pd.read_csv(file_path)
    df = df.rename(columns=MAPPING)
    col_to_keep = ["date", "type", "amount", "currency", "security", "shares", "commission"]
    col_to_add  = {"total_or_share": "Total",
                   "fx": pandas.NA,
                   "is_price_fx": pandas.NA,
                   "is_commission_fx": pandas.NA,
                   "account": "questrade",
                   "in_day_id": pandas.NA
                   }
    df = df[col_to_keep]
    df = df.assign(**col_to_add)

    # set datatype
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.astype(DTYPE_MAPPING)

    # filter out base on transaction type
    types = ["DEP", "EFT", "FCH", "CON"]
    df = df[~df["type"].isin(types)]

    # adding in day incremental id, start from 0
    # Questrade have the oldest data at the bottom, so we need to reverse it for a sec
    df["in_day_id"] = df.iloc[::-1].groupby("date").cumcount()

    return df


def sanitize(file, type):
    sanitizers = {
        "wealthsimple": sanitize_wealthsimple,
        "questrade": sanitize_questrade
    }
    return sanitizers.get(type)(file)


def main():
    start_date = "2023-01-01"
    end_date = "2025-02-27"

    # Get exchange rate from Canadian Central Bank
    usd_cad = get_exchange_rate(start_date, end_date, "USDCAD")
    cad_usd = get_exchange_rate(start_date, end_date, "CADUSD")

    source_files = {
        "Questrade.csv": "questrade",
        "WS_temp.csv"  : "wealthsimple"
    }

    file_dfs = []
    for file in source_files.keys():
        file_dfs.append(sanitize(file, source_files.get(file)))

    df = pd.concat(file_dfs, ignore_index=True)
    df = df.sort_values(
        by=["date", "in_day_id"],
        ascending=[True, True],
        kind="mergesort"
    )

    df.to_csv("output.csv", index=False)

    # 他妈的这些东西都是同一天在不同券商交易的东西，我真是草了
    filtered_df = df[
        df.groupby(["date", "security"])["account"]
        .transform("nunique") > 1
        ]
    filtered_df.to_csv("problematic_entries.csv", index=False)


main()