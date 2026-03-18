import warnings
import requests
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
import re

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

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
    "symbol"              : "string",
    "type"                : "string",
    "total_or_share"      : "string",
    "amount"              : "object",
    "shares"              : "object",
    "commission"          : "object",
    "fx"                  : "object",
    "is_price_fx"         : "boolean",
    "is_commission_fx"    : "boolean",
    "currency"            : "string",
    "in_day_id"           : "string"
}


def sanitize_wealthsimple(file_path):
    MAPPING = {
        "transaction": "type"
    }

    # rename cols
    df = pd.read_csv(file_path)
    df = df.rename(columns=MAPPING)

    PATTERN = re.compile(
        r'^(?P<symbol>[A-Z0-9.\-]+)\s+-\s+.*?:\s+'
        r'(?:Bought|Sold)\s+(?P<shares>\d+(?:\.\d+)?)\s+shares\s+'
        r'\(executed at\s+(?P<executed_at>\d{4}-\d{2}-\d{2})\)',
        re.IGNORECASE
    )

    extracted = df["description"].str.extract(PATTERN)

    df["symbol"] = extracted["symbol"]
    df["shares"] = extracted["shares"]
    df["execute_time"] = extracted["executed_at"]

    # remove and add columns
    col_to_keep = ["date", "type", "amount", "currency", "symbol", "execute_time", "shares"]
    col_to_add  = {"commission": 0,
                   "total_or_share": "Total",
                   "fx": pd.NA,
                   "is_price_fx": pd.NA,
                   "is_commission_fx": False,
                   "account": "wealthsimple",
                   "in_day_id": pd.NA
                   }
    df = df[col_to_keep]
    df = df.assign(**col_to_add)

    # set datatype
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df["execute_time"] = pd.to_datetime(df["execute_time"], format="%Y-%m-%d", errors="coerce")
    df = df.astype(DTYPE_MAPPING)

    # convert datatype
    df["amount"] = df["amount"].map(lambda x: abs(Decimal(str(x)))).astype("object")
    df["amount"] = abs(df["amount"])
    df["shares"] = df["shares"].map(lambda x: Decimal(str(x))).astype("object")
    df["shares"] = abs(df["shares"])
    df["commission"] = df["commission"].map(lambda x: Decimal(str(x))).astype("object")
    df["commission"] = abs(df["commission"])

    # filter out base on transaction type
    types = ["BUYTOOPEN", "SELLTOCLOSE", "CONT", "FEE", "REFER", "TRFIN", "TRFOUT"]
    df = df[~df["type"].isin(types)]

    # 处理Detailed的CSV
    # read from the more detailed df, sanitize the format ⚠️警告：hard coded path
    df_detailed = pd.read_csv("Wealthsimple/result/wealthsimple_detailed.csv", dtype=str)
    df_detailed["filled_time"] = pd.to_datetime(df_detailed["filled_time"], format="ISO8601", utc=True)
    df_detailed["tmp_filled_date"] = df_detailed["filled_time"].dt.date
    # Do the same thing for df
    df["tmp_filled_date"] = df["execute_time"].dt.date

    df_detailed["type"] = df_detailed["type"].str.upper()

    # 把这个也转换成decimal
    df_detailed["total_amount"] = df_detailed["total_amount"].apply( lambda x: Decimal(x) )

    # 然后固定成同样的小数点，放到一个tmp col里面
    def dec_key(d: Decimal) -> str:
        return format(d.quantize(Decimal("0.00001")), "f")
    df["tmp_amount_key"] = df["amount"].apply(dec_key)
    df_detailed["tmp_amount_key"] = df_detailed["total_amount"].apply(dec_key)


    # Pre-Merge processing：修改一下symbol
    df["symbol"] = df["symbol"].str.replace('SONDQ', 'SOND', regex=False)
    df_detailed["symbol"] = df_detailed["symbol"].str.replace('SONDQ', 'SOND', regex=False)


    #sort一下，这里又要引入一个新的tmp id来merge，有点像in_day_id
    df_detailed = df_detailed.sort_values(
        by=["filled_time"],
        ascending=[True],
        kind="mergesort"
    )
    df_detailed["tmp_merge_id"] = df_detailed.groupby(["symbol", "type", "tmp_amount_key"]).cumcount()
    df["tmp_merge_id"] = df.groupby(["symbol", "type", "tmp_amount_key"]).cumcount()


    merged = df.merge(
        df_detailed[
            ["symbol", "type", "tmp_amount_key", "filled_time", "tmp_merge_id"]
        ],
        left_on=["symbol", "type", "tmp_amount_key", "tmp_merge_id"],
        right_on=["symbol", "type", "tmp_amount_key", "tmp_merge_id"],
        how="left"
    )

    merged = merged.drop(columns=["tmp_amount_key", "tmp_filled_date", "tmp_merge_id", "execute_time"])
    merged = merged.rename(columns={"filled_time": "execute_time"})

    merged = merged.sort_values(
        by=["date", "execute_time"],
        ascending=[True, True],
        kind="mergesort"
    )

    # adding in day incremental id, start from 0
    merged["in_day_id"] = merged.groupby("date").cumcount()

    return merged


def sanitize_questrade(file_path):
    MAPPING = {
        "Transaction Date": "date",
        "Action": "type",
        "Symbol": "symbol",
        "Quantity": "shares",
        "Gross Amount": "amount",
        "Commission": "commission",
        "Currency": "currency",
    }
    df = pd.read_csv(file_path)
    df = df.rename(columns=MAPPING)
    col_to_keep = ["date", "type", "amount", "currency", "symbol", "shares", "commission"]
    col_to_add  = {"total_or_share": "Total",
                   "execute_time": pd.NaT,  # Questrade time is not meaningful (fake 9:30 AM)
                   "fx": pd.NA,
                   "is_price_fx": pd.NA,
                   "is_commission_fx": False,
                   "account": "questrade",
                   "in_day_id": pd.NA
                   }
    df = df[col_to_keep]
    df = df.assign(**col_to_add)

    # set datatype — Transaction Date is "2024-01-15 9:30:00 AM", normalize to date only
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %I:%M:%S %p", errors="coerce").dt.normalize()
    df = df.astype(DTYPE_MAPPING)

    # convert datatype
    df["amount"] = df["amount"].map(lambda x: Decimal(str(x))).astype("object")
    df["amount"] = abs(df["amount"])
    df["shares"] = df["shares"].map(lambda x: Decimal(str(x))).astype("object")
    df["shares"] = abs(df["shares"])
    df["commission"] = df["commission"].map(lambda x: Decimal(str(x))).astype("object")
    df["commission"] = abs(df["commission"])

    # filter out base on transaction type
    types = ["DEP", "EFT", "FCH", "CON"]
    df = df[~df["type"].isin(types)]

    df = df.sort_index(ascending=False)

    # adding in day incremental id, start from 0
    # Note: Questrade exports don't include real execution times (the time component is always
    # a fake 9:30 AM), so execute_time is NaT and intra-day ordering is date-level only.
    # If Questrade ever provides real execution times, map that column to execute_time and
    # change this groupby to ["date", "execute_time"] to properly handle after-hours trades.
    df["in_day_id"] = df.groupby("date").cumcount()

    # replaceing some symbols
    mapping = {
        "U079524": "HISU",
        "G036247": "DLR.TO",
    }

    df["symbol"] = df["symbol"].map(mapping).fillna(df["symbol"])

    return df


def sanitize(file, type):
    sanitizers = {
        "wealthsimple": sanitize_wealthsimple,
        "questrade": sanitize_questrade
    }
    return sanitizers.get(type)(file)


def main():
    source_files = {
        "data_container/questrade.csv": "questrade",
        "Wealthsimple/result/merged_wealthsimple.csv": "wealthsimple"
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

    # FX
    start_date = df["date"].min().strftime("%Y-%m-%d")
    end_date   = df["date"].max().strftime("%Y-%m-%d")
    # Get exchange rate from Canadian Central Bank
    usd_cad = get_exchange_rate(start_date, end_date, "USDCAD")

    # Fill weekend/holiday gaps with the preceding business day's rate
    cursor = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_d  = datetime.strptime(end_date,   "%Y-%m-%d").date()
    last_known = None
    while cursor <= end_d:
        key = cursor.strftime("%Y-%m-%d")
        if key in usd_cad:
            last_known = usd_cad[key]
        elif last_known is not None:
            usd_cad[key] = last_known
        cursor += timedelta(days=1)

    # 更新一下这个flag
    df["is_price_fx"]      = df["currency"] != "CAD"
    df["is_commission_fx"] = df["currency"] != "CAD"
    # 这里是因为date这个column实际上是date。。所以需要改成str再去map
    df["fx"] = df["date"].dt.strftime("%Y-%m-%d").map(usd_cad)

    from pathlib import Path
    Path("result").mkdir(exist_ok=True)
    df.to_csv("data_container/combined_trades.csv", index=False)

    # 这里输出的东西就是需要修改的内容的提示
    filtered_df = df[
        df.groupby(["date", "symbol"])["account"]
        .transform("nunique") > 1
        ]
    filtered_df.to_csv("data_container/output_problematic_entries.csv", index=False)


main()