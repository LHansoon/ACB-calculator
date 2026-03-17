import pandas as pd
from pathlib import Path

def merge_csvs(input_dir: str, output_file: str):
    csv_dir = Path(input_dir)

    # 找到所有 csv 文件
    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError("No CSV files found")

    # 读取并合并
    df_list = []
    for file in csv_files:
        df = pd.read_csv(file)
        df = df.replace(",", pd.NA).dropna(how="all")
        df_list.append(df)

    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df = merged_df[merged_df["transaction"].isin(["BUY", "SELL"])]

    # 导出
    merged_df.to_csv(output_file, index=False)
    print(f"Merged {len(csv_files)} files -> {output_file}")

if __name__ == "__main__":
    Path("result").mkdir(exist_ok=True)
    merge_csvs(
        input_dir="data_container/ws_statements",
        output_file="result/merged_wealthsimple.csv"
    )
