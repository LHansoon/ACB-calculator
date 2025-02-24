
import requests
import csv
from datetime import datetime, timedelta
from decimal import Decimal


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

def read_csv_to_dict(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data

def main():
    start_date = "2023-01-01"
    end_date = "2024-12-31"
    usd_cad = get_exchange_rate(start_date, end_date, "USDCAD")
    cad_usd = get_exchange_rate(start_date, end_date, "CADUSD")

    csv_data = read_csv_to_dict("test_file.csv")
    ACB = 0
    total_shares = 0
    total_capital_gain = 0

    csv_data = sorted(csv_data, key=lambda x: x["date"])
    for i in range(len(csv_data)):
        row = csv_data[i]
        date = row.get("date")
        action = row.get("action")
        fmv = Decimal(row.get("FMV", 0)) if row.get("FMV", 0) != "" else 0
        shares = Decimal(row.get("shares", 0)) if row.get("shares", 0) != "" else 0
        fee = Decimal(row.get("fee")) if row.get("fee", 0) != "" else 0

        usd_cad_today = usd_cad.get(date)
        cad_usd_today = cad_usd.get(date)

        # TODO: 这个待定，就是说判断是不是要convert，更进一步的话，从哪个货币convert
        if True:
            fmv *= usd_cad_today
            fee *= usd_cad_today

        if action == "PURCHASE":
            cost = shares * fmv + fee
            ACB += cost
            total_shares += shares

        else:
            # =([Share Price] x [Number of Shares Sold]) – [Transaction Costs] – (([Total ACB] / [Previous Number of Shares]) x [Number of Shares Sold])
            capital_gain = (fmv * shares) - fee - (ACB / total_shares * shares)

            # [Previous Total ACB] – ([ACB per Share] x [Number of Shares Sold])
            ACB = ACB - ACB / total_shares * shares

            total_shares -= shares

            # TODO: 这里应该是or i> 0，因为rule适用于前后三十天。
            if capital_gain < 0 and (i < len(csv_data) - 1 and i > 0):
                previous_date_object = datetime.strptime(csv_data[i - 1].get("date"), '%Y-%m-%d')
                current_date_object = datetime.strptime(date, '%Y-%m-%d')
                next_date_object = datetime.strptime(csv_data[i + 1].get("date"), '%Y-%m-%d')

                # https://www.adjustedcostbase.ca/blog/what-is-the-superficial-loss-rule/
                if current_date_object - timedelta(days = 30) <= previous_date_object or current_date_object + timedelta(days = 30) >= next_date_object:
                    ACB -= capital_gain # 这里capital gain是个负数，所以实际上是加回了ACB
                    capital_gain = 0

            total_capital_gain += capital_gain


    print(total_capital_gain)








main()