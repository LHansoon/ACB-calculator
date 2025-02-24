
import requests


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
            return result
        else:
            print("No exchange rate data available for the given date range.")
    else:
        print(f"Error fetching data: {response.status_code}")


def main():
    start_date = "2024-01-01"
    end_date = "2024-12-31"
    get_exchange_rate(start_date, end_date)

main()