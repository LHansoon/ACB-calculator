from pathlib import Path
import json
import traceback
from decimal import ROUND_HALF_UP
from decimal import Decimal
import pandas as pd

TRADE_BRIEF = 0
TRADE_DETAIL = 1


def get_har_file(path):
    base_dir = Path(path)

    har_files = list(base_dir.rglob("*.har"))

    for har_file in har_files:
        with open(har_file, "r", encoding="utf-8") as f:
            har_dict = json.load(f)

    return har_dict


def process_har(har_dict):
    trade_entries = {}
    log = har_dict.get("log")
    entries = log.get("entries")
    for entry in entries:
        # Data
        request = None
        response = None
        response_data = None
        request_id = None

        # structure loading and validation
        entry_type = None
        error = None
        try:
            request = entry.get("request")
            response = entry.get("response")

            response_text = response.get("content").get("text")
            response_text = json.loads(response_text)
            response_data = response_text.get("data")

            request_post_data = request.get("postData")
            request_text = request_post_data.get("text")
            request_text = json.loads(request_text)

        except Exception as e:
            error = "⚠️：response结构错误\n" +  traceback.print_exc()
            print(error)


        if("activityFeedItems" in response_data.keys()):
            entry_type = TRADE_BRIEF
        elif("soOrdersExtendedOrder" in response_data.keys()):
            entry_type = TRADE_DETAIL

        # 处理打开activity就加载的那个东西
        if(entry_type == TRADE_BRIEF):
            edges = response_data.get("activityFeedItems").get("edges")
            for edge in edges:
                # FILLED。说实话这里不做这个filter应该也无所谓，反正之后还要再处理
                node = edge.get("node")
                if node.get("status") == "FILLED":
                    data_dict = {
                        "total_amount": node.get("amount"),
                        "symbol": node.get("assetSymbol"),
                        "type": "BUY" if "BUY" in node.get("type") else "SELL" # 这个值可能是DIY_SELL这种
                    }
                    id = node.get("externalCanonicalId")

                    if id in trade_entries:
                        trade_entries[id].update(data_dict)
                    else:
                        trade_entries[id] = data_dict
        # 处理点开每个entry以后load的东西
        elif(entry_type == TRADE_DETAIL):
            request_id = request_text.get("variables").get("externalId")

            order_information = response_data.get("soOrdersExtendedOrder")
            lastFilledAtUtc = order_information.get("lastFilledAtUtc")

            # 没有完成的交易，比如取消了，这种就不存在lastFilledAtUtc，就不需要加进去
            if(lastFilledAtUtc is not None):
                # ⚠️警告：这破玩意儿不能按照submittedNetValue来，他居然是averageFilledPrice*filledQuantity然后round，😅而且最后结算的时候真的就是这样的
                total_amount = Decimal(order_information.get("averageFilledPrice")) * Decimal(order_information.get("filledQuantity"))
                total_amount = total_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

                data_dict = {
                    # "total_amount": str(total_amount),
                    "currency": order_information.get("securityCurrency"),
                    "shares": order_information.get("filledQuantity"),
                    "filled_time": lastFilledAtUtc
                    #"filled_time": datetime.fromisoformat(lastFilledAtUtc.replace("Z", "+00:00")).astimezone(ZoneInfo("America/Halifax"))
                }

                if request_id in trade_entries:
                    trade_entries[request_id].update(data_dict)
                else:
                    trade_entries[request_id] = data_dict

    return trade_entries






DATA_PATH = "data_container/"
RESULT_PATH = "result/"





def main():
    har_dict = get_har_file(DATA_PATH)
    if har_dict is None:
        print(fr"⚠️ No har file found in {DATA_PATH}")
    else:
        processed_dict = process_har(har_dict)

    for key, value in processed_dict.items():
        if len(value.keys()) != 6:
            print(f"{key}: missing value, its filled time is: {value.get('filled_time')}")

    Path(RESULT_PATH).mkdir(exist_ok=True)
    df = pd.DataFrame.from_dict(processed_dict, orient="index").reset_index(drop=True)
    df.to_csv(RESULT_PATH + "wealthsimple_detailed.csv", index=False)

    with open(RESULT_PATH + "output.json", "w", encoding="utf-8") as f:
        json.dump(processed_dict, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()