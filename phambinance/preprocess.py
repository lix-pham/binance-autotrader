def preprocess(event):
    preprocessor = {
        "aggTrade": preprocess_aggTrade,
    }.get(event["e"], lambda e: e)

    return preprocessor(event)


def preprocess_aggTrade(event):
    return {
        "type": event["e"],
        "symbol": event["s"],
        "price": float(event["p"]),
        "quantity": float(event["q"]),
        # binance time is in milliseconds
        "time": event["T"] / 1000,
        # ----------------------
        # "eventTime": event["E"],
        # "aggTradeId": event["a"],
        # "firstTradeId": event["f"],
        # "lastTradeId": event["l"],
        # "isBuyerMM": event["m"],
    }
