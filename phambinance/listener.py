import enum
import json
import time
import threading
import queue

import websocket


class MessageType(enum.Enum):
    ON_OPEN, ON_MESSAGE = range(2)


class Listener:

    def __init__(self, message_queue):
        self.message_id = 0
        self.message_queue = message_queue
        self.socket = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws/listener",
            on_open=self.on_open,
            on_message=self.on_message,
        )

    def on_open(self, ws):
        self.message_queue.put(
            (MessageType.ON_OPEN, None))

    def on_message(self, ws, message):
        message = json.loads(message)
        self.message_queue.put(
            (MessageType.ON_MESSAGE, message))

    def run(self):
        self.socket.run_forever()

    def close(self):
        self.socket.close()

    def subscribe_aggregate_trade(self, ticker):
        message_id = self.message_id
        self.message_id += 1

        self.socket.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{ticker}@aggTrade"],
            "id": message_id,
        }))


if __name__ == "__main__":
    # websocket.enableTrace(True)

    message_queue = queue.Queue()
    listener = Listener(message_queue=message_queue)
    thread = threading.Thread(target=listener.run, daemon=True)

    thread.start()

    try:
        while True:
            message_type, message = message_queue.get()
            if message_type == MessageType.ON_OPEN:
                listener.subscribe_aggregate_trade("adausdt")
            print(message)
            message_queue.task_done()
    except KeyboardInterrupt:
        pass

    listener.close()
    thread.join()
