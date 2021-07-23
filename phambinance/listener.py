import json
import threading
import queue
import logging
from typing import Any

import websocket

from .id_manager import IdManager


log = logging.getLogger(__name__)


class Listener:

    def __init__(self, message_queue: queue.Queue):
        self.id_manager = IdManager()
        self.is_open = threading.Event()

        self.message_queue = message_queue
        self.socket = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws/listener",
            on_open=self.on_open,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message,
        )

    def on_open(
            self,
            ws: websocket.WebSocketApp
    ) -> None:
        """Callback for opened connection."""
        log.debug("Websocket now opens")
        self.is_open.set()

    def on_close(
            self,
            ws: websocket.WebSocketApp,
            code: int,
            msg: str,
    ) -> None:
        """Callback on connection close."""
        log.debug("Websocket now closed: (%s) %s", code, msg)
        self.is_open.clear()

    def on_error(
            self,
            ws: websocket.WebSocketApp,
            exception: Exception,
    ) -> None:
        """Callback for errors."""
        log.error("Encountered websocket error: %s", exception)

    def on_message(
            self,
            ws: websocket.WebSocketApp,
            message: str,
    ) -> None:
        """Callback for received messages from Binance."""
        log.debug("Received message on websocket")
        message = json.loads(message)

        if isinstance(message, dict) and "id" in message:
            log.debug("Received reply for request %s", message["id"])
            self.id_manager.set_response(message["id"], message)
        else:
            self.message_queue.put(message)

    def run(self) -> None:
        """Start connection to Binance and listen.

        This call blocks until the connection is closed.
        """
        self.socket.run_forever()

    def close(self) -> None:
        """Close Listener connection to Binance."""
        self.socket.close()

    def subscribe_aggregate_trade(
            self,
            *tickers: str,
    ) -> Any:
        """Subscribe Listener to aggregate trades data.

        This call will block until a response from the exchange
        has been received.

        Attributes:
            tickers (str): One or more tickers to subscribe to.

        Returns:
            Decoded response from exchange.
        """
        self.is_open.wait()
        message_id = self.id_manager.issue_id()
        self.socket.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{ticker}@aggTrade" for ticker in tickers],
            "id": message_id,
        }))
        return self.id_manager.wait(message_id)


if __name__ == "__main__":
    # websocket.enableTrace(True)
    logging.basicConfig(level=logging.DEBUG)

    message_queue = queue.Queue()
    listener = Listener(message_queue=message_queue)
    thread = threading.Thread(target=listener.run, daemon=True)

    thread.start()
    result = listener.subscribe_aggregate_trade("adausdt")
    log.debug("Subscribtion result: %s", result)
    try:
        while True:
            message = message_queue.get()
            print(message)
            message_queue.task_done()
    except KeyboardInterrupt:
        pass

    listener.close()
    thread.join()
