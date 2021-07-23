import threading
import argparse
import queue
import logging
import time
from typing import Iterable, Dict, List, Callable

from phambinance.bots import Bot, VolumeBot
from phambinance.listener import Listener
from phambinance.preprocess import preprocess
from phambinance.slack import Slack


log = logging.getLogger(__name__)


def feed_bots(bots: Iterable[Bot], market_data: queue.Queue) -> None:
    try:
        while True:
            message = preprocess(market_data.get_nowait())
            log.debug(message)
            for bot in bots:
                bot.digest_event(message)
    except queue.Empty:
        pass


def update_bots(
        bots: Iterable[Bot],
        last_update: float,
) -> List[Dict]:
    now = time.time()
    responses = []
    if now - last_update >= 1:
        last_update = now
        responses = list(
            filter(None, (bot.update() for bot in bots)))
        log.debug("processed responses: %s", responses)
    return responses, last_update


def process_responses(
        responses: Iterable[Dict],
        processors: Iterable[Callable[[Dict], None]]
) -> None:
    for processor in processors:
        for response in responses:
            processor(response)


def loop(
        bots: Iterable[Bot],
        market_data: queue.Queue,
        processors: Iterable[Callable[[Dict], None]],
) -> None:
    last_update = time.time()
    while True:
        feed_bots(bots, market_data)
        responses, last_update = update_bots(bots, last_update)
        process_responses(responses, processors)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("ticker")
    parser.add_argument("volume", type=float)
    parser.add_argument("--verbose", "-v", action="store_true")

    parser.add_argument("--token", default=None)

    return parser.parse_args()


def main():
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    bots = [VolumeBot(args.ticker, volume=args.volume)]

    market_data = queue.Queue()
    listener = Listener(message_queue=market_data)
    thread = threading.Thread(target=listener.run, daemon=True)
    thread.start()
    response = listener.subscribe_aggregate_trade(args.ticker)
    log.info(response)

    processors = [lambda e: log.info(e["text"])]
    if args.token:
        slack_processor = Slack(args.token)
        processors.append(lambda e: slack_processor.print(e["text"]))

    try:
        loop(bots, market_data, processors)
    except KeyboardInterrupt:
        pass

    listener.close()
    thread.join()


if __name__ == "__main__":
    main()
