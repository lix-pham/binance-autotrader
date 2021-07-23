import datetime
import logging
import time
from typing import Dict


log = logging.getLogger(__name__)


class Bot:
    def digest_event(self, event: Dict) -> None:
        pass

    def update(self) -> Dict:
        pass


class VolumeBot(Bot):
    def __init__(
            self,
            ticker: str,
            volume: float,
            interval: float=10 * 60,
            cooldown: float=60 * 60,
            cd_exception: float=20,
    ):
        self.ticker = ticker.upper()
        self.volume = volume
        self.interval = interval
        self.cooldown = cooldown
        self.cd_exception = cd_exception

        self.buckets = dict()
        self.bucket_width = 30

        self.prev_volume = 0
        self.prev_ts = 0

    def digest_event(self, event: Dict) -> None:
        skip = (event["type"] != "aggTrade" or
                event["symbol"] != self.ticker)
        if skip:
            return

        index = self._bucket_index(event["time"])
        current_volume = self.buckets.get(index, 0)
        self.buckets[index] = current_volume + event["quantity"]

    def _bucket_index(self, timestamp: float) -> int:
        return int(timestamp / self.bucket_width)

    def update(self) -> Dict:
        now = time.time()

        timestamp_threshold = self._bucket_index(now - self.interval)
        self.buckets = {key: value for key, value in self.buckets.items()
                        if key > timestamp_threshold}

        volume = sum(self.buckets.values())

        event = {
            "type": "volume",
            "value": volume,
            "text":
                f"{datetime.datetime.fromtimestamp(now)} -> "
                f"{self.ticker}: {volume:.1f} traded in the last "
                f"{self.interval}s",
            "timestamp": now,
        }
        log.debug("Created volume event: %s", event)

        trigger = volume >= self.volume
        cool = now > self.cooldown + self.prev_ts
        significant = (
            volume > self.prev_volume * (1 + self.cd_exception / 100))

        if trigger and (cool or significant):
            self.prev_ts = now
            self.prev_volume = volume
            return event

        return None


def main():
    # force a cooldown after first list
    timestamps = list(range(30)) + list(range(60,80))
    events = [{
        "type": "aggTrade",
        "symbol": "ADAUSDT",
        "quantity": 2,
        "time": timestamp,
    } for timestamp in timestamps]

    bot = VolumeBot(
        ticker="ADAUSDT",
        volume=11,
        cooldown=20,
    )
    bot.bucket_width = 0.5

    for idx, event in enumerate(events):
        with mock.patch("time.time", return_value=event["time"]):
            bot.digest_event(event)
            # Call update on every second event
            if (idx % 2) and (alert := bot.update()):
                print(event["time"], alert)


if __name__ == "__main__":
    from unittest import mock
    main()
