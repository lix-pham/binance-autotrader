import argparse
from slack_sdk.web import WebClient


class Slack:
    def __init__(self, token):
        self.client = WebClient(token=token)

    def print(self, text: str):
        response = self.client.conversations_list()
        bot_channels = response.data["channels"]
        for channel in bot_channels:
            if not channel["is_member"]:
                continue
            self.client.chat_postMessage(
                channel=channel["id"], text=text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("token")
    args = parser.parse_args()

    client = WebClient(token=args.token)
    resp = client.conversations_list()
    channels = resp.data["channels"]
    channels = [channel for channel in channels
                if channel["is_member"]]
    for channel in channels:
        client.chat_postMessage(
            channel=channel["id"],
            text="This is a test",
        )


if __name__ == "__main__":
    main()
