import argparse
from slack_sdk.web import WebClient


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
