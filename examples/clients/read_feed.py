"""Reads a datafeed feed: two pages, then a stream that survives dropped connections."""
import json
import os
import sys
import urllib.request

SERVER = os.environ.get("DATAFEED_SERVER", "http://127.0.0.1:8080")
TOKEN = os.environ.get("DATAFEED_TOKEN")
feed = sys.argv[1]


def get(path):
    headers = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}
    request = urllib.request.Request(f"{SERVER}/api/v1/feeds/{feed}/{path}", headers=headers)
    return urllib.request.urlopen(request, timeout=30)


# Pages: carry on from X-Next-Offset, which is absent at the end.
offset = 0
for _ in range(2):
    with get(f"records?offset={offset}&limit=3") as response:
        for line in response:
            print("page:", json.loads(line)["id"])
        offset = response.headers.get("X-Next-Offset")
    if offset is None:
        break

# Stream: the position is the start plus the records received, so a dropped connection
# picks up exactly where it stopped. Stops here after 5 records.
position = 0
while position < 5:
    try:
        with get(f"stream?from={position}&rate=10") as response:
            for line in response:
                print("stream:", position, json.loads(line)["id"])
                position += 1
                if position == 5:
                    break
    except OSError as error:
        print("reconnecting after:", error)
