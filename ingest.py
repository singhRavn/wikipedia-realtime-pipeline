import json
import requests
import duckdb
from sseclient import SSEClient
from datetime import datetime

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
BATCH_SIZE = 200

HEADERS = {
    "User-Agent": "wiki-pipeline/1.0 "
}

con = duckdb.connect("wiki.db")
batch = []

def remove_batch():
    global batch
    if batch:
        con.executemany(
            "INSERT INTO wiki_events VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch
        )
        batch = []

resp = requests.get(URL, headers=HEADERS, stream=True)

client = SSEClient(resp)

for event in client.events():
    try:
        data = json.loads(event.data)
        if data.get("bot"):
            continue
        if data.get("type") != "edit":
            continue
        length = data.get("length")
        if not length:
            continue
        change_size = length.get("new", 0) - length.get("old", 0)
        if change_size == 0:
            continue
        batch.append((
            datetime.fromtimestamp(data["timestamp"]),
            data.get("wiki"),
            data.get("title"),
            data.get("user"),
            data.get("bot", False),
            change_size,
            data.get("comment")
        ))

        if len(batch) >= BATCH_SIZE:
            remove_batch()
    except Exception as e:
        print("Error processing event:", e)
