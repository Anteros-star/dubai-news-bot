import feedparser
import requests
import os
import time

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

keywords = ["IPO", "AI", "startup", "investment"]

url = "https://news.google.com/rss/search?q=Dubai&hl=en&gl=US&ceid=US:en"

def send(msg):
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": msg}
    )

while True:
    feed = feedparser.parse(url)

    for entry in feed.entries:
        title = entry.title
        
        for k in keywords:
            if k.lower() in title.lower():
                send(f"🚨 {title}\n{entry.link}")
                break

    time.sleep(600)