import feedparser
import requests
import os
import time

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

keywords = ["flydubai", "emirates airline", "difc", "dfm", "economy", "market", "stock", "IPO", "investment", "real estate", "finance"]
url = "https://news.google.com/rss/search?q=Dubai&hl=en&gl=US&ceid=US:en"

sent_file = "sent.txt"
if os.path.exists(sent_file):
    with open(sent_file, "r") as f:
        sent_links = set(line.strip() for line in f.readlines())
else:
    sent_links = set()

first_run = True

def send(msg):
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": msg}
    )

while True:
    feed = feedparser.parse(url)

    for entry in feed.entries:
        title = entry.title
        link = entry.link

        if any(k.lower() in title.lower() for k in keywords):
            if link not in sent_links:
                if not first_run:
                    send(f"🚨 {title}\n{link}")
                sent_links.add(link)
                with open(sent_file, "a") as f:
                    f.write(link + "\n")

    first_run = False
    time.sleep(600)
