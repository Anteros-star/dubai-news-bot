import feedparser
import requests
import os
import time

# بيانات التليغرام
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# كلمات اقتصادية فقط
keywords = ["economy", "market", "stock", "IPO", "investment", "real estate", "finance"]

# رابط RSS
url = "https://news.google.com/rss/search?q=Dubai&hl=en&gl=US&ceid=US:en"

# تحميل الأخبار المرسلة مسبقاً (أو إنشاء مجموعة جديدة إذا الملف غير موجود)
if os.path.exists("sent.txt"):
    with open("sent.txt", "r") as f:
        sent_links = set(line.strip() for line in f.readlines())
else:
    sent_links = set()

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
                send(f"🚨 {title}\n{link}")
                sent_links.add(link)
                # تخزين الرابط في الملف لمنع التكرار بعد إعادة التشغيل
                with open("sent.txt", "a") as f:
                    f.write(link + "\n")

    time.sleep(600)  # تحقق كل 10 دقائق