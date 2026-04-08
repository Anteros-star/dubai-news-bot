from time import mktime
from datetime import datetime, timedelta
import feedparser
import requests
import os
import time
import json
import logging
import re
from openai import OpenAI
import redis

# ============================
# CONFIG
# ============================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
REDIS_URL = os.getenv("REDIS_URL")

client = OpenAI(api_key=OPENAI_API_KEY)
r = redis.from_url(REDIS_URL, decode_responses=True)

CHECK_INTERVAL = 1800  # كل 30 دقيقة
TOP_COUNT = 3

# ============================
# FILTERS
# ============================
UAE_KEYWORDS = ["uae","dubai","abu dhabi","emirates","adnoc","dfm","adx","emaar"]
LOW_QUALITY = ["live","update","podcast","video","opinion"]

# ============================
# NORMALIZE
# ============================
def normalize(t):
    t = t.lower()
    t = re.sub(r'[^\w\s]', '', t)
    return ' '.join(sorted(t.split()[:10]))

# ============================
# SOURCES
# ============================
def get_sources():
    y = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return [
        f"https://news.google.com/rss/search?q=UAE+business+after:{y}&hl=en&gl=AE&ceid=AE:en"
    ]

# ============================
# TELEGRAM
# ============================
def send(msg):
    requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", data={
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    })

# ============================
# AI
# ============================
def analyze(title):
    prompt = f"""
Rate importance of UAE business news (1-10) and summarize in Arabic.
Return JSON:
{{"importance": 1-10, "summary":"..."}}

News: {title}
"""
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0.2
        )
        return json.loads(res.choices[0].message.content)
    except:
        return None

# ============================
# MAIN LOGIC
# ============================
def collect_news():
    seen = set()
    news_list = []

    for url in get_sources():
        feed = feedparser.parse(url)

        for e in feed.entries[:20]:
            title = e.get("title","")
            link = e.get("link","")

            if not title or not link:
                continue

            norm = normalize(title)

            if norm in seen:
                continue

            if not any(k in title.lower() for k in UAE_KEYWORDS):
                continue

            if any(x in title.lower() for x in LOW_QUALITY):
                continue

            seen.add(norm)

            analysis = analyze(title)
            if not analysis:
                continue

            importance = analysis.get("importance",0)

            if importance < 5:
                continue

            news_list.append({
                "title": title,
                "link": link,
                "importance": importance,
                "summary": analysis.get("summary","")
            })

            time.sleep(0.3)

    return news_list

# ============================
# SEND TOP NEWS
# ============================
def send_top_news(news):
    if not news:
        return

    news = sorted(news, key=lambda x: x["importance"], reverse=True)[:TOP_COUNT]

    msg = "🔥 <b>أهم الأخبار الاقتصادية في الإمارات</b>\n\n"

    for i, n in enumerate(news, 1):
        msg += (
            f"{i}. <b>{n['title']}</b>\n"
            f"📊 {n['summary']}\n"
            f"🔗 {n['link']}\n\n"
        )

    msg += f"🕐 {datetime.now().strftime('%H:%M')}"

    send(msg)

# ============================
# LOOP
# ============================
def main():
    while True:
        log.info("collecting...")
        news = collect_news()
        log.info(f"{len(news)} valid news")

        send_top_news(news)

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
