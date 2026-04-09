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

CHECK_INTERVAL = 1800
TOP_COUNT = 5

# ============================
# FILTER
# ============================
LOW_QUALITY = ["podcast","video","opinion","newsletter"]

UAE_HINTS = ["uae","dubai","abu dhabi","emirates","adnoc","emaar","etihad"]

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
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.reuters.com/news/wealth",
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        "https://www.marketwatch.com/rss/topstories",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://gulfnews.com/rss/business",
        "https://www.khaleejtimes.com/rss/business",
        "https://www.thenationalnews.com/arc/outboundfeeds/rss/",
        f"https://news.google.com/rss/search?q=UAE+Dubai+AbuDhabi+economy+after:{y}&hl=en&gl=AE&ceid=AE:en"
    ]

# ============================
# TELEGRAM
# ============================
def send(msg):
    requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", data={
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true"
    })

# ============================
# 🧠 AI (STRICT UAE)
# ============================
def analyze(title):
    prompt = f"""
You are a UAE business news editor.

STRICT RULES:

Only accept news if:
1. Directly about UAE (companies, economy, real estate, aviation, banks)
OR
2. Has CLEAR and STRONG impact on UAE economy

Reject:
- General world news
- US/Europe news without UAE link
- Vague Middle East news

Return JSON:
{{
"send": true,
"importance": 8,
"category": "أسواق",
"summary": "ملخص عربي قصير"
}}

If not relevant → {{ "send": false }}

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
# COLLECT
# ============================
def collect_news():
    seen = set()
    news = []
    source_count = {}

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

            if any(x in title.lower() for x in LOW_QUALITY):
                continue

            # فلتر خفيف (يساعد بدون ما يخنق الأخبار)
            if not any(k in title.lower() for k in UAE_HINTS):
                continue

            if source_count.get(url, 0) >= 5:
                continue

            seen.add(norm)
            source_count[url] = source_count.get(url, 0) + 1

            analysis = analyze(title)

            if not analysis or not analysis.get("send"):
                continue

            importance = analysis.get("importance",0)

            if importance < 6:
                continue

            item = {
                "title": title,
                "link": link,
                "importance": importance,
                "summary": analysis.get("summary",""),
                "category": analysis.get("category","أخبار")
            }

            news.append(item)

            # 🚨 BREAKING
            if importance >= 8:
                if not r.sismember("breaking_sent", norm):
                    msg = (
                        "🚨 <b>خبر عاجل - الإمارات</b>\n\n"
                        f"📌 <b>{title}</b>\n"
                        f"📊 {item['summary']}\n"
                        f"🔗 {link}"
                    )
                    send(msg)
                    r.sadd("breaking_sent", norm)

            time.sleep(0.3)

    return news

# ============================
# TOP NEWS
# ============================
def send_top(news):
    if not news:
        return

    news = sorted(news, key=lambda x: x["importance"], reverse=True)[:TOP_COUNT]

    msg = "🇦🇪 <b>أهم أخبار الإمارات الاقتصادية</b>\n\n"

    for i, n in enumerate(news, 1):
        msg += (
            f"{i}. <b>{n['title']}</b>\n"
            f"🗂️ {n['category']}\n"
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
        log.info("collecting UAE news...")
        news = collect_news()
        log.info(f"{len(news)} UAE news")

        send_top(news)

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
