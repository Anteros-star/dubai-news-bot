from time import mktime
from datetime import datetime, timedelta
import feedparser
import requests
import os
import time
import json
import logging
from openai import OpenAI
import redis

# ============================
# 🔧 Settings
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

TOKEN          = os.getenv("TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
REDIS_URL      = os.getenv("REDIS_URL")

if not all([TOKEN, CHAT_ID, OPENAI_API_KEY, REDIS_URL]):
    raise EnvironmentError("❌ Missing environment variables")

client = OpenAI(api_key=OPENAI_API_KEY)
r      = redis.from_url(REDIS_URL, decode_responses=True)

REDIS_KEY      = "dubai_news_bot:sent_links"
CHECK_INTERVAL = 600

# ============================
# 📰 News Feeds
# ============================
def get_feeds():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return [
        f"https://news.google.com/rss/search?q=UAE+business+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=UAE+companies+stocks+banks+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=Dubai+AbuDhabi+business+market+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=DFM+ADX+UAE+stocks+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=الإمارات+أعمال+شركات+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        f"https://news.google.com/rss/search?q=دبي+أبوظبي+بنوك+أسهم+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        f"https://news.google.com/rss/search?q=سوق+دبي+أبوظبي+المالي+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
    ]

# ============================
# 💾 Redis - Check & Save Links
# ============================
def is_sent(link: str) -> bool:
    return r.sismember(REDIS_KEY, link)

def mark_sent(link: str):
    r.sadd(REDIS_KEY, link)
    r.expire(REDIS_KEY, 60 * 60 * 24 * 7)  # Keep for 7 days

# ============================
# 📲 Send Telegram Message
# ============================
def send(msg: str, retries: int = 3):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    for attempt in range(retries):
        try:
            res = requests.post(
                url,
                data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10
            )
            if res.status_code == 200:
                log.info("✅ Message sent")
                return True
            else:
                log.warning(f"⚠️ Failed ({res.status_code}): {res.text}")
        except requests.RequestException as e:
            log.error(f"Telegram error (attempt {attempt+1}): {e}")
        time.sleep(2)
    return False

# ============================
# 🤖 Analyze with OpenAI
# ============================
def analyze_news(title: str) -> dict | None:
    prompt = f"""You are a UAE business news editor.

Your job:
1. Check if the news is related to UAE (companies, banks, markets, real estate, aviation, trade, investment...)
2. If related to UAE → send it regardless of importance
3. If NOT related to UAE at all → don't send it

Reply ONLY with valid JSON, no extra text:

{{
  "send": true,
  "category": "Financial Markets",
  "summary": "One sentence summary in Arabic",
  "emoji": "📈"
}}

Category options: أسواق مالية، بنوك، عقارات، شركات، طيران، تجارة، استثمار، اقتصاد كلي، أخرى
Emoji options: 📈 markets, 🏦 banks, 🏗️ real estate, ✈️ aviation, 🏢 companies, 💰 investment, 📊 economy
"send" is false ONLY if the news has absolutely no relation to UAE

News: {title}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=200,
        )
        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.error(f"JSON error: {e}")
    except Exception as e:
        log.error(f"OpenAI error: {e}")
    return None

# ============================
# 📝 Format Message
# ============================
def format_message(title: str, link: str, analysis: dict) -> str:
    emoji    = analysis.get("emoji", "📊")
    category = analysis.get("category", "أخبار")
    now      = datetime.now().strftime("%H:%M · %d/%m/%Y")
    return (
        f"{emoji} <b>{category}</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📋 {analysis.get('summary', '')}\n\n"
        f"🔗 <a href='{link}'>Read more</a>\n"
        f"🕐 {now}"
    )

# ============================
# 🔄 Main Loop
# ============================
def main():
    log.info("🚀 Bot started with Redis")
    send("🤖 <b>UAE Business News Bot</b> started ✅\n⚡ Powered by GPT-4o-mini + Redis")

    while True:
        log.info("🔍 Checking news...")
        new_count  = 0
        seen_links = set()
        RSS_FEEDS  = get_feeds()

        for feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
            except Exception as e:
                log.error(f"RSS error: {e}")
                continue

            for entry in feed.entries[:30]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")

                if not title or not link:
                    continue

                # Skip duplicates
                if link in seen_links or is_sent(link):
                    continue

                seen_links.add(link)

                # Skip news older than 24 hours
                published = entry.get("published_parsed")
                if published:
                    age_hours = (time.time() - mktime(published)) / 3600
                    if age_hours > 24:
                        mark_sent(link)
                        continue

                analysis = analyze_news(title)
                mark_sent(link)

                if not analysis:
                    continue

                if analysis.get("send"):
                    msg = format_message(title, link, analysis)
                    if send(msg):
                        new_count += 1

                time.sleep(1)

        log.info(f"✅ Done — {new_count} news sent")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
