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

REDIS_KEY      = "uae_realestate_bot:sent_titles"
CHECK_INTERVAL = 300  # كل 5 دقائق

# ============================
# 🧹 Normalize Title
# ============================
def normalize(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title)
    words = title.split()
    return ' '.join(sorted(words[:8]))

# ============================
# 📰 Real Estate Feeds
# ============================
def get_feeds():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return [
        # إنجليزي
        f"https://news.google.com/rss/search?q=UAE+real+estate+property+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=Dubai+real+estate+launch+project+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=Abu+Dhabi+real+estate+property+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=Emaar+Damac+Nakheel+Aldar+property+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=UAE+property+prices+developer+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        # عربي
        f"https://news.google.com/rss/search?q=عقارات+الإمارات+دبي+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        f"https://news.google.com/rss/search?q=إعمار+نخيل+دامك+مشروع+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        f"https://news.google.com/rss/search?q=عقارات+أبوظبي+الشارقة+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
    ]

# ============================
# 💾 Redis
# ============================
def is_sent(key: str) -> bool:
    return r.sismember(REDIS_KEY, key)

def mark_sent(key: str):
    r.sadd(REDIS_KEY, key)
    r.expire(REDIS_KEY, 60 * 60 * 24 * 7)  # 7 days

# ============================
# 📲 Send Telegram
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
    prompt = f"""أنت محلل عقاري متخصص في سوق الإمارات.

مهمتك:
1. تحقق إذا كان الخبر متعلق بالعقارات في الإمارات
2. صنّف نوع الخبر بدقة
3. إذا لم يكن له علاقة بعقارات الإمارات → لا ترسله

أجب فقط بـ JSON صالح بدون أي نص إضافي:

{{
  "send": true,
  "category": "إطلاق مشروع",
  "developer": "إعمار",
  "location": "دبي",
  "summary": "ملخص بجملة واحدة بالعربية",
  "emoji": "🏗️"
}}

قيم "category": إطلاق مشروع، تسليم مشروع، صفقة واستحواذ، أسعار وتوجهات، تصريح مطور، تمويل وقروض، أخرى
قيم "emoji": 🏗️ إطلاق، 🔑 تسليم، 💰 صفقة، 📊 أسعار، 🎙️ تصريح، 🏦 تمويل، 🏢 أخرى
"developer": اسم المطور إذا مذكور، وإلا "—"
"location": المدينة أو الإمارة إذا مذكورة، وإلا "الإمارات"
"send" يكون false فقط إذا لا علاقة له بعقارات الإمارات نهائياً

الخبر: {title}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=250,
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
    emoji     = analysis.get("emoji", "🏢")
    category  = analysis.get("category", "عقارات")
    developer = analysis.get("developer", "—")
    location  = analysis.get("location", "الإمارات")
    now       = datetime.now().strftime("%H:%M · %d/%m/%Y")
    return (
        f"{emoji} <b>{category}</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📋 {analysis.get('summary', '')}\n\n"
        f"🏙️ <b>الموقع:</b> {location}\n"
        f"👷 <b>المطور:</b> {developer}\n\n"
        f"🔗 <a href='{link}'>اقرأ الخبر كاملاً</a>\n"
        f"🕐 {now}"
    )

# ============================
# 🔄 Main Loop
# ============================
def main():
    log.info("🚀 Real Estate Bot started")
    send("🏗️ <b>مرصد عقارات الإمارات</b> بدأ العمل ✅\n⚡ تغطية فورية لأخبار العقارات")

    while True:
        log.info("🔍 Checking real estate news...")
        new_count   = 0
        seen_titles = set()
        RSS_FEEDS   = get_feeds()

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

                norm = normalize(title)
                if norm in seen_titles or is_sent(norm):
                    continue

                seen_titles.add(norm)

                # تجاهل الأخبار الأقدم من 24 ساعة
                published = entry.get("published_parsed")
                if published:
                    age_hours = (time.time() - mktime(published)) / 3600
                    if age_hours > 24:
                        mark_sent(norm)
                        continue

                analysis = analyze_news(title)
                mark_sent(norm)

                if not analysis:
                    continue

                if analysis.get("send"):
                    msg = format_message(title, link, analysis)
                    if send(msg):
                        new_count += 1

                time.sleep(1)

        log.info(f"✅ Done — {new_count} real estate news sent")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
