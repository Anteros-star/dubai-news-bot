from time import mktime
from datetime import datetime, timedelta
import feedparser
import requests
import os
import time
import json
import logging
import re
import threading
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

REDIS_KEY      = "dubai_news_bot:sent_titles"
REDIS_STATS    = "dubai_news_bot:stats"
REDIS_LATEST   = "dubai_news_bot:latest"
REDIS_PAUSED   = "dubai_news_bot:paused"
CHECK_INTERVAL = 600

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
# 📰 News Feeds — Google News (original)
# ============================
def get_google_feeds():
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
# 🌍 News Feeds — Global Agencies (NEW)
# ============================
def get_global_feeds():
    return [
        # Reuters
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.reuters.com/news/wealth",
        # AP News
        "https://feeds.apnews.com/rss/business",
        "https://feeds.apnews.com/rss/finance",
        # Bloomberg (via Google News search)
        "https://news.google.com/rss/search?q=site:bloomberg.com+UAE+OR+Dubai+OR+Abu+Dhabi&hl=en&gl=AE",
        "https://news.google.com/rss/search?q=site:bloomberg.com+Middle+East+economy&hl=en&gl=AE",
        # BBC Business
        "http://feeds.bbci.co.uk/news/business/rss.xml",
        "https://news.google.com/rss/search?q=site:bbc.com+UAE+OR+Dubai+economy&hl=en",
        # CNBC
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://news.google.com/rss/search?q=site:cnbc.com+UAE+OR+Middle+East+business&hl=en",
        # Financial Times (via Google News)
        "https://news.google.com/rss/search?q=site:ft.com+UAE+OR+Gulf+economy&hl=en",
        # Al Jazeera Economy
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://news.google.com/rss/search?q=site:aljazeera.com+UAE+economy&hl=ar",
        # Sky News Arabia
        "https://news.google.com/rss/search?q=site:skynewsarabia.com+الإمارات+اقتصاد&hl=ar",
        # Gulf News
        "https://gulfnews.com/rss/business",
        "https://gulfnews.com/rss/uae",
        # Khaleej Times
        "https://www.khaleejtimes.com/rss/business",
        # Arabian Business
        "https://www.arabianbusiness.com/rss/articles",
        # Zawya
        "https://news.google.com/rss/search?q=site:zawya.com+UAE+economy&hl=en",
        # The National UAE
        "https://www.thenationalnews.com/arc/outboundfeeds/rss/",
        # WAM (وكالة أنباء الإمارات)
        "https://wam.ae/ar/feed",
    ]

# ============================
# 💾 Redis Helpers
# ============================
def is_sent(key: str) -> bool:
    return r.sismember(REDIS_KEY, key)

def mark_sent(key: str):
    r.sadd(REDIS_KEY, key)
    r.expire(REDIS_KEY, 60 * 60 * 24 * 7)

def is_paused() -> bool:
    return r.exists(REDIS_PAUSED) == 1

def pause_bot():
    r.set(REDIS_PAUSED, "1")

def resume_bot():
    r.delete(REDIS_PAUSED)

def increment_stat(key: str):
    r.hincrby(REDIS_STATS, key, 1)

def get_stats() -> dict:
    return r.hgetall(REDIS_STATS) or {}

def save_latest(title: str, link: str, category: str):
    item = json.dumps({"title": title, "link": link, "category": category, "time": datetime.now().strftime("%H:%M · %d/%m/%Y")})
    r.lpush(REDIS_LATEST, item)
    r.ltrim(REDIS_LATEST, 0, 9)  # keep last 10

def get_latest(n: int = 5) -> list:
    items = r.lrange(REDIS_LATEST, 0, n - 1)
    return [json.loads(i) for i in items]

# ============================
# 📲 Send Telegram
# ============================
def send(msg: str, retries: int = 3, chat_id: str = None) -> bool:
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    target = chat_id or CHAT_ID
    for attempt in range(retries):
        try:
            res = requests.post(
                url,
                data={"chat_id": target, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": "true"},
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

def get_updates(offset: int = None) -> list:
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    params = {"timeout": 10}
    if offset:
        params["offset"] = offset
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            return res.json().get("result", [])
    except Exception as e:
        log.error(f"getUpdates error: {e}")
    return []

# ============================
# 🤖 Analyze with OpenAI — Enhanced Classification
# ============================
def analyze_news(title: str) -> dict | None:
    prompt = f"""You are a UAE business news editor and classifier.

Your job:
1. Determine if the news is related to UAE (companies, banks, markets, real estate, aviation, trade, investment, economy, crypto, tech, energy...)
2. If related → classify it with a MAIN category AND a SUB category
3. Assign an importance score 1-10 (10 = breaking/major news)
4. Detect if it's BREAKING news (score >= 8 or urgent language)
5. If NOT related to UAE at all → send: false

MAIN categories & SUB categories:
- أسواق مالية → [سوق دبي DFM, سوق أبوظبي ADX, سوق السلع, صناديق الاستثمار, العملات, مؤشرات عالمية]
- بنوك ومالية → [بنوك إماراتية, بنوك خليجية, تمويل ورهن, تأمين, فنتك]
- عقارات → [دبي, أبوظبي, الشارقة, مشاريع جديدة, بيع وشراء, أسعار]
- شركات → [طاقة, تقنية, اتصالات, تجزئة, صناعة, ناشئة]
- طيران وسياحة → [طيران الإمارات, فلاي دبي, مطارات, فنادق, سياحة]
- تجارة واقتصاد → [صادرات, واردات, موانئ, مناطق حرة, اتفاقيات تجارية]
- استثمار → [استثمار أجنبي, صناديق سيادية, M&A, IPO]
- طاقة ونفط → [نفط وغاز, طاقة متجددة, OPEC, أرامكو, ADNOC]
- تقنية وابتكار → [ذكاء اصطناعي, بلوك تشين, كريبتو, شركات ناشئة]
- اقتصاد كلي → [GDP, تضخم, بطالة, سياسات حكومية, ميزانية]
- أخرى → [متنوع]

Reply ONLY with valid JSON, no extra text:

{{
  "send": true,
  "main_category": "أسواق مالية",
  "sub_category": "سوق دبي DFM",
  "summary": "ملخص الخبر بجملة واحدة بالعربية",
  "emoji": "📈",
  "importance": 7,
  "breaking": false
}}

"send" is false ONLY if news has absolutely no relation to UAE.
"breaking" is true if importance >= 8 or news contains urgent/breaking language.

Emoji guide: 📈 markets, 🏦 banks, 🏗️ real estate, ✈️ aviation, 🏢 companies, 💰 investment, ⚡ energy, 💻 tech, 📊 economy, 🚢 trade

News: {title}"""

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
# 📝 Format Message — Enhanced
# ============================
def format_message(title: str, link: str, analysis: dict, source: str = "") -> str:
    emoji        = analysis.get("emoji", "📊")
    main_cat     = analysis.get("main_category", "أخبار")
    sub_cat      = analysis.get("sub_category", "")
    importance   = analysis.get("importance", 5)
    breaking     = analysis.get("breaking", False)
    now          = datetime.now().strftime("%H:%M · %d/%m/%Y")

    # Importance stars
    if importance >= 8:
        stars = "🔴 عاجل"
    elif importance >= 6:
        stars = "🟡 مهم"
    else:
        stars = "🟢 عادي"

    # Breaking header
    breaking_header = "🚨 <b>خبر عاجل</b>\n" if breaking else ""

    # Category line
    category_line = f"{main_cat}"
    if sub_cat:
        category_line += f" › {sub_cat}"

    # Source tag
    source_tag = f"\n📡 <i>{source}</i>" if source else ""

    return (
        f"{breaking_header}"
        f"{emoji} <b>{category_line}</b>  {stars}\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📋 {analysis.get('summary', '')}\n\n"
        f"🔗 <a href='{link}'>اقرأ المزيد</a>"
        f"{source_tag}\n"
        f"🕐 {now}"
    )

# ============================
# 💬 Telegram Commands Handler
# ============================
def handle_commands():
    log.info("💬 Command listener started")
    offset = None

    while True:
        updates = get_updates(offset)
        for update in updates:
            offset = update["update_id"] + 1
            msg = update.get("message", {})
            text = msg.get("text", "").strip()
            from_id = str(msg.get("chat", {}).get("id", ""))

            if not text.startswith("/"):
                continue

            cmd = text.split()[0].lower()
            log.info(f"📩 Command: {cmd} from {from_id}")

            if cmd == "/start":
                send(
                    "🤖 <b>UAE Business News Bot</b>\n\n"
                    "أهلاً! أنا بوت الأخبار الاقتصادية الإماراتية.\n\n"
                    "الأوامر المتاحة:\n"
                    "/help - قائمة الأوامر\n"
                    "/stats - إحصائيات البوت\n"
                    "/latest - آخر 5 أخبار\n"
                    "/categories - الأخبار حسب التصنيف\n"
                    "/pause - إيقاف الإرسال مؤقتاً\n"
                    "/resume - استئناف الإرسال\n"
                    "/status - حالة البوت",
                    chat_id=from_id
                )

            elif cmd == "/help":
                send(
                    "📖 <b>قائمة الأوامر</b>\n\n"
                    "/start — رسالة ترحيب\n"
                    "/stats — عدد الأخبار المرسلة والتصنيفات\n"
                    "/latest — آخر 5 أخبار تم إرسالها\n"
                    "/categories — الأخبار حسب التصنيف الرئيسي\n"
                    "/pause — إيقاف الإرسال مؤقتاً\n"
                    "/resume — استئناف الإرسال\n"
                    "/status — هل البوت يعمل؟",
                    chat_id=from_id
                )

            elif cmd == "/stats":
                stats = get_stats()
                total = stats.get("total_sent", "0")
                breaking_count = stats.get("breaking", "0")
                cycles = stats.get("cycles", "0")
                lines = [f"📊 <b>إحصائيات البوت</b>\n"]
                lines.append(f"📨 إجمالي الأخبار المرسلة: <b>{total}</b>")
                lines.append(f"🚨 أخبار عاجلة: <b>{breaking_count}</b>")
                lines.append(f"🔄 دورات الفحص: <b>{cycles}</b>")
                lines.append(f"\n🗂️ <b>حسب التصنيف:</b>")
                for k, v in stats.items():
                    if k not in ("total_sent", "breaking", "cycles"):
                        lines.append(f"  • {k}: {v}")
                send("\n".join(lines), chat_id=from_id)

            elif cmd == "/latest":
                items = get_latest(5)
                if not items:
                    send("📭 لا توجد أخبار محفوظة بعد.", chat_id=from_id)
                else:
                    msg_text = "📰 <b>آخر 5 أخبار</b>\n\n"
                    for i, item in enumerate(items, 1):
                        msg_text += f"{i}. <a href='{item['link']}'>{item['title'][:60]}...</a>\n"
                        msg_text += f"   🗂️ {item['category']} · 🕐 {item['time']}\n\n"
                    send(msg_text, chat_id=from_id)

            elif cmd == "/categories":
                stats = get_stats()
                lines = ["🗂️ <b>الأخبار حسب التصنيف</b>\n"]
                cat_keys = [k for k in stats if k not in ("total_sent", "breaking", "cycles")]
                if cat_keys:
                    for k in sorted(cat_keys, key=lambda x: int(stats[x]), reverse=True):
                        lines.append(f"• {k}: <b>{stats[k]}</b> خبر")
                else:
                    lines.append("لا توجد بيانات بعد.")
                send("\n".join(lines), chat_id=from_id)

            elif cmd == "/pause":
                pause_bot()
                send("⏸️ تم إيقاف الإرسال مؤقتاً.\nاستخدم /resume لاستئنافه.", chat_id=from_id)
                log.info("⏸️ Bot paused by command")

            elif cmd == "/resume":
                resume_bot()
                send("▶️ تم استئناف الإرسال! ✅", chat_id=from_id)
                log.info("▶️ Bot resumed by command")

            elif cmd == "/status":
                paused = is_paused()
                stats = get_stats()
                total = stats.get("total_sent", "0")
                status_icon = "⏸️ متوقف مؤقتاً" if paused else "✅ يعمل"
                send(
                    f"🤖 <b>حالة البوت</b>\n\n"
                    f"الحالة: {status_icon}\n"
                    f"📨 أخبار مرسلة: {total}\n"
                    f"🕐 الوقت: {datetime.now().strftime('%H:%M · %d/%m/%Y')}",
                    chat_id=from_id
                )

        time.sleep(2)

# ============================
# 🔄 Process a Single Feed
# ============================
def process_feed(feed_url: str, seen_titles: set, source_name: str = "") -> int:
    count = 0
    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        log.error(f"RSS error [{source_name}]: {e}")
        return 0

    for entry in feed.entries[:20]:
        title = entry.get("title", "").strip()
        link  = entry.get("link", "")

        if not title or not link:
            continue

        norm = normalize(title)
        if norm in seen_titles or is_sent(norm):
            log.info(f"⏭️ Duplicate: {title[:50]}")
            continue

        seen_titles.add(norm)

        published = entry.get("published_parsed")
        if published:
            age_hours = (time.time() - mktime(published)) / 3600
            if age_hours > 24:
                mark_sent(norm)
                continue

        analysis = analyze_news(title)
        mark_sent(norm)

        if not analysis or not analysis.get("send"):
            continue

        msg = format_message(title, link, analysis, source_name)
        if send(msg):
            count += 1
            # Save stats
            increment_stat("total_sent")
            increment_stat(analysis.get("main_category", "أخرى"))
            if analysis.get("breaking"):
                increment_stat("breaking")
            # Save to latest
            cat_label = analysis.get("main_category", "أخبار")
            sub = analysis.get("sub_category", "")
            if sub:
                cat_label += f" › {sub}"
            save_latest(title, link, cat_label)

        time.sleep(1)

    return count

# ============================
# 🔄 Main Loop
# ============================
def main():
    log.info("🚀 Bot started with Redis + Global Sources + Commands")
    send(
        "🤖 <b>UAE Business News Bot</b> started ✅\n"
        "⚡ GPT-4o-mini + Redis\n"
        "🌍 مصادر عالمية: Reuters, AP, BBC, Bloomberg, CNBC, FT, Al Jazeera\n"
        "📰 مصادر محلية: Gulf News, Khaleej Times, Arabian Business, The National, WAM\n"
        "💬 أوامر تفاعلية متاحة — اكتب /help"
    )

    # Start command listener in background thread
    cmd_thread = threading.Thread(target=handle_commands, daemon=True)
    cmd_thread.start()

    while True:
        if is_paused():
            log.info("⏸️ Bot is paused, skipping cycle...")
            time.sleep(60)
            continue

        log.info("🔍 Checking news...")
        increment_stat("cycles")
        new_count   = 0
        seen_titles = set()

        # Google News feeds
        for feed_url in get_google_feeds():
            new_count += process_feed(feed_url, seen_titles, "Google News")

        # Global agency feeds
        global_sources = {
            "Reuters":          ["https://feeds.reuters.com/reuters/businessNews", "https://feeds.reuters.com/news/wealth"],
            "AP News":          ["https://feeds.apnews.com/rss/business"],
            "BBC Business":     ["http://feeds.bbci.co.uk/news/business/rss.xml"],
            "CNBC":             ["https://www.cnbc.com/id/10001147/device/rss/rss.html"],
            "Al Jazeera":       ["https://www.aljazeera.com/xml/rss/all.xml"],
            "Gulf News":        ["https://gulfnews.com/rss/business", "https://gulfnews.com/rss/uae"],
            "Khaleej Times":    ["https://www.khaleejtimes.com/rss/business"],
            "Arabian Business": ["https://www.arabianbusiness.com/rss/articles"],
            "The National":     ["https://www.thenationalnews.com/arc/outboundfeeds/rss/"],
            "Bloomberg (GN)":   ["https://news.google.com/rss/search?q=site:bloomberg.com+UAE+OR+Dubai+OR+Abu+Dhabi&hl=en&gl=AE"],
            "FT (GN)":          ["https://news.google.com/rss/search?q=site:ft.com+UAE+OR+Gulf+economy&hl=en"],
            "WAM":              ["https://wam.ae/ar/feed"],
        }

        for source_name, urls in global_sources.items():
            for url in urls:
                new_count += process_feed(url, seen_titles, source_name)

        log.info(f"✅ Done — {new_count} news sent")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
