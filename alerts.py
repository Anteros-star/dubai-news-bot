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
    raise EnvironmentError("Missing environment variables")

client = OpenAI(api_key=OPENAI_API_KEY)
r      = redis.from_url(REDIS_URL, decode_responses=True)

REDIS_KEY    = "dubai_news_bot:sent_titles"
REDIS_STATS  = "dubai_news_bot:stats"
REDIS_LATEST = "dubai_news_bot:latest"
REDIS_PAUSED = "dubai_news_bot:paused"

CHECK_INTERVAL = 600  # 10 minutes


# ============================
# Normalize Title
# ============================
def normalize(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title)
    words = title.split()
    return ' '.join(sorted(words[:8]))


# ============================
# ALL NEWS SOURCES
# Tier 1 — Wire agencies
# Tier 2 — Global financial press
# Tier 3 — General international press
# Tier 4 — Middle East & Gulf regional
# Tier 5 — Google News targeted
# ============================
def get_all_sources() -> dict:
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    return {

        # ── TIER 1: WIRE AGENCIES ──────────────────────────
        "Reuters — Business": [
            "https://feeds.reuters.com/reuters/businessNews",
            "https://feeds.reuters.com/reuters/companyNews",
            "https://feeds.reuters.com/news/wealth",
        ],
        "Reuters — World": [
            "https://feeds.reuters.com/Reuters/worldNews",
        ],
        "AP News — Business": [
            "https://feeds.apnews.com/rss/business",
            "https://feeds.apnews.com/rss/finance",
            "https://feeds.apnews.com/rss/economy",
        ],
        "AP News — World": [
            "https://feeds.apnews.com/rss/world-news",
        ],
        "AFP": [
            f"https://news.google.com/rss/search?q=source:AFP+UAE+OR+Dubai+economy+after:{yesterday}&hl=en&gl=AE",
        ],
        "UPI": [
            "https://rss.upi.com/news/business.rss",
            "https://rss.upi.com/news/tn_int.rss",
        ],
        "WAM": [
            "https://wam.ae/en/feed",
            "https://wam.ae/ar/feed",
        ],

        # ── TIER 2: GLOBAL FINANCIAL PRESS ────────────────
        "Bloomberg": [
            "https://feeds.bloomberg.com/markets/news.rss",
            "https://feeds.bloomberg.com/business/news.rss",
            f"https://news.google.com/rss/search?q=site:bloomberg.com+UAE+OR+Dubai+OR+Abu+Dhabi+after:{yesterday}&hl=en&gl=AE",
            f"https://news.google.com/rss/search?q=site:bloomberg.com+Middle+East+economy+after:{yesterday}&hl=en",
        ],
        "Financial Times": [
            "https://www.ft.com/?format=rss",
            f"https://news.google.com/rss/search?q=site:ft.com+UAE+OR+Gulf+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "Wall Street Journal": [
            "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
            "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
            f"https://news.google.com/rss/search?q=site:wsj.com+UAE+OR+Gulf+after:{yesterday}&hl=en",
        ],
        "CNBC": [
            "https://www.cnbc.com/id/100003114/device/rss/rss.html",
            "https://www.cnbc.com/id/10001147/device/rss/rss.html",
            "https://www.cnbc.com/id/20910258/device/rss/rss.html",
            f"https://news.google.com/rss/search?q=site:cnbc.com+Middle+East+OR+UAE+after:{yesterday}&hl=en",
        ],
        "Forbes": [
            "https://www.forbes.com/business/feed/",
            "https://www.forbes.com/money/feed/",
            f"https://news.google.com/rss/search?q=site:forbes.com+UAE+OR+Dubai+after:{yesterday}&hl=en",
        ],
        "Fortune": [
            f"https://news.google.com/rss/search?q=site:fortune.com+UAE+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "MarketWatch": [
            "https://www.marketwatch.com/rss/topstories",
            "https://www.marketwatch.com/rss/marketpulse",
        ],
        "The Economist": [
            "https://www.economist.com/finance-and-economics/rss.xml",
            "https://www.economist.com/middle-east-and-africa/rss.xml",
        ],
        "Business Insider": [
            "https://feeds.businessinsider.com/custom/all",
            f"https://news.google.com/rss/search?q=site:businessinsider.com+UAE+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "Yahoo Finance": [
            "https://finance.yahoo.com/news/rssindex",
        ],
        "Seeking Alpha": [
            f"https://news.google.com/rss/search?q=site:seekingalpha.com+UAE+OR+Gulf+after:{yesterday}&hl=en",
        ],
        "Morningstar": [
            f"https://news.google.com/rss/search?q=site:morningstar.com+UAE+OR+Gulf+stocks+after:{yesterday}&hl=en",
        ],
        "Barron's": [
            f"https://news.google.com/rss/search?q=site:barrons.com+UAE+OR+Gulf+after:{yesterday}&hl=en",
        ],

        # ── TIER 3: GENERAL INTERNATIONAL PRESS ───────────
        "BBC — Business": [
            "https://feeds.bbci.co.uk/news/business/rss.xml",
            "https://feeds.bbci.co.uk/news/world/rss.xml",
            f"https://news.google.com/rss/search?q=site:bbc.com+UAE+OR+Dubai+after:{yesterday}&hl=en",
        ],
        "CNN — Business": [
            "http://rss.cnn.com/rss/money_news_international.rss",
            "http://rss.cnn.com/rss/edition_business.rss",
            f"https://news.google.com/rss/search?q=site:cnn.com+UAE+OR+Middle+East+economy+after:{yesterday}&hl=en",
        ],
        "The Guardian": [
            "https://www.theguardian.com/business/rss",
            "https://www.theguardian.com/world/middleeast/rss",
        ],
        "New York Times": [
            "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        ],
        "Washington Post": [
            "https://feeds.washingtonpost.com/rss/business",
            "https://feeds.washingtonpost.com/rss/world",
        ],
        "Sky News — Business": [
            "https://feeds.skynews.com/feeds/rss/business.xml",
            "https://feeds.skynews.com/feeds/rss/world.xml",
        ],
        "Deutsche Welle": [
            "https://rss.dw.com/rdf/rss-en-bus",
            "https://rss.dw.com/rdf/rss-en-world",
            f"https://news.google.com/rss/search?q=site:dw.com+UAE+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "Euronews": [
            f"https://news.google.com/rss/search?q=site:euronews.com+UAE+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "Channel NewsAsia": [
            "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=business",
        ],
        "NBC News": [
            "https://feeds.nbcnews.com/nbcnews/public/business",
        ],
        "NPR Business": [
            "https://feeds.npr.org/1006/rss.xml",
        ],
        "South China Morning Post": [
            f"https://news.google.com/rss/search?q=site:scmp.com+UAE+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "Nikkei Asia": [
            f"https://news.google.com/rss/search?q=site:asia.nikkei.com+UAE+OR+Gulf+after:{yesterday}&hl=en",
        ],

        # ── TIER 4: MIDDLE EAST & GULF REGIONAL ───────────
        "Al Jazeera": [
            "https://www.aljazeera.com/xml/rss/all.xml",
            f"https://news.google.com/rss/search?q=site:aljazeera.com+UAE+economy+after:{yesterday}&hl=ar",
        ],
        "Sky News Arabia": [
            f"https://news.google.com/rss/search?q=site:skynewsarabia.com+الإمارات+اقتصاد+after:{yesterday}&hl=ar",
            f"https://news.google.com/rss/search?q=site:skynewsarabia.com+دبي+after:{yesterday}&hl=ar",
        ],
        "Al Arabiya": [
            f"https://news.google.com/rss/search?q=site:alarabiya.net+الإمارات+اقتصاد+after:{yesterday}&hl=ar",
            f"https://news.google.com/rss/search?q=site:alarabiya.net+دبي+أبوظبي+after:{yesterday}&hl=ar",
        ],
        "Gulf News": [
            "https://gulfnews.com/rss/business",
            "https://gulfnews.com/rss/uae",
            "https://gulfnews.com/rss/markets",
        ],
        "Khaleej Times": [
            "https://www.khaleejtimes.com/rss/business",
            "https://www.khaleejtimes.com/rss/uae",
        ],
        "Arabian Business": [
            "https://www.arabianbusiness.com/rss/articles",
        ],
        "The National UAE": [
            "https://www.thenationalnews.com/arc/outboundfeeds/rss/",
        ],
        "Zawya": [
            f"https://news.google.com/rss/search?q=site:zawya.com+UAE+after:{yesterday}&hl=en",
            f"https://news.google.com/rss/search?q=site:zawya.com+دبي+اقتصاد+after:{yesterday}&hl=ar",
        ],
        "Arab News": [
            f"https://news.google.com/rss/search?q=site:arabnews.com+UAE+economy+after:{yesterday}&hl=en",
        ],
        "MEED": [
            f"https://news.google.com/rss/search?q=site:meed.com+UAE+after:{yesterday}&hl=en",
        ],
        "Asharq Al-Awsat": [
            f"https://news.google.com/rss/search?q=site:aawsat.com+الإمارات+after:{yesterday}&hl=ar",
        ],
        "Al Ittihad": [
            f"https://news.google.com/rss/search?q=site:alittihad.ae+اقتصاد+after:{yesterday}&hl=ar",
        ],
        "Emarat Al Youm": [
            f"https://news.google.com/rss/search?q=site:emaratalyoum.com+اقتصاد+after:{yesterday}&hl=ar",
        ],

        # ── TIER 5: GOOGLE NEWS TARGETED ──────────────────
        "Google News — UAE EN": [
            f"https://news.google.com/rss/search?q=UAE+business+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
            f"https://news.google.com/rss/search?q=UAE+companies+stocks+banks+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
            f"https://news.google.com/rss/search?q=Dubai+AbuDhabi+business+market+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
            f"https://news.google.com/rss/search?q=DFM+ADX+UAE+stocks+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
            f"https://news.google.com/rss/search?q=UAE+real+estate+property+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
            f"https://news.google.com/rss/search?q=UAE+IPO+investment+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        ],
        "Google News — UAE AR": [
            f"https://news.google.com/rss/search?q=الإمارات+أعمال+شركات+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
            f"https://news.google.com/rss/search?q=دبي+أبوظبي+بنوك+أسهم+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
            f"https://news.google.com/rss/search?q=سوق+دبي+أبوظبي+المالي+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
            f"https://news.google.com/rss/search?q=الإمارات+عقارات+استثمار+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
            f"https://news.google.com/rss/search?q=اقتصاد+الإمارات+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        ],
    }


# ============================
# Redis Helpers
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
    item = json.dumps({
        "title": title,
        "link": link,
        "category": category,
        "time": datetime.now().strftime("%H:%M · %d/%m/%Y")
    })
    r.lpush(REDIS_LATEST, item)
    r.ltrim(REDIS_LATEST, 0, 9)

def get_latest(n: int = 5) -> list:
    items = r.lrange(REDIS_LATEST, 0, n - 1)
    return [json.loads(i) for i in items]


# ============================
# Send Telegram
# ============================
def send(msg: str, retries: int = 3, chat_id: str = None) -> bool:
    url    = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    target = chat_id or CHAT_ID
    for attempt in range(retries):
        try:
            res = requests.post(
                url,
                data={
                    "chat_id": target,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true"
                },
                timeout=10
            )
            if res.status_code == 200:
                return True
            log.warning(f"Telegram {res.status_code}: {res.text[:100]}")
        except requests.RequestException as e:
            log.error(f"Telegram error (attempt {attempt+1}): {e}")
        time.sleep(2)
    return False

def get_updates(offset: int = None) -> list:
    url    = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
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
# Analyze with OpenAI
# ============================
def analyze_news(title: str) -> dict | None:
    prompt = f"""You are a UAE business news editor and classifier.

Rules:
1. Check if the news is related to UAE (companies, banks, markets, real estate, aviation, trade, investment, economy, energy, tech, fintech, crypto...)
2. If related → classify with MAIN + SUB category, importance score, breaking flag
3. If NOT related to UAE at all → "send": false

MAIN categories & SUB categories:
- أسواق مالية → [سوق دبي DFM, سوق أبوظبي ADX, سلع, صناديق, عملات, مؤشرات عالمية]
- بنوك ومالية → [بنوك إماراتية, بنوك خليجية, تمويل, تأمين, فنتك]
- عقارات → [دبي, أبوظبي, الشارقة, مشاريع جديدة, أسعار]
- شركات → [طاقة, تقنية, اتصالات, تجزئة, صناعة, شركات ناشئة]
- طيران وسياحة → [طيران الإمارات, فلاي دبي, مطارات, فنادق, سياحة]
- تجارة واقتصاد → [صادرات, واردات, موانئ, مناطق حرة, اتفاقيات]
- استثمار → [استثمار أجنبي, صناديق سيادية, M&A, IPO, طرح عام]
- طاقة ونفط → [نفط وغاز, طاقة متجددة, OPEC, ADNOC]
- تقنية وابتكار → [ذكاء اصطناعي, بلوك تشين, كريبتو, شركات ناشئة]
- اقتصاد كلي → [GDP, تضخم, سياسات حكومية, ميزانية]
- أخرى → [متنوع]

Reply ONLY with valid JSON:
{{
  "send": true,
  "main_category": "أسواق مالية",
  "sub_category": "سوق دبي DFM",
  "summary": "ملخص الخبر بجملة واحدة بالعربية",
  "emoji": "📈",
  "importance": 7,
  "breaking": false
}}

"send" = false ONLY if zero relation to UAE.
"breaking" = true if importance >= 8 OR urgent/breaking language.

Emoji: 📈 markets · 🏦 banks · 🏗️ real estate · ✈️ aviation · 🏢 companies · 💰 investment · ⚡ energy · 💻 tech · 📊 economy · 🚢 trade

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
# Format Message
# ============================
def format_message(title: str, link: str, analysis: dict, source: str = "") -> str:
    emoji      = analysis.get("emoji", "📊")
    main_cat   = analysis.get("main_category", "أخبار")
    sub_cat    = analysis.get("sub_category", "")
    importance = analysis.get("importance", 5)
    breaking   = analysis.get("breaking", False)
    now        = datetime.now().strftime("%H:%M · %d/%m/%Y")

    priority       = "🔴 عاجل" if importance >= 8 else ("🟡 مهم" if importance >= 6 else "🟢 عادي")
    category_line  = f"{main_cat} › {sub_cat}" if sub_cat else main_cat
    breaking_hdr   = "🚨 <b>خبر عاجل</b>\n" if breaking else ""
    source_tag     = f"\n📡 <i>{source}</i>" if source else ""

    return (
        f"{breaking_hdr}"
        f"{emoji} <b>{category_line}</b>  {priority}\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📋 {analysis.get('summary', '')}\n\n"
        f"🔗 <a href='{link}'>اقرأ المزيد</a>"
        f"{source_tag}\n"
        f"🕐 {now}"
    )


# ============================
# Command Handler (background thread)
# ============================
def handle_commands():
    log.info("Command listener started")
    offset = None

    TIER_MAP = {
        "🔴 وكالات أنباء": [
            "Reuters — Business", "Reuters — World",
            "AP News — Business", "AP News — World",
            "AFP", "UPI", "WAM",
        ],
        "📰 صحافة مالية عالمية": [
            "Bloomberg", "Financial Times", "Wall Street Journal", "CNBC",
            "Forbes", "Fortune", "MarketWatch", "The Economist",
            "Business Insider", "Yahoo Finance", "Seeking Alpha",
            "Morningstar", "Barron's",
        ],
        "🌐 صحافة دولية عامة": [
            "BBC — Business", "CNN — Business", "The Guardian",
            "New York Times", "Washington Post", "Sky News — Business",
            "Deutsche Welle", "Euronews", "Channel NewsAsia",
            "NBC News", "NPR Business",
            "South China Morning Post", "Nikkei Asia",
        ],
        "🌙 إقليمي وخليجي": [
            "Al Jazeera", "Sky News Arabia", "Al Arabiya",
            "Gulf News", "Khaleej Times", "Arabian Business",
            "The National UAE", "Zawya", "Arab News",
            "MEED", "Asharq Al-Awsat", "Al Ittihad", "Emarat Al Youm",
        ],
        "🔍 Google News": [
            "Google News — UAE EN", "Google News — UAE AR",
        ],
    }

    while True:
        updates = get_updates(offset)
        for update in updates:
            offset = update["update_id"] + 1
            msg    = update.get("message", {})
            text   = msg.get("text", "").strip()
            cid    = str(msg.get("chat", {}).get("id", ""))

            if not text.startswith("/"):
                continue

            cmd = text.split()[0].lower()
            sources = get_all_sources()
            total_feeds = sum(len(v) for v in sources.values())

            if cmd == "/start":
                send(
                    "🤖 <b>UAE Business News Bot</b>\n\n"
                    f"أهلاً! أنا بوت الأخبار الاقتصادية الإماراتية.\n"
                    f"أغطّي <b>{len(sources)} مصدر · {total_feeds} feed</b>\n\n"
                    "/help — قائمة الأوامر\n"
                    "/stats — إحصائيات\n"
                    "/latest — آخر 5 أخبار\n"
                    "/categories — أخبار حسب التصنيف\n"
                    "/sources — قائمة المصادر\n"
                    "/pause — إيقاف الإرسال\n"
                    "/resume — استئناف الإرسال\n"
                    "/status — حالة البوت",
                    chat_id=cid
                )

            elif cmd == "/help":
                send(
                    "📖 <b>قائمة الأوامر</b>\n\n"
                    "/start — رسالة ترحيب\n"
                    "/stats — إحصائيات الأخبار المرسلة\n"
                    "/latest — آخر 5 أخبار\n"
                    "/categories — الأخبار حسب التصنيف\n"
                    "/sources — قائمة المصادر المفعّلة\n"
                    "/pause — إيقاف الإرسال مؤقتاً\n"
                    "/resume — استئناف الإرسال\n"
                    "/status — هل البوت يعمل؟",
                    chat_id=cid
                )

            elif cmd == "/sources":
                lines = [f"🌍 <b>المصادر المفعّلة — {len(sources)} مصدر</b>\n"]
                for tier_name, names in TIER_MAP.items():
                    active = [n for n in names if n in sources]
                    if active:
                        lines.append(f"\n<b>{tier_name}</b>")
                        for name in active:
                            count = len(sources[name])
                            lines.append(f"  • {name} ({count} feed{'s' if count > 1 else ''})")
                send("\n".join(lines), chat_id=cid)

            elif cmd == "/stats":
                stats  = get_stats()
                total  = stats.get("total_sent", "0")
                brk    = stats.get("breaking", "0")
                cycles = stats.get("cycles", "0")
                lines  = [
                    "📊 <b>إحصائيات البوت</b>\n",
                    f"📨 إجمالي الأخبار: <b>{total}</b>",
                    f"🚨 أخبار عاجلة: <b>{brk}</b>",
                    f"🔄 دورات الفحص: <b>{cycles}</b>",
                    f"🌍 المصادر: <b>{len(sources)}</b>",
                    "\n🗂️ <b>حسب التصنيف:</b>"
                ]
                for k, v in sorted(stats.items(), key=lambda x: -int(x[1])):
                    if k not in ("total_sent", "breaking", "cycles"):
                        lines.append(f"  • {k}: {v}")
                send("\n".join(lines), chat_id=cid)

            elif cmd == "/latest":
                items = get_latest(5)
                if not items:
                    send("📭 لا توجد أخبار محفوظة بعد.", chat_id=cid)
                else:
                    out = "📰 <b>آخر 5 أخبار</b>\n\n"
                    for i, item in enumerate(items, 1):
                        out += (
                            f"{i}. <a href='{item['link']}'>{item['title'][:70]}...</a>\n"
                            f"   🗂️ {item['category']} · 🕐 {item['time']}\n\n"
                        )
                    send(out, chat_id=cid)

            elif cmd == "/categories":
                stats    = get_stats()
                cat_keys = [k for k in stats if k not in ("total_sent", "breaking", "cycles")]
                lines    = ["🗂️ <b>الأخبار حسب التصنيف</b>\n"]
                if cat_keys:
                    for k in sorted(cat_keys, key=lambda x: int(stats[x]), reverse=True):
                        lines.append(f"• {k}: <b>{stats[k]}</b> خبر")
                else:
                    lines.append("لا توجد بيانات بعد.")
                send("\n".join(lines), chat_id=cid)

            elif cmd == "/pause":
                pause_bot()
                send("⏸️ تم إيقاف الإرسال.\nاستخدم /resume للاستئناف.", chat_id=cid)

            elif cmd == "/resume":
                resume_bot()
                send("▶️ تم استئناف الإرسال ✅", chat_id=cid)

            elif cmd == "/status":
                paused = is_paused()
                stats  = get_stats()
                send(
                    f"🤖 <b>حالة البوت</b>\n\n"
                    f"الحالة: {'⏸️ متوقف' if paused else '✅ يعمل'}\n"
                    f"📨 أخبار مرسلة: {stats.get('total_sent', '0')}\n"
                    f"🌍 عدد المصادر: {len(sources)}\n"
                    f"🕐 {datetime.now().strftime('%H:%M · %d/%m/%Y')}",
                    chat_id=cid
                )

        time.sleep(2)


# ============================
# Process One Feed URL
# ============================
def process_feed(feed_url: str, seen_titles: set, source_name: str) -> int:
    count = 0
    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        log.error(f"RSS error [{source_name}]: {e}")
        return 0

    for entry in feed.entries[:15]:
        title = entry.get("title", "").strip()
        link  = entry.get("link", "")

        if not title or not link:
            continue

        norm = normalize(title)
        if norm in seen_titles or is_sent(norm):
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
            increment_stat("total_sent")
            increment_stat(analysis.get("main_category", "أخرى"))
            if analysis.get("breaking"):
                increment_stat("breaking")
            cat = analysis.get("main_category", "أخبار")
            sub = analysis.get("sub_category", "")
            save_latest(title, link, f"{cat} › {sub}" if sub else cat)

        time.sleep(1)

    return count


# ============================
# Main Loop
# ============================
def main():
    sources     = get_all_sources()
    total_feeds = sum(len(v) for v in sources.values())

    log.info(f"Bot started — {len(sources)} sources, {total_feeds} feeds")
    send(
        "🤖 <b>UAE Business News Bot</b> started ✅\n\n"
        f"🌍 <b>{len(sources)} مصدر · {total_feeds} feed</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔴 Reuters · AP · AFP · UPI · WAM\n"
        "📰 Bloomberg · FT · WSJ · CNBC · Forbes\n"
        "📰 MarketWatch · Economist · Business Insider\n"
        "🌐 BBC · CNN · Guardian · NYT · WashPost\n"
        "🌐 Sky · DW · Euronews · CNA · NPR\n"
        "🌙 Al Jazeera · Al Arabiya · Sky News Arabia\n"
        "🌙 Gulf News · Khaleej Times · The National\n"
        "🌙 Zawya · Arabian Business · MEED · Arab News\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💬 اكتب /help للأوامر"
    )

    threading.Thread(target=handle_commands, daemon=True).start()

    while True:
        if is_paused():
            log.info("Paused, sleeping 60s...")
            time.sleep(60)
            continue

        sources     = get_all_sources()
        total_feeds = sum(len(v) for v in sources.values())
        log.info(f"Starting cycle — {len(sources)} sources, {total_feeds} feeds")
        increment_stat("cycles")

        new_count   = 0
        seen_titles = set()

        for source_name, urls in sources.items():
            for url in urls:
                new_count += process_feed(url, seen_titles, source_name)

        log.info(f"Cycle done — {new_count} news sent")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
