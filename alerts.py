"""
UAE Business News Bot — Fixed
Changes:
  - Fixed broken Reuters/AP/Gulf News/Khaleej Times/WAM URLs
  - Added debug logging to explain why 0 news sent
  - Relaxed keyword filter (was too aggressive)
  - Added Redis flush command for stuck dedup cache
"""

from time import mktime
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import feedparser
import requests
import os
import time
import json
import logging
import re
import threading
import queue
from openai import OpenAI
import redis

# ============================
# Settings
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

REDIS_KEY        = "dubai_news_bot:sent_titles"
REDIS_STATS      = "dubai_news_bot:stats"
REDIS_LATEST     = "dubai_news_bot:latest"
REDIS_PAUSED     = "dubai_news_bot:paused"
REDIS_FEED_STATS = "dubai_news_bot:feed_stats"

CHECK_INTERVAL      = 600
MAX_FEED_WORKERS    = 15
TELEGRAM_RATE_LIMIT = 1.2
FEED_TIMEOUT        = 12


# ============================
# UAE Keyword Pre-filter (relaxed)
# ============================
UAE_KEYWORDS_EN = {
    "uae", "dubai", "abu dhabi", "abudhabi", "sharjah", "ajman",
    "ras al khaimah", "fujairah", "emirates", "emirati",
    "dfm", "adx", "adnoc", "etisalat", "du telecom",
    "mashreq", "fab ", "first abu dhabi", "emirates nbd",
    "dewa", "emaar", "aldar", "damac", "nakheel", "meraas",
    "flydubai", "air arabia", "expo city", "difc", "dmcc",
    "jebel ali", "zayed", "maktoum", "gulf", "gcc",
    "arabian", "middle east", "dirham", "riyadh", "saudi",
    "kuwait", "qatar", "bahrain", "oman",
}

UAE_KEYWORDS_AR = {
    "الإمارات", "إمارات", "دبي", "أبوظبي", "أبو ظبي",
    "الشارقة", "عجمان", "رأس الخيمة", "الفجيرة", "أم القيوين",
    "إماراتي", "إماراتية", "سوق دبي", "سوق أبوظبي",
    "أدنوك", "طيران الإمارات", "فلاي دبي", "ديوا",
    "إعمار", "ألدار", "داماك", "نخيل", "درهم", "دراهم",
    "الخليج", "مجلس التعاون", "أوبك", "السعودية", "الكويت",
    "قطر", "البحرين", "عُمان", "الرياض",
}

EXCLUDE_KEYWORDS = {
    "premier league", "champions league", "la liga", "bundesliga",
    "transfer fee", "cricket score", "ipl match", "nba game", "nfl game",
    "bollywood", "grammy award", "oscar winner",
}


def passes_keyword_filter(title: str) -> bool:
    title_lower = title.lower()

    for kw in EXCLUDE_KEYWORDS:
        if kw in title_lower:
            return False

    for kw in UAE_KEYWORDS_EN:
        if kw in title_lower:
            return True

    for kw in UAE_KEYWORDS_AR:
        if kw in title:
            return True

    # Pass ambiguous titles through to OpenAI — better safe than sorry
    return True


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
# ALL NEWS SOURCES — Fixed URLs
# ============================
def get_all_sources() -> dict:
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    return {

        # ── TIER 1: WIRE AGENCIES (fixed URLs) ────────────
        "Reuters — Business": [
            # Reuters changed their feed URLs
            f"https://news.google.com/rss/search?q=site:reuters.com+UAE+OR+Dubai+business+after:{yesterday}&hl=en&gl=AE",
            f"https://news.google.com/rss/search?q=site:reuters.com+Middle+East+economy+after:{yesterday}&hl=en",
            f"https://news.google.com/rss/search?q=site:reuters.com+Gulf+markets+after:{yesterday}&hl=en",
        ],
        "AP News — Business": [
            # AP News feeds via Google News
            f"https://news.google.com/rss/search?q=source:AP+UAE+OR+Dubai+business+after:{yesterday}&hl=en&gl=AE",
            f"https://news.google.com/rss/search?q=source:AP+Middle+East+economy+after:{yesterday}&hl=en",
        ],
        "AFP": [
            f"https://news.google.com/rss/search?q=source:AFP+UAE+OR+Dubai+economy+after:{yesterday}&hl=en&gl=AE",
        ],
        "UPI": [
            "https://rss.upi.com/news/business.rss",
        ],
        "WAM": [
            # WAM direct XML is broken — use Google News
            f"https://news.google.com/rss/search?q=site:wam.ae+after:{yesterday}&hl=ar&gl=AE",
            f"https://news.google.com/rss/search?q=WAM+UAE+economy+after:{yesterday}&hl=en&gl=AE",
        ],

        # ── TIER 2: GLOBAL FINANCIAL PRESS ────────────────
        "Bloomberg": [
            "https://feeds.bloomberg.com/markets/news.rss",
            "https://feeds.bloomberg.com/business/news.rss",
            f"https://news.google.com/rss/search?q=site:bloomberg.com+UAE+OR+Dubai+OR+Abu+Dhabi+after:{yesterday}&hl=en&gl=AE",
            f"https://news.google.com/rss/search?q=site:bloomberg.com+Middle+East+economy+after:{yesterday}&hl=en",
        ],
        "Financial Times": [
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
            f"https://news.google.com/rss/search?q=site:cnbc.com+Middle+East+OR+UAE+after:{yesterday}&hl=en",
        ],
        "Forbes": [
            # Forbes RSS broken — use Google News
            f"https://news.google.com/rss/search?q=site:forbes.com+UAE+OR+Dubai+after:{yesterday}&hl=en",
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
            f"https://news.google.com/rss/search?q=site:businessinsider.com+UAE+OR+Middle+East+after:{yesterday}&hl=en",
        ],
        "Yahoo Finance": [
            "https://finance.yahoo.com/news/rssindex",
        ],
        "Seeking Alpha": [
            f"https://news.google.com/rss/search?q=site:seekingalpha.com+UAE+OR+Gulf+after:{yesterday}&hl=en",
        ],
        "Barron's": [
            f"https://news.google.com/rss/search?q=site:barrons.com+UAE+OR+Gulf+after:{yesterday}&hl=en",
        ],

        # ── TIER 3: GENERAL INTERNATIONAL PRESS ───────────
        "BBC — Business": [
            "https://feeds.bbci.co.uk/news/business/rss.xml",
            f"https://news.google.com/rss/search?q=site:bbc.com+UAE+OR+Dubai+after:{yesterday}&hl=en",
        ],
        "CNN — Business": [
            "http://rss.cnn.com/rss/money_news_international.rss",
            "http://rss.cnn.com/rss/edition_business.rss",
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
        ],
        "Channel NewsAsia": [
            "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=business",
        ],
        "NPR Business": [
            "https://feeds.npr.org/1006/rss.xml",
        ],

        # ── TIER 4: MIDDLE EAST & GULF REGIONAL ───────────
        "Al Jazeera": [
            "https://www.aljazeera.com/xml/rss/all.xml",
            f"https://news.google.com/rss/search?q=site:aljazeera.com+UAE+economy+after:{yesterday}&hl=ar",
        ],
        "Sky News Arabia": [
            f"https://news.google.com/rss/search?q=site:skynewsarabia.com+الإمارات+after:{yesterday}&hl=ar",
            f"https://news.google.com/rss/search?q=site:skynewsarabia.com+دبي+after:{yesterday}&hl=ar",
        ],
        "Al Arabiya": [
            f"https://news.google.com/rss/search?q=site:alarabiya.net+الإمارات+اقتصاد+after:{yesterday}&hl=ar",
            f"https://news.google.com/rss/search?q=site:alarabiya.net+دبي+أبوظبي+after:{yesterday}&hl=ar",
        ],
        "Gulf News": [
            # Gulf News RSS broken — use Google News
            f"https://news.google.com/rss/search?q=site:gulfnews.com+after:{yesterday}&hl=en&gl=AE",
        ],
        "Khaleej Times": [
            # Khaleej Times RSS broken — use Google News
            f"https://news.google.com/rss/search?q=site:khaleejtimes.com+after:{yesterday}&hl=en&gl=AE",
        ],
        "Arabian Business": [
            # Arabian Business RSS broken — use Google News
            f"https://news.google.com/rss/search?q=site:arabianbusiness.com+after:{yesterday}&hl=en&gl=AE",
        ],
        "The National UAE": [
            # The National RSS broken — use Google News
            f"https://news.google.com/rss/search?q=site:thenationalnews.com+after:{yesterday}&hl=en&gl=AE",
        ],
        "Zawya": [
            f"https://news.google.com/rss/search?q=site:zawya.com+UAE+after:{yesterday}&hl=en",
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

def increment_stat(key: str, amount: int = 1):
    r.hincrby(REDIS_STATS, key, amount)

def get_stats() -> dict:
    return r.hgetall(REDIS_STATS) or {}

def save_latest(title: str, link: str, category: str):
    item = json.dumps({
        "title": title, "link": link,
        "category": category,
        "time": datetime.now().strftime("%H:%M · %d/%m/%Y")
    })
    r.lpush(REDIS_LATEST, item)
    r.ltrim(REDIS_LATEST, 0, 9)

def get_latest(n: int = 5) -> list:
    return [json.loads(i) for i in r.lrange(REDIS_LATEST, 0, n - 1)]

def flush_dedup_cache():
    """Clear the dedup cache — useful when bot sends 0 news due to stale Redis."""
    r.delete(REDIS_KEY)
    log.info("🗑️ Dedup cache flushed")

def track_feed(source: str, success: bool):
    field = f"{source}:{'ok' if success else 'fail'}"
    r.hincrby(REDIS_FEED_STATS, field, 1)

def get_feed_health() -> dict:
    return r.hgetall(REDIS_FEED_STATS) or {}


# ============================
# Telegram Rate-Limited Queue
# ============================
_tg_queue: queue.Queue = queue.Queue()

def _telegram_sender():
    while True:
        item = _tg_queue.get()
        if item is None:
            break
        msg, chat_id = item
        _send_direct(msg, chat_id)
        time.sleep(TELEGRAM_RATE_LIMIT)

def _send_direct(msg: str, chat_id: str, retries: int = 3) -> bool:
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    for attempt in range(retries):
        try:
            res = requests.post(
                url,
                data={
                    "chat_id": chat_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true"
                },
                timeout=10
            )
            if res.status_code == 200:
                return True
            if res.status_code == 429:
                retry_after = res.json().get("parameters", {}).get("retry_after", 30)
                log.warning(f"Rate limited — sleeping {retry_after}s")
                time.sleep(retry_after)
                continue
            log.warning(f"Telegram {res.status_code}: {res.text[:100]}")
        except requests.RequestException as e:
            log.error(f"Telegram error (attempt {attempt+1}): {e}")
        time.sleep(2 ** attempt)
    return False

def send(msg: str, chat_id: str = None):
    _tg_queue.put((msg, chat_id or CHAT_ID))

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
1. Check if the news is related to UAE OR Gulf region (companies, banks, markets, real estate, aviation, trade, investment, economy, energy, tech, fintech, crypto, oil, OPEC...)
2. If related → classify with MAIN + SUB category, importance score, breaking flag
3. If NOT related at all → "send": false

MAIN categories & SUB categories:
- أسواق مالية → [سوق دبي DFM, سوق أبوظبي ADX, سلع, صناديق, عملات, مؤشرات عالمية]
- بنوك ومالية → [بنوك إماراتية, بنوك خليجية, تمويل, تأمين, فنتك]
- عقارات → [دبي, أبوظبي, الشارقة, مشاريع جديدة, أسعار]
- شركات → [طاقة, تقنية, اتصالات, تجزئة, صناعة, شركات ناشئة]
- طيران وسياحة → [طيران الإمارات, فلاي دبي, مطارات, فنادق, سياحة]
- تجارة واقتصاد → [صادرات, واردات, موانئ, مناطق حرة, اتفاقيات]
- استثمار → [استثمار أجنبي, صناديق سيادية, M&A, IPO]
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

News: {title}"""

    for attempt in range(3):
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
        except json.JSONDecodeError:
            log.warning(f"JSON parse failed (attempt {attempt+1})")
        except Exception as e:
            log.error(f"OpenAI error (attempt {attempt+1}): {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
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

    priority      = "🔴 عاجل" if importance >= 8 else ("🟡 مهم" if importance >= 6 else "🟢 عادي")
    category_line = f"{main_cat} › {sub_cat}" if sub_cat else main_cat
    breaking_hdr  = "🚨 <b>خبر عاجل</b>\n" if breaking else ""
    source_tag    = f"\n📡 <i>{source}</i>" if source else ""

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
# Fetch ONE feed
# ============================
def fetch_feed(feed_url: str, source_name: str) -> list[dict]:
    try:
        feed = feedparser.parse(
            feed_url,
            request_headers={"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}
        )
        if feed.bozo and not feed.entries:
            raise ValueError(f"Bozo: {feed.bozo_exception}")
        track_feed(source_name, True)
        return [
            {
                "title":     e.get("title", "").strip(),
                "link":      e.get("link", ""),
                "published": e.get("published_parsed"),
                "source":    source_name,
            }
            for e in feed.entries[:15]
            if e.get("title", "").strip() and e.get("link", "")
        ]
    except Exception as e:
        log.warning(f"Feed error [{source_name}]: {type(e).__name__}: {str(e)[:80]}")
        track_feed(source_name, False)
        return []


# ============================
# Process Articles
# ============================
def process_articles(articles: list[dict], seen_titles: set) -> int:
    sent            = 0
    skipped_dedup   = 0
    skipped_age     = 0
    skipped_filter  = 0
    skipped_openai  = 0

    for art in articles:
        title  = art["title"]
        link   = art["link"]
        source = art["source"]
        pub    = art["published"]

        norm = normalize(title)
        if norm in seen_titles or is_sent(norm):
            skipped_dedup += 1
            continue
        seen_titles.add(norm)

        if pub:
            age_hours = (time.time() - mktime(pub)) / 3600
            if age_hours > 24:
                mark_sent(norm)
                skipped_age += 1
                continue

        if not passes_keyword_filter(title):
            skipped_filter += 1
            mark_sent(norm)
            continue

        analysis = analyze_news(title)
        mark_sent(norm)

        if not analysis or not analysis.get("send"):
            skipped_openai += 1
            continue

        msg = format_message(title, link, analysis, source)
        send(msg)
        sent += 1

        increment_stat("total_sent")
        increment_stat(analysis.get("main_category", "أخرى"))
        if analysis.get("breaking"):
            increment_stat("breaking")

        cat = analysis.get("main_category", "أخبار")
        sub = analysis.get("sub_category", "")
        save_latest(title, link, f"{cat} › {sub}" if sub else cat)

    # Debug log — shows exactly why articles weren't sent
    log.info(
        f"📊 Article breakdown — "
        f"sent: {sent} | "
        f"dedup: {skipped_dedup} | "
        f"age: {skipped_age} | "
        f"keyword: {skipped_filter} | "
        f"openai_rejected: {skipped_openai}"
    )

    return sent


# ============================
# Command Handler
# ============================
def handle_commands():
    log.info("Command listener started")
    offset = None

    while True:
        updates = get_updates(offset)
        for update in updates:
            offset = update["update_id"] + 1
            msg    = update.get("message", {})
            text   = msg.get("text", "").strip()
            cid    = str(msg.get("chat", {}).get("id", ""))

            if not text.startswith("/"):
                continue

            cmd     = text.split()[0].lower()
            sources = get_all_sources()
            total_f = sum(len(v) for v in sources.values())

            if cmd == "/start":
                send(
                    "🤖 <b>UAE Business News Bot</b>\n\n"
                    f"أهلاً! أغطّي <b>{len(sources)} مصدر · {total_f} feed</b>\n\n"
                    "/help · /stats · /latest · /categories\n"
                    "/sources · /health · /flush\n"
                    "/pause · /resume · /status",
                    chat_id=cid
                )

            elif cmd == "/help":
                send(
                    "📖 <b>قائمة الأوامر</b>\n\n"
                    "/start — رسالة ترحيب\n"
                    "/stats — إحصائيات\n"
                    "/latest — آخر 5 أخبار\n"
                    "/categories — حسب التصنيف\n"
                    "/sources — قائمة المصادر\n"
                    "/health — صحة المصادر\n"
                    "/flush — مسح cache التكرار (إذا 0 أخبار)\n"
                    "/pause — إيقاف الإرسال\n"
                    "/resume — استئناف الإرسال\n"
                    "/status — حالة البوت",
                    chat_id=cid
                )

            elif cmd == "/flush":
                cache_size = r.scard(REDIS_KEY)
                flush_dedup_cache()
                send(
                    f"🗑️ تم مسح الـ cache!\n"
                    f"حُذف {cache_size} عنوان محفوظ.\n"
                    f"الدورة القادمة ستُرسل الأخبار الجديدة.",
                    chat_id=cid
                )

            elif cmd == "/stats":
                stats  = get_stats()
                total  = stats.get("total_sent", "0")
                brk    = stats.get("breaking", "0")
                cycles = stats.get("cycles", "0")
                cache  = r.scard(REDIS_KEY)
                lines  = [
                    "📊 <b>إحصائيات البوت</b>\n",
                    f"📨 إجمالي الأخبار: <b>{total}</b>",
                    f"🚨 أخبار عاجلة: <b>{brk}</b>",
                    f"🔄 دورات الفحص: <b>{cycles}</b>",
                    f"💾 Cache size: <b>{cache}</b> عنوان",
                    "\n🗂️ <b>حسب التصنيف:</b>"
                ]
                skip = {"total_sent", "breaking", "cycles"}
                for k, v in sorted(stats.items(), key=lambda x: -int(x[1])):
                    if k not in skip:
                        lines.append(f"  • {k}: {v}")
                send("\n".join(lines), chat_id=cid)

            elif cmd == "/latest":
                items = get_latest(5)
                if not items:
                    send("📭 لا توجد أخبار محفوظة بعد.\nجرب /flush إذا البوت لا يرسل.", chat_id=cid)
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
                skip     = {"total_sent", "breaking", "cycles"}
                cat_keys = [k for k in stats if k not in skip]
                lines    = ["🗂️ <b>الأخبار حسب التصنيف</b>\n"]
                for k in sorted(cat_keys, key=lambda x: int(stats[x]), reverse=True):
                    lines.append(f"• {k}: <b>{stats[k]}</b> خبر")
                send("\n".join(lines) if cat_keys else "لا توجد بيانات بعد.", chat_id=cid)

            elif cmd == "/sources":
                lines = [f"🌍 <b>المصادر — {len(sources)} مصدر</b>\n"]
                for name, urls in sources.items():
                    lines.append(f"• {name} ({len(urls)} feeds)")
                send("\n".join(lines), chat_id=cid)

            elif cmd == "/health":
                health = get_feed_health()
                if not health:
                    send("📭 لا توجد بيانات صحة بعد.", chat_id=cid)
                    continue
                source_names = set(k.rsplit(":", 1)[0] for k in health)
                good, bad = [], []
                for s in sorted(source_names):
                    ok   = int(health.get(f"{s}:ok", 0))
                    fail = int(health.get(f"{s}:fail", 0))
                    total_calls = ok + fail
                    pct  = int(ok / total_calls * 100) if total_calls else 0
                    icon = "✅" if pct >= 70 else ("⚠️" if pct >= 30 else "❌")
                    entry = f"{icon} {s}: {pct}%"
                    (good if pct >= 70 else bad).append(entry)
                lines = ["📊 <b>صحة المصادر</b>\n"]
                if bad:
                    lines.append("<b>❌ تحتاج إصلاح:</b>")
                    lines.extend(bad)
                    lines.append("")
                lines.append("<b>✅ تعمل بشكل جيد:</b>")
                lines.extend(good[:20])
                send("\n".join(lines), chat_id=cid)

            elif cmd == "/pause":
                pause_bot()
                send("⏸️ تم إيقاف الإرسال.\nاستخدم /resume للاستئناف.", chat_id=cid)

            elif cmd == "/resume":
                resume_bot()
                send("▶️ تم استئناف الإرسال ✅", chat_id=cid)

            elif cmd == "/status":
                stats = get_stats()
                send(
                    f"🤖 <b>حالة البوت</b>\n\n"
                    f"الحالة: {'⏸️ متوقف' if is_paused() else '✅ يعمل'}\n"
                    f"📨 أخبار مرسلة: {stats.get('total_sent','0')}\n"
                    f"💾 Cache: {r.scard(REDIS_KEY)} عنوان\n"
                    f"🌍 المصادر: {len(sources)}\n"
                    f"🕐 {datetime.now().strftime('%H:%M · %d/%m/%Y')}",
                    chat_id=cid
                )

        time.sleep(2)


# ============================
# Main Loop
# ============================
def main():
    sources     = get_all_sources()
    total_feeds = sum(len(v) for v in sources.values())

    log.info(f"Bot started — {len(sources)} sources, {total_feeds} feeds")

    # Start Telegram sender thread
    threading.Thread(target=_telegram_sender, daemon=True).start()

    # Start command listener thread
    threading.Thread(target=handle_commands, daemon=True).start()

    send(
        "🤖 <b>UAE Business News Bot</b> started ✅\n\n"
        f"🌍 <b>{len(sources)} مصدر · {total_feeds} feed</b>\n"
        f"⚡ معالجة متوازية ({MAX_FEED_WORKERS} workers)\n"
        f"🔧 URLs محدّثة ومُصلحة\n"
        "💬 /flush إذا البوت لا يرسل أخبار\n"
        "💬 /help للأوامر"
    )

    while True:
        if is_paused():
            log.info("Paused — sleeping 60s")
            time.sleep(60)
            continue

        sources     = get_all_sources()
        total_feeds = sum(len(v) for v in sources.values())
        cycle_start = time.time()
        log.info(f"▶ Cycle start — {total_feeds} feeds across {len(sources)} sources")
        increment_stat("cycles")

        tasks = [
            (url, source_name)
            for source_name, urls in sources.items()
            for url in urls
        ]

        all_articles: list[dict] = []
        with ThreadPoolExecutor(max_workers=MAX_FEED_WORKERS) as executor:
            futures = {
                executor.submit(fetch_feed, url, src): (url, src)
                for url, src in tasks
            }
            for future in as_completed(futures):
                try:
                    all_articles.extend(future.result(timeout=FEED_TIMEOUT + 5))
                except Exception as e:
                    _, src = futures[future]
                    log.error(f"Future error [{src}]: {e}")

        log.info(f"Fetched {len(all_articles)} raw articles from {len(tasks)} feeds")

        seen_titles: set = set()
        sent_count = process_articles(all_articles, seen_titles)

        elapsed = time.time() - cycle_start
        log.info(f"✅ Cycle done in {elapsed:.1f}s — {sent_count} news sent")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
