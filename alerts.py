from time import mktime
from datetime import datetime, timedelta
import feedparser
import requests
import os
import time
import json
import logging
from openai import OpenAI

# ============================
# 🔧 الإعدادات
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

TOKEN          = os.getenv("TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not all([TOKEN, CHAT_ID, OPENAI_API_KEY]):
    raise EnvironmentError("❌ تأكد من ضبط TOKEN و CHAT_ID و OPENAI_API_KEY في Railway")

client = OpenAI(api_key=OPENAI_API_KEY)

# ============================
# 📰 مصادر الأخبار
# ============================
def get_feeds():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return [
        # أخبار بيزنس شاملة بالإنجليزي
        f"https://news.google.com/rss/search?q=UAE+business+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=UAE+companies+stocks+banks+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=Dubai+AbuDhabi+business+market+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        f"https://news.google.com/rss/search?q=DFM+ADX+UAE+stocks+after:{yesterday}&hl=en&gl=AE&ceid=AE:en",
        # أخبار بيزنس شاملة بالعربي
        f"https://news.google.com/rss/search?q=الإمارات+أعمال+شركات+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        f"https://news.google.com/rss/search?q=دبي+أبوظبي+بنوك+أسهم+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
        f"https://news.google.com/rss/search?q=سوق+دبي+أبوظبي+المالي+after:{yesterday}&hl=ar&gl=AE&ceid=AE:ar",
    ]

CHECK_INTERVAL = 600
SENT_FILE      = "sent_news.json"

# ============================
# 💾 حفظ الأخبار المرسلة
# ============================
def load_sent() -> set:
    if os.path.exists(SENT_FILE):
        try:
            with open(SENT_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()

def save_sent(sent: set):
    trimmed = list(sent)[-500:]
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(trimmed, f, ensure_ascii=False)

# ============================
# 📲 إرسال رسالة Telegram
# ============================
def send(msg: str, retries: int = 3):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    for attempt in range(retries):
        try:
            r = requests.post(
                url,
                data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10
            )
            if r.status_code == 200:
                log.info("✅ تم إرسال الرسالة")
                return True
            else:
                log.warning(f"⚠️ فشل الإرسال ({r.status_code}): {r.text}")
        except requests.RequestException as e:
            log.error(f"خطأ في Telegram (محاولة {attempt+1}): {e}")
        time.sleep(2)
    return False

# ============================
# 🤖 تحليل الخبر بـ OpenAI
# ============================
def analyze_news(title: str) -> dict | None:
    prompt = f"""أنت محرر أخبار اقتصادية متخصص في الإمارات.

مهمتك:
1. تحقق إذا كان الخبر متعلق بالإمارات (شركات، بنوك، أسواق، قطاعات، مؤسسات، عقارات، طيران، تجارة، استثمار...)
2. إذا كان متعلقاً بالإمارات → أرسله بغض النظر عن أهميته
3. إذا لم يكن له علاقة بالإمارات نهائياً → لا ترسله

أجب فقط بـ JSON صالح بهذا الشكل بدون أي نص إضافي:

{{
  "send": true,
  "category": "أسواق مالية",
  "summary": "ملخص الخبر بجملة واحدة بالعربية",
  "emoji": "📈"
}}

قيم "category" المتاحة: أسواق مالية، بنوك، عقارات، شركات، طيران، تجارة، استثمار، اقتصاد كلي، أخرى
قيم "emoji": 📈 للأسواق، 🏦 للبنوك، 🏗️ للعقارات، ✈️ للطيران، 🏢 للشركات، 💰 للاستثمار، 📊 للاقتصاد
"send" يكون false فقط إذا كان الخبر لا علاقة له بالإمارات نهائياً

الخبر: {title}"""

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
        log.error(f"GPT أرجع JSON غلط: {e}")
    except Exception as e:
        log.error(f"خطأ في OpenAI API: {e}")
    return None

# ============================
# 📝 تنسيق الرسالة
# ============================
def format_message(title: str, link: str, analysis: dict) -> str:
    emoji    = analysis.get("emoji", "📊")
    category = analysis.get("category", "أخبار")
    now      = datetime.now().strftime("%H:%M · %d/%m/%Y")
    return (
        f"{emoji} <b>{category}</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📋 {analysis.get('summary', '')}\n\n"
        f"🔗 <a href='{link}'>اقرأ الخبر كاملاً</a>\n"
        f"🕐 {now}"
    )

# ============================
# 🔄 الحلقة الرئيسية
# ============================
def main():
    sent_news = load_sent()
    log.info(f"🚀 البوت بدأ — {len(sent_news)} خبر محفوظ مسبقاً")
    send("🤖 <b>بوت أخبار الإمارات الاقتصادية</b> بدأ العمل ✅\n⚡ يعمل بـ GPT-4o-mini")

    while True:
        log.info("🔍 جاري فحص الأخبار...")
        new_count = 0
        RSS_FEEDS = get_feeds()

        for feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
            except Exception as e:
                log.error(f"خطأ في قراءة RSS: {e}")
                continue

            for entry in feed.entries[:30]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")

                if not title or title in sent_news:
                    continue

                # تجاهل الأخبار الأقدم من 48 ساعة
                published = entry.get("published_parsed")
                if published:
                    age_hours = (time.time() - mktime(published)) / 3600
                    if age_hours > 48:
                        sent_news.add(title)
                        continue

                analysis = analyze_news(title)
                sent_news.add(title)

                if not analysis:
                    continue

                if analysis.get("send"):
                    msg = format_message(title, link, analysis)
                    if send(msg):
                        new_count += 1

                time.sleep(1)

        save_sent(sent_news)
        log.info(f"✅ انتهى الفحص — {new_count} خبر أُرسل")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
