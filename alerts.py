import feedparser
import requests
import os
import time
import json
import logging
from datetime import datetime

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
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([TOKEN, CHAT_ID, GEMINI_API_KEY]):
    raise EnvironmentError("❌ تأكد من ضبط TOKEN و CHAT_ID و GEMINI_API_KEY في Railway")

GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent?key={GEMINI_API_KEY}"

# ============================
# 📰 مصادر الأخبار
# ============================
RSS_FEEDS = [
    "https://news.google.com/rss/search?q=Dubai+economy&hl=en&gl=AE&ceid=AE:en",
    "https://news.google.com/rss/search?q=دبي+اقتصاد&hl=ar&gl=AE&ceid=AE:ar",
    "https://news.google.com/rss/search?q=Dubai+real+estate+finance&hl=en&gl=AE&ceid=AE:en",
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
# 🤖 تحليل الخبر بـ Gemini
# ============================
def analyze_news(title: str) -> dict | None:
    prompt = f"""أنت محلل اقتصادي متخصص في اقتصاد دبي والإمارات.
حلّل الخبر التالي وأجب فقط بـ JSON صالح بهذا الشكل بدون أي نص إضافي:

{{
  "important": true,
  "importance": "عالي",
  "summary": "ملخص الخبر بجملة واحدة بالعربية",
  "impact": "التأثير المتوقع على الاقتصاد بجملة واحدة بالعربية"
}}

قيم "importance" المسموحة فقط: عالي أو متوسط أو منخفض
"important" يكون true إذا كانت الأهمية عالي أو متوسط، وfalse إذا كانت منخفض

الخبر: {title}"""

    for attempt in range(3):
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 300}
            }
            r = requests.post(GEMINI_URL, json=payload, timeout=15)

            # لو 429 انتظر وحاول مجدداً
            if r.status_code == 429:
                wait = 15 * (attempt + 1)
                log.warning(f"⏳ Gemini 429 — انتظار {wait} ثانية (محاولة {attempt+1}/3)")
                time.sleep(wait)
                continue

            r.raise_for_status()
            raw = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(raw)

        except json.JSONDecodeError as e:
            log.error(f"Gemini أرجع JSON غلط: {e}")
            return None
        except Exception as e:
            log.error(f"خطأ في Gemini API: {e}")
            return None

    log.error("❌ فشل الاتصال بـ Gemini بعد 3 محاولات")
    return None

# ============================
# 📝 تنسيق الرسالة
# ============================
IMPORTANCE_EMOJI = {
    "عالي":  "🔴",
    "متوسط": "🟡",
    "منخفض": "🟢",
}

def format_message(title: str, link: str, analysis: dict) -> str:
    emoji = IMPORTANCE_EMOJI.get(analysis.get("importance", ""), "📊")
    now   = datetime.now().strftime("%H:%M · %d/%m/%Y")
    return (
        f"{emoji} <b>خبر اقتصادي - دبي</b>\n\n"
        f"📌 <b>{title}</b>\n\n"
        f"📋 <b>الملخص:</b> {analysis.get('summary', '')}\n"
        f"📈 <b>التأثير:</b> {analysis.get('impact', '')}\n"
        f"⚡ <b>الأهمية:</b> {analysis.get('importance', '')}\n\n"
        f"🔗 <a href='{link}'>اقرأ الخبر كاملاً</a>\n"
        f"🕐 {now}"
    )

# ============================
# 🔄 الحلقة الرئيسية
# ============================
def main():
    sent_news = load_sent()
    log.info(f"🚀 البوت بدأ — {len(sent_news)} خبر محفوظ مسبقاً")
    send("🤖 <b>بوت أخبار دبي الاقتصادية</b> بدأ العمل ✅\n⚡ يعمل بـ Gemini AI مجاناً")

    while True:
        log.info("🔍 جاري فحص الأخبار...")
        new_count = 0

        for feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
            except Exception as e:
                log.error(f"خطأ في قراءة RSS: {e}")
                continue

            for entry in feed.entries[:15]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")

                if not title or title in sent_news:
                    continue

                analysis = analyze_news(title)
                sent_news.add(title)

                if not analysis:
                    continue

                if analysis.get("important"):
                    msg = format_message(title, link, analysis)
                    if send(msg):
                        new_count += 1

                time.sleep(8)  # انتظار بعد كل تحليل

        save_sent(sent_news)
        log.info(f"✅ انتهى الفحص — {new_count} خبر مهم أُرسل")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
