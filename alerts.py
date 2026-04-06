import feedparser
import requests
import os
import time
import json
import logging
from datetime import datetime
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
# 📰 مصادر الأخبار (متعددة)
# ============================
RSS_FEEDS = [
    "https://news.google.com/rss/search?q=Dubai+economy&hl=en&gl=AE&ceid=AE:en",
    "https://news.google.com/rss/search?q=دبي+اقتصاد&hl=ar&gl=AE&ceid=AE:ar",
    "https://news.google.com/rss/search?q=Dubai+real+estate+finance&hl=en&gl=AE&ceid=AE:en",
]

CHECK_INTERVAL = 600   # 10 دقائق
SENT_FILE      = "sent_news.json"

# ============================
# 💾 حفظ الأخبار المرسلة (لمنع التكرار بعد الريستارت)
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
    # نحتفظ فقط بآخر 500 خبر لتجنب تضخم الملف
    trimmed = list(sent)[-500:]
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(trimmed, f, ensure_ascii=False)

# ============================
# 📲 إرسال رسالة Telegram مع retry
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
            log.error(f"خطأ في الاتصال بـ Telegram (محاولة {attempt+1}): {e}")
        time.sleep(2)
    return False

# ============================
# 🤖 تحليل الخبر بـ GPT
# ============================
def analyze_news(title: str, link: str) -> dict | None:
    """
    يرجع dict فيه:
      important: bool
      importance: str  (عالي / متوسط / منخفض)
      summary: str
      impact: str
    أو None عند الخطأ
    """
    prompt = f"""
أنت محلل اقتصادي متخصص في اقتصاد دبي والإمارات.
حلّل الخبر التالي وأجب فقط بـ JSON صالح بهذا الشكل:

{{
  "important": true أو false,
  "importance": "عالي" أو "متوسط" أو "منخفض",
  "summary": "ملخص الخبر بجملة واحدة بالعربية",
  "impact": "التأثير المتوقع على الاقتصاد بجملة واحدة بالعربية"
}}

الخبر: {title}
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=300,
        )
        raw = response.choices[0].message.content.strip()
        # تنظيف لو أضاف GPT backticks
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.error(f"GPT أرجع JSON غلط: {e}\nالرد: {raw}")
    except Exception as e:
        log.error(f"خطأ في OpenAI: {e}")
    return None

# ============================
# 📝 تنسيق الرسالة
# ============================
IMPORTANCE_EMOJI = {
    "عالي":    "🔴",
    "متوسط":   "🟡",
    "منخفض":   "🟢",
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
    send("🤖 <b>بوت أخبار دبي الاقتصادية</b> بدأ العمل ✅")

    while True:
        log.info("🔍 جاري فحص الأخبار...")
        new_count = 0

        for feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
            except Exception as e:
                log.error(f"خطأ في قراءة RSS: {e}")
                continue

            for entry in feed.entries[:15]:  # أحدث 15 خبر من كل مصدر
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")

                if not title or title in sent_news:
                    continue

                analysis = analyze_news(title, link)
                if not analysis:
                    sent_news.add(title)  # نتجاهله لتجنب التكرار
                    continue

                if analysis.get("important"):
                    msg = format_message(title, link, analysis)
                    if send(msg):
                        new_count += 1
                        time.sleep(1)  # تجنب flood Telegram

                sent_news.add(title)

        save_sent(sent_news)
        log.info(f"✅ انتهى الفحص — {new_count} خبر مهم أُرسل")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
