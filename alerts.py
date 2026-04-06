import feedparser
import requests
import os
import time
from openai import OpenAI

# 🔐 المتغيرات من Railway
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 🤖 إعداد OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# 📰 مصدر الأخبار
url = "https://news.google.com/rss/search?q=Dubai&hl=en&gl=US&ceid=US:en"

# 🧠 تخزين الأخبار المرسلة (منع التكرار)
sent_news = set()

# 📲 إرسال إلى Telegram
def send(msg):
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": msg}
    )

# 🤖 تحليل الخبر بالذكاء الاصطناعي
def analyze_news(title):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are an economic analyst. Decide if the news is important for Dubai economy. Answer in Arabic with a short explanation. Start your answer with: مهم أو غير مهم."
                },
                {
                    "role": "user",
                    "content": title
                }
            ]
        )

        return response.choices[0].message.content

    except Exception as e:
        return f"خطأ في التحليل: {e}"

# 🔄 التشغيل المستمر
while True:
    print("Checking news...")

    feed = feedparser.parse(url)

    for entry in feed.entries:
        title = entry.title

        # ❌ منع التكرار
        if title in sent_news:
            continue

        # 🤖 تحليل AI
        analysis = analyze_news(title)

        # 🔥 فقط الأخبار المهمة
        if "مهم" in analysis:
            message = f"🚨 {title}\n{entry.link}\n\n{analysis}"
            send(message)
            sent_news.add(title)

    # ⏱️ كل 10 دقائق
    time.sleep(600)
