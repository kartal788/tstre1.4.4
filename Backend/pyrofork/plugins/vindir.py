from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import json
from time import time
from dotenv import load_dotenv

CONFIG_PATH = "/home/debian/dfbot/config.env"
flood_wait = 30  # saniye
last_command_time = {}  # kullanıcı_id : zaman

# ---------------- .env Yükleme ----------------
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

# ---------------- Koleksiyonları JSON'a Çekme ----------------
def export_collections_to_json(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return None

    db = client[db_name_list[0]]

    # _id hariç tüm dokümanlar
    movie_data = list(db["movie"].find({}, {"_id": 0}))
    tv_data = list(db["tv"].find({}, {"_id": 0}))

    return {"movie": movie_data, "tv": tv_data}

# ---------------- /vtindir Komutu ----------------
@Client.on_message(filters.command("vtindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    # Flood kontrolü
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        combined_data = export_collections_to_json(db_urls[1])
        if combined_data is None:
            await message.reply_text("⚠️ Koleksiyonlar boş veya bulunamadı.")
            return

        # Dosya yolu
        file_path = "/tmp/vt_collections.json"

        # JSON yazarken datetime ve diğer serialize edilemeyen tipleri string yap
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

        # Telegram'a gönder
        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📁 Movie ve TV Koleksiyonları"
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("vtindir hata:", e)
