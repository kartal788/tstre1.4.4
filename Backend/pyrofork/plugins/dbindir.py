from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util
import json
import datetime
import tempfile
import os

# ------------ DATABASE Bağlantısı ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

# ------------ /dbindir Komutu (Tek JSON Dosya) ------------
@Client.on_message(filters.command("dbindir") & filters.private & CustomFilters.owner)
async def download_database(client, message: Message):
    start_msg = await message.reply_text("💾 Database hazırlanıyor, lütfen bekleyin...")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp_file_path = tmp_file.name
    tmp_file.close()

    try:
        # Tüm koleksiyonları tek sözlükte birleştir
        db_data = {}
        for col_name in db.list_collection_names():
            db_data[col_name] = list(db[col_name].find({}))

        # Tek JSON dosyası olarak kaydet
        with open(tmp_file_path, "w", encoding="utf-8") as f:
            json.dump(db_data, f, default=str, ensure_ascii=False)

        # Telegram'a gönder
        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_file_path,
            caption=f"📂 Veritabanı: {db_name} ({timestamp})"
        )

        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ Database indirilemedi.\nHata: {e}")

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
