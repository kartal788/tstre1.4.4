import os
import importlib.util
import json
import tempfile
import asyncio
from pymongo import MongoClient
from pyrogram import Client, filters
from Backend.helper.custom_filter import CustomFilters

CONFIG_PATH = "/home/debian/dfbot/config.env"

# ---------------- Database URL alma ----------------
def read_database_from_config():
    if os.path.exists(CONFIG_PATH):
        spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return getattr(config, "DATABASE", None)
    return None

def get_db_urls():
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ---------------- /vtindir komutu ----------------
@Client.on_message(filters.command("vtindir") & filters.private & CustomFilters.owner)
async def export_db_to_json(client, message):
    try:
        status_msg = await message.reply_text("⏳ JSON export hazırlanıyor...")
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file_path = f"{tmpdir}/db_export.json"
            combined_data = {"movies": [], "tv_shows": []}
            # ---------------- MOVIE ----------------
            for i, doc in enumerate(movie_col.find({})):
                doc.pop("_id", None)
                combined_data["movies"].append(doc)
                if i % 100 == 0:
                    await asyncio.sleep(0.1)  # FloodWait önlemi
            # ---------------- TV ----------------
            for i, doc in enumerate(series_col.find({})):
                doc.pop("_id", None)
                combined_data["tv_shows"].append(doc)
                if i % 100 == 0:
                    await asyncio.sleep(0.1)  # FloodWait önlemi
            with open(output_file_path,"w",encoding="utf-8") as f:
                json.dump(combined_data,f,ensure_ascii=False, indent=2)
            await status_msg.edit_text("📤 Dosya hazırlanıyor, gönderiliyor...")
            await client.send_document(
                chat_id=message.chat.id,
                document=output_file_path,
                caption=f"💾 Database export\nFilmler: {len(combined_data['movies'])} | Diziler: {len(combined_data['tv_shows'])}"
            )
        await status_msg.edit_text("✅ JSON export tamamlandı ve gönderildi!")
    except Exception as e:
        await message.reply_text(f"⚠️ Hata oluştu: {e}")
