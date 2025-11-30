from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util
import subprocess
import datetime

# ------------ DATABASE Bağlantısı ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"  # <- dfbot olarak değişti

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
client = MongoClient(MONGO_URL)
db_name = client.list_database_names()[0]
db = client[db_name]

# ------------ /dbindir Komutu ------------
@Client.on_message(filters.command("dbindir") & filters.private & CustomFilters.owner)
async def download_database(client: Client, message: Message):
    start_msg = await message.reply_text("💾 Database hazırlanıyor, lütfen bekleyin...")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_dir = f"/tmp/db_dump_{timestamp}"
    archive_path = f"/tmp/db_{timestamp}.zip"

    try:
        os.makedirs(dump_dir, exist_ok=True)
        subprocess.run([
            "mongodump",
            "--uri", MONGO_URL,
            "--db", db_name,
            "--out", dump_dir
        ], check=True)

        subprocess.run([
            "zip", "-r", archive_path, dump_dir
        ], check=True)

        await client.send_document(
            chat_id=message.chat.id,
            document=archive_path,
            caption=f"📂 Veritabanı: {db_name} ({timestamp})"
        )

        await start_msg.delete()
    except Exception as e:
        await start_msg.edit_text(f"❌ Database indirilemedi.\nHata: {e}")
    finally:
        if os.path.exists(dump_dir):
            subprocess.run(["rm", "-rf", dump_dir])
        if os.path.exists(archive_path):
            os.remove(archive_path)
