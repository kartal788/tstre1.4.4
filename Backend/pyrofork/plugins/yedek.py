from pyrogram import Client, filters
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from pymongo import MongoClient
import os
import importlib.util

CONFIG_PATH = "/home/debian/tstre1.4.4/config.py"

def read_database_from_config():
    """config.py içinden DATABASE değişkenini al"""
    if not os.path.exists(CONFIG_PATH):
        return None

    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)

    return getattr(config, "DATABASE", None)

def get_db_urls():
    """Önce config.py → yoksa env"""
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")

    return [u.strip() for u in db_raw.split(",") if u.strip()]

def get_db_stats(url):
    """MongoDB istatistiklerini al"""
    client = MongoClient(url)
    db_name = client.list_database_names()[0] if client.list_database_names() else None
    if not db_name:
        return None

    db = client[db_name]
    movies_count = db["movie"].count_documents({})
    series_count = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats["storageSize"] / (1024 * 1024), 2)

    return movies_count, series_count, storage_mb

@Client.on_message(filters.command("yedek") & filters.private & CustomFilters.owner)
async def database_status(_, message: Message):
    try:
        db_urls = get_db_urls()
        if len(db_urls) < 2:
            return await message.reply_text("⚠️ İki adet DATABASE URL bulunamadı.")

        stats = get_db_stats(db_urls[1])
        if not stats:
            return await message.reply_text("⚠️ Database bilgisi alınamadı.")

        movies_count, series_count, storage_mb = stats

        text = (
            f"Filmler: {movies_count:,}\n"
            f"Diziler: {series_count:,}\n"
            f"Depolama:{storage_mb} MB"
        )

        await message.reply_text(text)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(e)
