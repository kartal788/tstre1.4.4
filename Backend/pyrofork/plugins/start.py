from pyrogram import Client, filters, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram
from pymongo import MongoClient
import os
import importlib.util

CONFIG_PATH = "/home/debian/tstre1.4.4/config.py"
TOTAL_STORAGE_MB = 500  # Toplam depolama

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
    storage_percent = round((storage_mb / TOTAL_STORAGE_MB) * 100, 2)  # Ondalık gösterim
    return movies_count, series_count, storage_mb, storage_percent

@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        # Eklenti URL'si yazı olarak gösterilecek
        base_url = Telegram.BASE_URL
        addon_url_text = f"Eklenti adresin: {base_url}/stremio/manifest.json"

        db_urls = get_db_urls()
        db_stats_text = ""
        if len(db_urls) >= 2:
            stats = get_db_stats(db_urls[1])
            if stats:
                movies_count, series_count, storage_mb, storage_percent = stats
                db_stats_text = (
                    f"\n\nFilmler:  {movies_count:,}\n"
                    f"Diziler:  {series_count:,}\n"
                    f"Depolama: {storage_percent}% ({storage_mb} MB)"
                )

        # Mesajı tek f-string olarak gönder
        await message.reply_text(
            f"{addon_url_text}\nBu adresi Stremio > Eklentiler bölümüne ekleyerek kullanabilirsin."
            f"{db_stats_text}",
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Bir hata oluştu: {e}")
        print(f"Error in /start handler: {e}")
