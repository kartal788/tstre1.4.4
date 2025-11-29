from pyrogram import Client, filters, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util

CONFIG_PATH = "/home/debian/tstre1.4.4/config.py"
TOTAL_STORAGE_MB = 500  # Toplam depolama
DOWNLOAD_DIR = "/"
bot_start_time = time()  # Bot başlama zamanı

# ---------------- Database Fonksiyonları ----------------
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

def get_db_stats(url):
    client = MongoClient(url)
    db_name = client.list_database_names()[0] if client.list_database_names() else None
    if not db_name:
        return None
    db = client[db_name]
    movies_count = db["movie"].count_documents({})
    series_count = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats["storageSize"] / (1024 * 1024), 2)
    storage_percent = round((storage_mb / TOTAL_STORAGE_MB) * 100, 2)
    return movies_count, series_count, storage_mb, storage_percent

# ---------------- Sistem Durumu Fonksiyonu ----------------
def get_system_status():
    cpu = cpu_percent(interval=1)
    ram = virtual_memory().percent
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # Sadece boş alan GB
    # total_disk artık gerek yok
    # disk_percent kaldırıldı
    uptime_sec = int(time() - bot_start_time)
    hours, remainder = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime = f"{hours}h{minutes}m{seconds}s"
    return cpu, ram, free_disk, uptime

# ---------------- /start Komutu ----------------
@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # DB stats
        db_urls = get_db_urls()
        db_stats_text = ""
        if len(db_urls) >= 2:
            stats = get_db_stats(db_urls[1])
            if stats:
                movies_count, series_count, storage_mb, storage_percent = stats
                db_stats_text = (
                    f"\n\nFilmler: {movies_count:,}"
                    f"\nDiziler: {series_count:,}"
                    f"\nDepolama: {storage_mb} MB"
                )

        # Sistem stats
        cpu, ram, free_disk, uptime = get_system_status()
        system_text = (
            f"\n\nBot Durumu:"
            f"\nCPU → {cpu}%"
            f"\nRAM → {ram}%"
            f"\nDisk → {free_disk} GB"
            f"\nUptime → {uptime}"
        )

        # Mesaj gönder
        await message.reply_text(
            f'<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n'
            f"Bu adresi Stremio > Eklentiler bölümüne ekleyerek kullanabilirsin."
            f"{db_stats_text}{system_text}",
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Bir hata oluştu: {e}")
        print(f"Error in /start handler: {e}")
