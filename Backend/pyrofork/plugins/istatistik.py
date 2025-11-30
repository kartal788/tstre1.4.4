from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util

CONFIG_PATH = "/home/debian/tstre1.4.4/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()


# ---------------- Config Database Okuma ----------------
def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)


def get_db_urls():
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
    return [u.strip() for u in db_raw.split(",") if u.strip()]


# ---------------- Database İstatistikleri ----------------
def get_db_stats(url):
    client = MongoClient(url)

    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0

    db = client[db_name_list[0]]

    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)

    return movies, series, storage_mb


# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=1), 1)
    ram = round(virtual_memory().percent, 1)

    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h}h{m}m{s}s"

    return cpu, ram, free_disk, free_percent, uptime


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()

        movies = series = storage_mb = 0

        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş Alan</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>UP</b> → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
