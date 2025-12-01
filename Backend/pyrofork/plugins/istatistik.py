from pyrogram import Client, filters, enums
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util

CONFIG_PATH = "/home/debian/dfbot/config.env"
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
    uptime = f"{h}s{m}d{s}s"

    return cpu, ram, free_disk, free_percent, uptime


# ---------------- Aylık Trafik ----------------
def get_monthly_traffic():
    # Örnek veri, kendi DB veya loglardan çekebilirsin
    upload_mb = 0.19
    download_mb = 1.33
    total_mb = round(upload_mb + download_mb, 2)
    return upload_mb, download_mb, total_mb


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client, message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        upload_mb, download_mb, total_mb = get_monthly_traffic()

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ Filmler: {movies}\n"
            f"┠ Diziler: {series}\n"
            f"┖ Depolama: {storage_mb} MB\n\n"
            f"┠ İndirilen: {download_mb}MB\n"
            f"┠ Yüklenen: {upload_mb}MB\n"
            f"┖ Toplam: {total_mb}MB\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        print("istatistik hata:", e)
