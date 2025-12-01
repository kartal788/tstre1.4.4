from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
import psutil
import os
import importlib.util
from datetime import datetime

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
    uptime = f"{h} saat {m} dakika {s} saniye"

    return cpu, ram, free_disk, free_percent, uptime


# ---------------- Ağ Trafiği ----------------
def format_size(size):
    tb = 1024 ** 4
    gb = 1024 ** 3

    if size >= tb:
        return f"{size / tb:.2f}TB"
    elif size >= gb:
        return f"{size / gb:.2f}GB"
    else:
        return f"{size / (1024 ** 2):.2f}MB"


def get_network_usage():
    counters = net_io_counters()
    return counters.bytes_sent, counters.bytes_recv


# ---------------- Günlük / Aylık Trafik ----------------
def update_traffic_stats(db_url):
    client = MongoClient(db_url)
    db = client["TrafficStats"]
    col = db["daily_usage"]

    today = datetime.utcnow().strftime("%Y-%m-%d")
    month = datetime.utcnow().strftime("%Y-%m")

    upload, download = get_network_usage()

    # Günlük veri
    col.update_one(
        {"date": today},
        {"$set": {"upload": upload, "download": download}},
        upsert=True
    )

    # Aylık veri
    col.update_one(
        {"date": month},
        {"$set": {"upload": upload, "download": download}},
        upsert=True
    )

    # Günlük
    daily = col.find_one({"date": today})
    monthly = col.find_one({"date": month})

    daily_up = format_size(daily["upload"])
    daily_down = format_size(daily["download"])

    month_up = format_size(monthly["upload"])
    month_down = format_size(monthly["download"])

    return daily_up, daily_down, month_up, month_down


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()

        movies = series = storage_mb = 0

        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        # Günlük / Aylık trafik istatistikleri
        daily_up, daily_down, month_up, month_down = update_traffic_stats(db_urls[0])

        # Gerçek zamanlı trafik
        sent, recv = get_network_usage()
        realtime_up = format_size(sent)
        realtime_down = format_size(recv)

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┟ <b>Upload:</b> {realtime_up}\n"
            f"┠ <b>Download:</b> {realtime_down}\n"
            f"┠ <b>Bugün Upload:</b> {daily_up}\n"
            f"┠ <b>Bugün Download:</b> {daily_down}\n"
            f"┠ <b>Bu Ay Upload:</b> {month_up}\n"
            f"┖ <b>Bu Ay Download:</b> {month_down}\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
