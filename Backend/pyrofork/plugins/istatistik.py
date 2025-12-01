from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
from datetime import datetime, timedelta
import os
import importlib.util

from Backend.helper.custom_filter import CustomFilters  # <-- CustomFilters buraya eklendi

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
    free_disk = round(disk.free / (1024 ** 3), 2)
    free_percent = round((disk.free / disk.total) * 100, 1)
    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h}s {m}d {s}s"
    return cpu, ram, free_disk, free_percent, uptime


# ---------------- Ağ Trafiği ----------------
def format_size(size):
    tb = 1024 ** 4
    gb = 1024 ** 3
    mb = 1024 ** 2
    if size >= tb:
        return f"{size / tb:.2f}TB"
    elif size >= gb:
        return f"{size / gb:.2f}GB"
    else:
        return f"{size / mb:.2f}MB"


def get_network_usage():
    import psutil
    counters = psutil.net_io_counters()
    return counters.bytes_sent, counters.bytes_recv


# ---------------- Trafik Verisi ----------------
def update_traffic_stats(db_url):
    client = MongoClient(db_url)
    db = client["TrafficStats"]
    col = db["daily_usage"]

    today = datetime.utcnow().strftime("%Y-%m-%d")
    month_start = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

    upload, download = get_network_usage()

    # Günlük veri
    col.update_one({"date": today}, {"$set": {"upload": upload, "download": download}}, upsert=True)

    # 30 günlük veri
    thirty_days = list(col.find({"date": {"$gte": month_start}}))
    total_up = sum(d.get("upload", 0) for d in thirty_days)
    total_down = sum(d.get("download", 0) for d in thirty_days)

    return format_size(upload), format_size(download), format_size(total_up), format_size(total_down), thirty_days


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily_up, daily_down, total_up, total_down, thirty_days = update_traffic_stats(db_urls[0])

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
            f"┠ <b>Bugün Yüklenen:</b> {daily_up}\n"
            f"┖ <b>Bugün İndirilen:</b> {daily_down}\n"
            f"┠ <b>Son 30 Gün Yüklenen:</b> {total_up}\n"
            f"┖ <b>Son 30 Gün İndirilen:</b> {total_down}"
        )

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📄 Son 30 Gün Detay", callback_data="30gün_detay")]]
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)


# ---------------- Callback Query ----------------
@Client.on_callback_query(filters.create(lambda _, __, query: query.data == "30gün_detay") & CustomFilters.owner)
async def show_30day_detail(client: Client, query):
    db_urls = get_db_urls()
    _, _, _, _, thirty_days = update_traffic_stats(db_urls[0])
    text = "<b>📄 Son 30 Gün Detay</b>\n\n"
    for day in thirty_days:
        text += f"{day['date']} → Yüklenen: {format_size(day.get('upload',0))} | İndirilen: {format_size(day.get('download',0))}\n"

    await query.message.edit_text(text, parse_mode=enums.ParseMode.HTML)
