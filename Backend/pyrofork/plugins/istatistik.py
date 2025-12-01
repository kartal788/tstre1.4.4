from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
from datetime import datetime
import os
import json

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
TRAFFIC_FILE = "/tmp/traffic_stats.json"  # Docker geçici dosyası

# ---------------- MongoDB Config ----------------
import importlib.util

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

# ---------------- MongoDB Film/Dizi İstatistik ----------------
def get_db_stats(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0
    db = client[db_name_list[0]]
    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024*1024), 2)
    return movies, series, storage_mb

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=0), 1)
    ram = round(virtual_memory().percent, 1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)
    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h} saat {m} dakika {s} saniye"
    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Ağ Trafiği (Dosya Tabanlı) ----------------
def format_size(size):
    tb = 1024**4
    gb = 1024**3
    if size >= tb:
        return f"{size/tb:.2f}TB"
    elif size >= gb:
        return f"{size/gb:.2f}GB"
    else:
        return f"{size/(1024**2):.2f}MB"

def load_traffic():
    if not os.path.exists(TRAFFIC_FILE):
        return {"daily": {}, "monthly": {}}
    with open(TRAFFIC_FILE, "r") as f:
        try:
            return json.load(f)
        except:
            return {"daily": {}, "monthly": {}}

def save_traffic(data):
    with open(TRAFFIC_FILE, "w") as f:
        json.dump(data, f)

def get_network_usage():
    counters = net_io_counters()
    return counters.bytes_sent, counters.bytes_recv

def update_traffic_stats():
    data = load_traffic()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    month = datetime.utcnow().strftime("%Y-%m")
    sent, recv = get_network_usage()

    # Günlük ve aylık
    data.setdefault("daily", {})
    data.setdefault("monthly", {})
    data["daily"][today] = {"upload": sent, "download": recv}
    data["monthly"][month] = {"upload": sent, "download": recv}

    save_traffic(data)

    daily_up = format_size(data["daily"][today]["upload"])
    daily_down = format_size(data["daily"][today]["download"])
    month_up = format_size(data["monthly"][month]["upload"])
    month_down = format_size(data["monthly"][month]["download"])

    return daily_up, daily_down, month_up, month_down, format_size(sent), format_size(recv)

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])
        elif len(db_urls) == 1:
            movies, series, storage_mb = get_db_stats(db_urls[0])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily_up, daily_down, month_up, month_down, realtime_up, realtime_down = update_traffic_stats()

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┟ <b>İndirilen:</b> {realtime_up}\n"
            f"┠ <b>Yüklenen:</b> {realtime_down}\n"
            f"┠ <b>Bugün İndirilen:</b> {daily_down}\n"
            f"┠ <b>Bugün Yüklenen:</b> {daily_up}\n"
            f"┠ <b>Aylık İndirilen:</b> {month_down}\n"
            f"┖ <b>Aylık Yüklenen:</b> {month_up}\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş Disk</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}"
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
