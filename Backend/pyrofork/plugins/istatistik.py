from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
from datetime import datetime, timedelta
import os
import importlib.util
import json

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
USAGE_FILE = "/tmp/net_usage.json"
bot_start_time = time()
PAGE_SIZE = 10  # Her sayfada 10 günlük veri


# ---------------- Config Database ----------------
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
    uptime = f"{h} saat {m} dakika {s} saniye"
    return cpu, ram, free_disk, free_percent, uptime


# ---------------- Ağ Trafiği ----------------
def format_size(size_bytes):
    gb = 1024 ** 3
    mb = 1024 ** 2
    if size_bytes >= gb:
        return f"{size_bytes / gb:.2f} GB"
    return f"{size_bytes / mb:.2f} MB"


def read_usage_file():
    if not os.path.exists(USAGE_FILE):
        return {}
    with open(USAGE_FILE, "r") as f:
        return json.load(f)


def save_usage_file(data):
    with open(USAGE_FILE, "w") as f:
        json.dump(data, f)


def update_network_usage():
    counters = net_io_counters()
    upload_now = counters.bytes_sent
    download_now = counters.bytes_recv

    today = datetime.utcnow().strftime("%Y-%m-%d")
    usage_data = read_usage_file()

    usage_data[today] = {"uploaded": upload_now, "downloaded": download_now}

    cutoff = datetime.utcnow() - timedelta(days=30)
    usage_data = {k: v for k, v in usage_data.items() if datetime.strptime(k, "%Y-%m-%d") >= cutoff}

    save_usage_file(usage_data)

    daily_uploaded = usage_data[today]["uploaded"]
    daily_downloaded = usage_data[today]["downloaded"]

    month_uploaded = sum(v["uploaded"] for v in usage_data.values())
    month_downloaded = sum(v["downloaded"] for v in usage_data.values())

    total_uploaded = month_uploaded
    total_downloaded = month_downloaded

    daily_list = []
    for i in range(30):
        day = datetime.utcnow() - timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        data = usage_data.get(day_str, {"uploaded": 0, "downloaded": 0})
        daily_list.append(f"{day_str}: 📥 {format_size(data['downloaded'])} | 📤 {format_size(data['uploaded'])}")

    return daily_uploaded, daily_downloaded, month_uploaded, month_downloaded, total_uploaded, total_downloaded, daily_list


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily_uploaded, daily_downloaded, month_uploaded, month_downloaded, total_uploaded, total_downloaded, daily_list = update_network_usage()

        # Ana mesaj (özet)
        main_text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┠ <b>Bugün Yüklenen:</b> {format_size(daily_uploaded)}\n"
            f"┠ <b>Bugün İndirilen:</b> {format_size(daily_downloaded)}\n"
            f"┠ <b>Son 30 Gün Yüklenen:</b> {format_size(month_uploaded)}\n"
            f"┠ <b>Son 30 Gün İndirilen:</b> {format_size(month_downloaded)}\n"
            f"┖ <b>Toplam Yüklenen:</b> {format_size(total_uploaded)} | <b>Toplam İndirilen:</b> {format_size(total_downloaded)}\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}"
        )

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📅 30 Gün Detay", callback_data="page_0")]]
        )

        await message.reply_text(main_text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard, quote=True)

        # Veriyi bot hafızasında saklamak
        client.dailies = daily_list

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)


# ---------------- Callback Query ----------------
@Client.on_callback_query()
async def callback(client: Client, callback_query: CallbackQuery):
    try:
        data = callback_query.data
        if not data.startswith("page_"):
            return

        page = int(data.split("_")[1])
        daily_list = getattr(client, "dailies", [])

        if not daily_list:
            await callback_query.answer("Veri bulunamadı.")
            return

        start = page * PAGE_SIZE
        end = start + PAGE_SIZE
        page_text = "\n".join(daily_list[start:end])

        keyboard = []
        if start > 0:
            keyboard.append(InlineKeyboardButton("⬅️ Geri", callback_data=f"page_{page-1}"))
        if end < len(daily_list):
            keyboard.append(InlineKeyboardButton("➡️ İleri", callback_data=f"page_{page+1}"))

        await callback_query.message.edit_text(
            f"📅 <b>Son 30 Gün Detay (Sayfa {page+1}):</b>\n{page_text}",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([keyboard] if keyboard else None)
        )
        await callback_query.answer()
    except Exception as e:
        print("Callback hata:", e)
        await callback_query.answer("Hata oluştu.")
