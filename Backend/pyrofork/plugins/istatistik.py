from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
from datetime import datetime, timedelta
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


# ---------------- Ağ Trafiği ----------------
def format_size(size_bytes):
    gb = 1024 ** 3
    mb = 1024 ** 2
    if size_bytes >= gb:
        return f"{size_bytes/gb:.2f} GB"
    else:
        return f"{size_bytes/mb:.2f} MB"


def get_network_usage():
    counters = net_io_counters()
    return counters.bytes_sent, counters.bytes_recv


# ---------------- Trafik İstatistikleri ----------------
def get_daily_and_monthly_stats(db_url):
    client = MongoClient(db_url)
    db = client["TrafficStats"]
    col = db["daily_usage"]

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    month_str = datetime.utcnow().strftime("%Y-%m")

    # Günlük ve aylık verileri al
    daily = col.find_one({"date": today_str}) or {"upload": 0, "download": 0}
    month = col.find_one({"date": month_str}) or {"upload": 0, "download": 0}

    # Son 30 gün detay
    last_30_days = []
    for i in range(30):
        day = datetime.utcnow() - timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        day_data = col.find_one({"date": day_str}) or {"upload": 0, "download": 0}
        last_30_days.append(
            f"{i+1}. Gün → Yüklenen: {format_size(day_data['upload'])} | İndirilen: {format_size(day_data['download'])}"
        )
    last_30_days.reverse()  # Eski günler başta

    return daily, month, last_30_days


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily, month, last_30_days = get_daily_and_monthly_stats(db_urls[0])

        # Ana mesaj
        main_text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ Filmler: {movies}\n"
            f"┠ Diziler: {series}\n"
            f"┖ Depolama: {storage_mb} MB\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}\n\n"
            f"┠ Bugün Yüklenen: {format_size(daily['upload'])}\n"
            f"┠ Bugün İndirilen: {format_size(daily['download'])}\n"
            f"┖ Bugün Toplam: {format_size(daily['upload']+daily['download'])}\n\n"
            f"┠ Bu Ay Yüklenen: {format_size(month['upload'])}\n"
            f"┖ Bu Ay İndirilen: {format_size(month['download'])}\n"
            f"┖ Bu Ay Toplam: {format_size(month['upload']+month['download'])}"
        )

        # 30 Gün Detay butonu
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📅 30 Gün Detay", callback_data="last_30_days_page_0")]]
        )

        sent_message = await message.reply_text(main_text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)

        # Mesaj ve 30 gün detayını hafızaya al
        client.last_30_days = last_30_days
        client.message_id = sent_message.id
        client.chat_id = sent_message.chat.id

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)


# ---------------- 30 Gün Detay Callback ----------------
@Client.on_callback_query(filters.regex(r"last_30_days_page_(\d+)"))
async def last_30_days_callback(client: Client, callback_query: CallbackQuery):
    page = int(callback_query.data.split("_")[-1])
    per_page = 10
    last_30_days = client.last_30_days

    start = page * per_page
    end = start + per_page
    page_text = "\n".join(last_30_days[start:end])

    # Sayfa butonları
    keyboard = []
    if start > 0:
        keyboard.append(InlineKeyboardButton("⬅️ Önceki", callback_data=f"last_30_days_page_{page-1}"))
    if end < len(last_30_days):
        keyboard.append(InlineKeyboardButton("➡️ Sonraki", callback_data=f"last_30_days_page_{page+1}"))
    markup = InlineKeyboardMarkup([keyboard] if keyboard else [])

    await client.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.id,
        text=f"📅 <b>Son 30 Gün Detay (Sayfa {page+1}):</b>\n{page_text}",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=markup
    )
    await callback_query.answer()
