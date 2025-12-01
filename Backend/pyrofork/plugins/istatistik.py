from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util
from datetime import datetime, timedelta
import random

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


# ---------------- Günlük Trafik Simülasyonu ----------------
def generate_traffic(days=30):
    traffic = []
    total_download = 0
    total_upload = 0

    for i in range(days):
        download = random.randint(1, 10)  # GB
        upload = random.randint(0, 2)    # GB
        traffic.append({
            "date": (datetime.now() - timedelta(days=days - i - 1)).strftime("%d.%m.%Y"),
            "download": download,
            "upload": upload,
            "total": round(download + upload, 2)
        })
        total_download += download
        total_upload += upload

    return traffic, total_download, total_upload


# ---------------- İstatistik Mesajı ----------------
def build_statistics_page(page=0, db_stats=(0,0,0), system_stats=(0,0,0,0,"")):
    movies, series, storage_mb = db_stats
    cpu, ram, free_disk, free_percent, uptime = system_stats

    if page == 0:
        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ Filmler: {movies}\n"
            f"┠ Diziler: {series}\n"
            f"┖ Depolama: {storage_mb} MB\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ 30 Günlük Trafik", callback_data="next_page")],
            [InlineKeyboardButton("❌ İptal", callback_data="cancel")]
        ])
    else:
        traffic, total_download, total_upload = generate_traffic()
        lines = [f"┠{t['date']} İndirilen {t['download']}GB Yüklenen {t['upload']}GB Toplam: {t['total']}GB" for t in traffic]
        text = "⌬ 30 Günlük Trafik\n│\n" + "\n".join(lines) + \
               f"\n\n┠Toplam İndirilen: {total_download}GB" + \
               f"\n┠Toplam Yüklenen: {total_upload}GB" + \
               f"\n┖Toplam Kullanım: {round(total_download+total_upload,2)}GB"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Geri", callback_data="prev_page")],
            [InlineKeyboardButton("❌ İptal", callback_data="cancel")]
        ])
    return text, keyboard


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client, message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0

        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        system_stats = get_system_status()
        db_stats_tuple = (movies, series, storage_mb)
        text, keyboard = build_statistics_page(0, db_stats_tuple, system_stats)

        await message.reply_text(text, reply_markup=keyboard, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        print("istatistik hata:", e)


# ---------------- Callback Query ----------------
@Client.on_callback_query()
async def callback_handler(client, callback_query):
    data = callback_query.data
    if data == "next_page":
        # 30 günlük trafik sayfası
        text, keyboard = build_statistics_page(1)
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=enums.ParseMode.HTML)
    elif data == "prev_page":
        # Ana istatistik sayfası
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])
        system_stats = get_system_status()
        db_stats_tuple = (movies, series, storage_mb)
        text, keyboard = build_statistics_page(0, db_stats_tuple, system_stats)
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=enums.ParseMode.HTML)
    elif data == "cancel":
        # Mesajı sil
        await callback_query.message.delete()
        await callback_query.answer()
