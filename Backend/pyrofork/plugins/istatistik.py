from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
from datetime import datetime
import os, importlib.util, json

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
DAILY_FILE = "daily_traffic.json"
PAGE_SIZE = 30  # 30 günlük sayfa

# ---------------- JSON Yardımcı ----------------
def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def format_bytes(b):
    tb = 1024**4; gb = 1024**3; mb = 1024**2
    if b >= tb: return f"{b/tb:.2f}TB"
    if b >= gb: return f"{b/gb:.2f}GB"
    return f"{b/mb:.2f}MB"

# ---------------- Trafik Güncelle ----------------
def update_daily_traffic():
    today = datetime.utcnow().strftime("%d.%m.%Y")
    counters = net_io_counters()
    daily = load_json(DAILY_FILE)
    daily[today] = {"upload": counters.bytes_sent, "download": counters.bytes_recv}
    save_json(DAILY_FILE, daily)

# ---------------- Sayfa Metni ----------------
def get_page_text(page_index=0, movies=0, series=0, storage_mb=0):
    daily = load_json(DAILY_FILE)
    dates = sorted(daily.keys(), reverse=True)
    total_pages = max(1, (len(dates)-1)//PAGE_SIZE + 1)

    if page_index == 0:
        # İlk sayfa: Film/Dizi/Depolama + CPU/RAM/Disk + Süre
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        text = (
            "⌬ <b>İstatistik</b>\n│\n"
            f"┠ Filmler: {movies}\n"
            f"┠ Diziler: {series}\n"
            f"┖ Depolama: {storage_mb} MB\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )
        return text
    else:
        # Sonraki sayfalar: Trafik detayları
        start = (page_index-1) * PAGE_SIZE
        end = start + PAGE_SIZE
        page_dates = dates[start:end]

        lines = []
        total_up = 0
        total_down = 0

        for d in page_dates:
            up = daily[d]["upload"]
            down = daily[d]["download"]
            total_up += up
            total_down += down
            lines.append(f"┠{d} İndirilen {format_bytes(down)} Yüklenen {format_bytes(up)} Toplam: {format_bytes(up+down)}")

        total_line = (
            f"\n┠Toplam İndirilen: {format_bytes(total_down)}\n"
            f"┠Toplam Yüklenen: {format_bytes(total_up)}\n"
            f"┖Toplam Kullanım: {format_bytes(total_up + total_down)}"
        )

        text = "30 Günlük Trafik:\n" + "\n".join(lines) + total_line
        return text

# ---------------- Klavye ----------------
def get_keyboard(page_index, max_page):
    row = []
    if page_index > 0:
        row.append(InlineKeyboardButton("◀️ Geri", callback_data=f"prev:{page_index}"))
    if page_index < max_page:
        row.append(InlineKeyboardButton("İleri ▶️", callback_data=f"next:{page_index}"))
    row.append(InlineKeyboardButton("❌ İptal", callback_data="cancel"))
    return InlineKeyboardMarkup([row])

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

def get_db_stats(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0
    db = client[db_name_list[0]]
    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0)/(1024*1024),2)
    return movies, series, storage_mb

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=1),1)
    ram = round(virtual_memory().percent,1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free/(1024**3),2)
    free_percent = round((disk.free/disk.total)*100,1)
    uptime_sec = int(time() - bot_start_time)
    h,r = divmod(uptime_sec,3600)
    m,s = divmod(r,60)
    uptime = f"{h}s{m}d{s}s"
    return cpu, ram, free_disk, free_percent, uptime

# ---------------- /istatistik ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        update_daily_traffic()
        daily = load_json(DAILY_FILE)
        total_pages = max(1, len(daily)//PAGE_SIZE + 1)

        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls)>=2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        text = get_page_text(0, movies, series, storage_mb)
        keyboard = get_keyboard(0, total_pages)
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)

# ---------------- Callback ----------------
@Client.on_callback_query()
async def cb(c: Client, q: CallbackQuery):
    daily = load_json(DAILY_FILE)
    total_pages = max(1, len(daily)//PAGE_SIZE + 1)

    if q.data.startswith("prev:"):
        page = int(q.data.split(":")[1])
    elif q.data.startswith("next:"):
        page = int(q.data.split(":")[1])
    elif q.data == "cancel":
        await q.message.delete()
        await q.answer("İstatistik kapatıldı.")
        return
    else:
        await q.answer()
        return

    db_urls = get_db_urls()
    movies = series = storage_mb = 0
    if len(db_urls)>=2:
        movies, series, storage_mb = get_db_stats(db_urls[1])

    text = get_page_text(page, movies, series, storage_mb)
    if page == 0:
        # Sayfa 0 zaten Film/Dizi/Depolama + CPU/RAM/Disk
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        text += f"\n\n┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n┖ RAM → {ram}% | Süre → {uptime}"

    keyboard = get_keyboard(page, total_pages)
    await q.message.edit_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)
    await q.answer()
