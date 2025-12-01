from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
from datetime import datetime, timedelta
import os
import json
import importlib.util

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
TRAFFIC_FILE = "/tmp/traffic_stats.json"  # Devam ediyor

# ---------------- MongoDB Config ----------------
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
    db = client[db_name_list[0]]  # İlk DB (mevcut sistemle uyumlu)
    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    return movies, series, storage_mb

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=0), 1)
    ram = round(virtual_memory().percent, 1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h}h {m}m {s}s"

    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Ağ Trafiği Destek ----------------
def format_size(size):
    tb = 1024**4
    gb = 1024**3
    try:
        size = float(size)
    except Exception:
        size = 0.0
    if size >= tb:
        return f"{size/tb:.2f}TB"
    elif size >= gb:
        return f"{size/gb:.2f}GB"
    else:
        return f"{size/(1024**2):.2f}MB"

def load_traffic():
    if not os.path.exists(TRAFFIC_FILE):
        return {"daily": {}, "monthly": {}, "last": {}}
    with open(TRAFFIC_FILE, "r") as f:
        try:
            return json.load(f)
        except:
            return {"daily": {}, "monthly": {}, "last": {}}

def save_traffic(data):
    # ensure only json-serializable primitives are saved (ints/floats for totals)
    # convert any legacy dict daily/monthly entries into totals (defensive)
    def normalize_entry(v):
        if isinstance(v, dict):
            # legacy dict might have 'upload' and 'download'
            return int(v.get("upload", 0)) + int(v.get("download", 0))
        try:
            return int(v)
        except:
            try:
                return int(float(v))
            except:
                return 0

    data = data.copy()
    data.setdefault("daily", {})
    data.setdefault("monthly", {})
    data.setdefault("last", {})
    # normalize daily/monthly entries
    data["daily"] = {k: normalize_entry(v) for k, v in data["daily"].items()}
    data["monthly"] = {k: normalize_entry(v) for k, v in data["monthly"].items()}

    with open(TRAFFIC_FILE, "w") as f:
        json.dump(data, f)

def get_network_usage():
    counters = net_io_counters()
    return counters.bytes_sent, counters.bytes_recv

# ---------------- Düzeltilmiş Trafik Hesaplama (Legacy-safe) ----------------
def _to_total_if_legacy(val):
    """
    Eğer val bir dict ise (legacy: {"upload":..,"download":..}) onu toplam int'e çevir.
    Aksi halde int olarak döner (güvenli).
    """
    if isinstance(val, dict):
        return int(val.get("upload", 0)) + int(val.get("download", 0))
    try:
        return int(val)
    except:
        try:
            return int(float(val))
        except:
            return 0

def update_traffic_stats():
    data = load_traffic()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    month = datetime.utcnow().strftime("%Y-%m")

    sent, recv = get_network_usage()
    total = sent + recv

    # Önceki sayaçlar (last), yoksa mevcut değer kullanılır (ilk sefer için)
    last_sent = _to_total_if_legacy(data.get("last", {}).get("sent", sent))
    last_recv = _to_total_if_legacy(data.get("last", {}).get("recv", recv))

    # Günlük fark (sadece pozitif fark alınır)
    diff_sent = max(int(sent) - int(last_sent), 0)
    diff_recv = max(int(recv) - int(last_recv), 0)
    diff_total = diff_sent + diff_recv

    # Günlük toplam önceki tipleri normalize ederek al
    data.setdefault("daily", {})
    existing_today = data["daily"].get(today, 0)
    existing_today_total = _to_total_if_legacy(existing_today)

    # Aylık önceki tipleri normalize ederek al
    data.setdefault("monthly", {})
    existing_month_total = _to_total_if_legacy(data["monthly"].get(month, 0))

    # Günlük ve aylık toplamları güvenli şekilde arttır
    data["daily"][today] = existing_today_total + diff_total
    data["monthly"][month] = existing_month_total + diff_total

    # Sayaç güncelle
    data["last"] = {"sent": int(sent), "recv": int(recv)}

    # Kaydet (normalize et ve yaz)
    save_traffic(data)

    # Son 15 gün raporu (0 olanları atla)
    last_15_days = []
    for i in range(15):
        d = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        val = _to_total_if_legacy(data["daily"].get(d, 0))
        if val > 0:
            last_15_days.append((d, format_size(val)))

    daily_total = data["daily"].get(today, 0)
    month_total = data["monthly"].get(month, 0)
    total_traffic = total

    # Döndürülen değerler string format_size ile dönülüyor (kullanım uyumlu)
    return (
        format_size(diff_sent),      # bugünkü upload farkı (gönderme)
        format_size(diff_recv),      # bugünkü download farkı (alım)
        format_size(daily_total),    # bugünkü toplam (upload+download)
        format_size(month_total),    # bu ay toplam
        format_size(sent),           # sayaç: toplam gönderilen
        format_size(recv),           # sayaç: toplam alınan
        format_size(daily_total),    # duplicate uyumluluk (önceki API ile)
        format_size(month_total),    # duplicate uyumluluk
        format_size(total_traffic),  # makine açıldığından beri toplam sayaç
        last_15_days
    )

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

        (
            diff_up,
            diff_down,
            daily_total,
            month_total,
            total_up,
            total_down,
            _,
            _,
            total_traffic,
            last_15_days
        ) = update_traffic_stats()

        last_text = "\n".join([f"{d}: {t}" for d, t in last_15_days]) if last_15_days else "Veri yok"

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┠ <b>Bugün:</b> {daily_total}\n"
            f"┖ <b>Aylık:</b> {month_total}\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
            f"⌬ <b>Son 15 Gün:</b>\n{last_text}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
