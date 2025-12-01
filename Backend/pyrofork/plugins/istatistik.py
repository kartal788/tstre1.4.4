from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import psutil
from time import time
from datetime import datetime
import os
import json

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
TRAFFIC_FILE = "/tmp/traffic_stats.json"  # Konteyner içinde kalıcı olmayan bir dosya

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(psutil.cpu_percent(interval=1), 1)
    ram = round(psutil.virtual_memory().percent, 1)
    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)
    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h} saat {m} dakika {s} saniye"
    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Trafik Verisi ----------------
def format_size(bytes_value):
    gb = 1024 ** 3
    mb = 1024 ** 2
    if bytes_value >= gb:
        return f"{bytes_value / gb:.2f} GB"
    else:
        return f"{bytes_value / mb:.2f} MB"

def load_traffic():
    if os.path.exists(TRAFFIC_FILE):
        with open(TRAFFIC_FILE, "r") as f:
            return json.load(f)
    return {"month": datetime.now().strftime("%Y-%m"), "upload": 0, "download": 0}

def save_traffic(data):
    with open(TRAFFIC_FILE, "w") as f:
        json.dump(data, f)

def update_monthly_traffic():
    net = psutil.net_io_counters()
    upload_bytes, download_bytes = net.bytes_sent, net.bytes_recv

    stats = load_traffic()
    current_month = datetime.now().strftime("%Y-%m")

    # Ay değiştiyse sıfırla
    if stats["month"] != current_month:
        stats = {"month": current_month, "upload": 0, "download": 0}

    # Farkı ekle
    stats["upload"] += upload_bytes
    stats["download"] += download_bytes

    save_traffic(stats)
    return format_size(stats["upload"]), format_size(stats["download"])

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        month_up, month_down = update_monthly_traffic()

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
            f"┠ <b>Bu Ay Upload:</b> {month_up}\n"
            f"┖ <b>Bu Ay Download:</b> {month_down}"
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
