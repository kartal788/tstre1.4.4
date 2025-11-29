
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time

bot_start_time = time()  # Bot başlama zamanı
DOWNLOAD_DIR = "/"       # Disk kontrolü için dizin

# ---------------- Sistem Durumu Fonksiyonu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=1), 1)
    ram = round(virtual_memory().percent, 1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    disk_percent = round((disk.free / disk.total) * 100, 1)
    
    uptime_sec = int(time() - bot_start_time)
    hours, remainder = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime = f"{hours}h{minutes}m{seconds}s"
    
    return cpu, ram, free_disk, disk_percent, uptime

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command('istatistik') & filters.private & CustomFilters.owner)
async def send_stats(client: Client, message: Message):
    try:
        # Sabit örnek veriler
        movies_count = 0
        series_count = 1
        storage_mb = 0.02
        
        cpu, ram, free_disk, disk_percent, uptime = get_system_status()
        
        stats_text = (
            f"⌬ İstatistik\n"
            f"│\n"
            f"┠ Filmler:         {movies_count}\n"
            f"┠ Diziler:         {series_count}\n"
            f"┖Depolama: {storage_mb} MB\n\n"
            f"┟CPU {cpu}% | Boş Alan {free_disk}GB [{disk_percent}%]\n"
            f"┖RAM {ram}% | Süre {uptime}"
        )
        
        await message.reply_text(
            stats_text,
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )
        
    except Exception as e:
        await message.reply_text(f"⚠️ Bir hata oluştu: {e}")
        print(f"Error in /istatistik handler: {e}")
