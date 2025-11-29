from pyrogram import Client, filters
from pyrogram.types import Message
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time

# Bot başlama zamanı (genellikle main.py veya init dosyasında)
bot_start_time = time()

# Disk kullanımını kontrol etmek için dizin (root veya download klasörü)
DOWNLOAD_DIR = "/"

@Client.on_message(filters.command("yedek") & filters.private)
async def system_status(client: Client, message: Message):
    try:
        # Sistem bilgilerini al
        cpu = cpu_percent()
        ram = virtual_memory().percent
        free_disk = round(disk_usage(DOWNLOAD_DIR).free / (1024 ** 3), 2)  # GB cinsinden
        uptime_sec = time() - bot_start_time
        uptime = f"{int(uptime_sec // 3600)}h{int((uptime_sec % 3600) // 60)}m"

        # Mesajı hazırla
        text = (
            f"CPU: {cpu}% | FREE: {free_disk}GB\n"
            f"RAM: {ram}% | UPTIME: {uptime}"
        )

        # Cevap gönder
        await message.reply_text(text)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(e)
