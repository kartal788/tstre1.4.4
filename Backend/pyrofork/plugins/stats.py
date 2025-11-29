from pyrogram import filters, Client, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import psutil

@Client.on_message(filters.command('stats') & filters.private & CustomFilters.owner, group=10)
async def stats_system_info(client: Client, message: Message):
    """
    /stats komutu: CPU ve RAM bilgilerini gösterir
    """
    # CPU yüzdesi
    cpu_percent = psutil.cpu_percent(interval=1)
    # RAM kullanımı
    memory = psutil.virtual_memory()
    ram_total = round(memory.total / (1024 ** 3), 2)  # GB
    ram_used = round(memory.used / (1024 ** 3), 2)    # GB
    ram_percent = memory.percent

    # Mesaj metni
    stats_text = (
        f"Selam! Stats komutu alındı 😊\n\n"
        f"💻 CPU Kullanımı: {cpu_percent}%\n"
        f"🖥️ RAM Kullanımı: {ram_used}GB / {ram_total}GB ({ram_percent}%)"
    )

    await message.reply_text(stats_text, quote=True, parse_mode=enums.ParseMode.HTML)
