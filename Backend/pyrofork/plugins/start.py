from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.config import Telegram

def colored_bar(percent: int, size: int = 20):
    """
    Renkli progress bar üretir:
    %0–60 → 🟩
    %60–80 → 🟨
    %80–100 → 🟥
    """
    green_limit = int(size * 0.6)
    yellow_limit = int(size * 0.8)

    bar = ""
    for i in range(size):
        if i < green_limit:
            bar += "🟩"
        elif i < yellow_limit:
            bar += "🟨"
        else:
            bar += "🟥"
    return bar

@Client.on_message(filters.command("start") & filters.private)
async def send_start_message(client: Client, message: Message):
    try:
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # Örnek depolama verileri — gerçek değerleri buraya koyabilirsin
        used_mb = 320
        total_mb = 500
        percent = round((used_mb / total_mb) * 100)

        # Renkli bar oluştur
        bar = colored_bar(percent)

        text = (
            "Eklentiyi Stremio’ya eklemek için aşağıdaki adresi kopyalayın:\n\n"
            f"<b>Eklenti adresiniz:</b>\n<code>{addon_url}</code>\n\n"
            "<b>💾 Depolama Kullanımı</b>\n"
            f"{used_mb}MB / {total_mb}MB ({percent}%)\n\n"
            f"{bar}"
        )

        await message.reply_text(
            text,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata oluştu: {e}")
        print("Start Hata:", e)
