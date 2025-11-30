# yayin.py

import os
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from dotenv import load_dotenv

# ---------------- Load Config ----------------
load_dotenv()  # .env veya config.env dosyasından yükler

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

# ----------------- Telegram Bot -----------------
app_bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ----------------- Owner Filter -----------------
from pyrogram.filters import Filter

class OwnerFilter(Filter):
    async def __call__(self, client, message: Message):
        return message.from_user and message.from_user.id == OWNER_ID

OwnerOnly = OwnerFilter()

# ----------------- /yayin Komutu -----------------
@app_bot.on_message(filters.command("yayin") & filters.private & OwnerOnly)
async def yayin_handler(client: Client, message: Message):
    try:
        # Mesajda dosya var mı kontrol et
        file_attr = message.document or message.video or message.audio
        if not file_attr:
            await message.reply_text("⚠️ Lütfen bir dosya gönderin.", quote=True)
            return

        # file_id ve file_name al
        file_id = file_attr.file_id
        file_name = file_attr.file_name or "video.mkv"

        # Stremio tarzı link üret
        stream_link = f"{BASE_URL}/dl/{file_id}/{file_name}"

        # Owner'a gönder
        await message.reply_text(
            f"📤 İşte dosyanın linki:\n<code>{stream_link}</code>",
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("Hata /yayin:", e)

# ----------------- Bot Başlat -----------------
if __name__ == "__main__":
    app_bot.run()
