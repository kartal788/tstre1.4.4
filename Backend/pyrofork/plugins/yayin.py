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
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")  # Stremio için kullanılabilir

# ---------------- Telegram Bot -----------------
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ---------------- Owner Filter -----------------
def owner_only(_, __, message: Message):
    return message.from_user and message.from_user.id == OWNER_ID

# ---------------- /yayin Komutu -----------------
@bot.on_message(filters.command("yayin") & filters.private & filters.create(owner_only))
async def start(client: Client, message: Message):
    await message.reply_text(
        f"Merhaba! Bana bir dosya gönder, sana Telegram linkini vereyim.\n\n"
        f"Stremio Addon Linki: {BASE_URL}/stremio/manifest.json",
        quote=True
    )

# ---------------- Dosya Mesajı -----------------
@bot.on_message(
    filters.private 
    & (filters.document | filters.video | filters.audio) 
    & filters.create(owner_only)
)
async def file_handler(client: Client, message: Message):
    try:
        file = message.document or message.video or message.audio
        file_info = await client.get_file(file.file_id)

        file_link = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"

        await message.reply_text(
            f"✅ Dosya alındı!\n\n📂 Dosya Adı: {file.file_name}\n🔗 Link: {file_link}",
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )

        print(f"[INFO] Owner dosya gönderdi: {file.file_name}")
        print(f"[INFO] Dosya linki: {file_link}")

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"[ERROR] Dosya link hatası: {e}")

# ---------------- Bot Başlat -----------------
if __name__ == "__main__":
    print("Bot başlatılıyor...")
    bot.run()
