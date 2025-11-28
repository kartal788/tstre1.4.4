from pyrogram import filters, Client, enums
from pyrogram.types import Message
from Backend.config import Telegram
from Backend.helper.database import Database

db = Database()  # global db objesi
# bot startup sırasında: await db.connect()

@Client.on_message(filters.command("start") & filters.private, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        addon_url = f"{Telegram.BASE_URL}/deneme/stremio/manifest.json"

        # Database istatistiklerini al
        db_stats_list = await db.get_database_stats()

        if not db_stats_list:
            await message.reply_text("⚠️ Storage DB bulunamadı veya bağlantı kurulamadı!")
            return

        # Tüm storage DB’lerini topla
        movie_count = sum(d['movie_count'] for d in db_stats_list)
        tv_count = sum(d['tv_count'] for d in db_stats_list)
        used_mb = sum(d['storageSize'] for d in db_stats_list) / 1024 / 1024
        total_mb = 500 * len(db_stats_list)  # her DB için 500 MB varsayıldı

        percent = round((used_mb / total_mb) * 100)

        # Telegram mesajı
        text = (
            "Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.\n\n"
            f"<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n"
            f"🎬 <b>Filmler:</b> {movie_count}\n"
            f"📺 <b>Diziler:</b> {tv_count}\n\n"
            f"💾 <b>Depolama:</b>\n"
            f"{used_mb:.1f}MB / {total_mb}MB ({percent}%)"
        )

        await message.reply_text(text, quote=True, parse_mode=enums.ParseMode.HTML)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("Error in /start handler:", e)
