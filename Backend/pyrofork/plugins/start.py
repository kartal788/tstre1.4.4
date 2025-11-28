from pyrogram import Client, filters, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram
from Backend.database import db_stat  # Senin projendeki gerçek db_stat objesi

def hex_bar(percent: int, size: int = 12):
    """
    Altıgen progress bar üretir:
    Dolu: ⬢
    Boş: ⬡
    """
    filled = int((percent / 100) * size)
    empty = size - filled
    return "⬢" * filled + "⬡" * empty

@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        # Stremio eklenti URL
        addon_url = f"{Telegram.BASE_URL}/deneme/stremio/manifest.json"

        # Film ve Dizi sayısı
        movie_count = f"{db_stat.movie_count:,}"
        tv_count = f"{db_stat.tv_count:,}"

        # Depolama hesaplaması (MB)
        used_mb = float(f"{db_stat.storageSize / 1024 / 1024:.1f}")
        total_mb = 500  # MB olarak limit
        percent = round((used_mb / total_mb) * 100)

        # Altıgen bar
        bar = hex_bar(percent)

        # Telegram mesajı
        text = (
            "Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.\n\n"
            f"<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n"
            f"🎬 <b>Filmler:</b> {movie_count}\n"
            f"📺 <b>Diziler:</b> {tv_count}\n\n"
            f"💾 <b>Depolama Kullanımı:</b>\n"
            f"{used_mb}MB / {total_mb}MB ({percent}%)\n"
            f"[{bar}]"
        )

        await message.reply_text(
            text,
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata oluştu: {e}")
        print(f"Error in /start handler: {e}")
