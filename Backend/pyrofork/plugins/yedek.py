from pyrogram import filters, Client
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os

@Client.on_message(filters.command('yedek') & filters.private & CustomFilters.owner, group=10)
async def send_backup(client: Client, message: Message):
    """
    /yedek komutu ile mevcut .env dosyasını Telegram'a gönderir.
    Eğer platformdaki env variables kullanılıyorsa, geçici olarak bir .env dosyası oluşturup gönderir.
    """
    try:
        config_path = "Backend/config.env"

        # Eğer fiziksel dosya yoksa, environment variables'dan oluştur
        if not os.path.exists(config_path):
            with open(config_path, "w") as f:
                for key, value in os.environ.items():
                    f.write(f"{key}={value}\n")

        await message.reply_document(
            document=config_path,
            caption="📄 İşte config/env yedeğiniz:",
            quote=True
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"Error in /yedek handler: {e}")
