from pyrogram import filters, Client
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os
from pymongo import MongoClient

@Client.on_message(filters.command('yedek') & filters.private & CustomFilters.owner, group=10)
async def show_db_usage(client: Client, message: Message):
    """
    /yedek komutu ile MongoDB database'in kullandığı depolama miktarını gösterir.
    Config dosyası artık gönderilmez.
    """
    try:
        # MongoDB bağlantı bilgisi (.env veya environment variables)
        mongo_url = os.environ.get("DATABASE_URL")
        if not mongo_url:
            await message.reply_text("⚠️ MongoDB bağlantısı bulunamadı.")
            return

        # MongoDB client oluştur
        mongo_client = MongoClient(mongo_url)
        db_name = mongo_client.get_default_database().name

        # Database istatistiklerini al
        db_stats = mongo_client[db_name].command("dbstats")
        used_storage_mb = db_stats.get("storageSize", 0) / (1024 * 1024)  # byte -> MB

        await message.reply_text(
            f"💾 MongoDB '{db_name}' database depolama kullanımı: {used_storage_mb:.2f} MB",
            quote=True
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"Error in /yedek handler: {e}")
