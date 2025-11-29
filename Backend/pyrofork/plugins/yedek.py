from pyrogram import filters, Client
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os
from pymongo import MongoClient

try:
    from Backend.config import DATABASE as CONFIG_DATABASE
except ImportError:
    CONFIG_DATABASE = None

@Client.on_message(filters.command('yedek') & filters.private & CustomFilters.owner, group=10)
async def show_second_db_info(client: Client, message: Message):
    """
    /yedek komutu ile ikinci database'in:
    - movies koleksiyonundaki kayıt sayısı
    - tv koleksiyonundaki kayıt sayısı
    - toplam kullanılan depolama
    bilgilerini gösterir.
    """
    try:
        # DATABASE URL’lerini al
        databases = CONFIG_DATABASE or os.environ.get("DATABASE") or os.environ.get("DATABASE_URL")
        if not databases:
            await message.reply_text("⚠️ MongoDB bağlantısı config/env değişkenlerinde bulunamadı.")
            return

        # Virgülle ayrılmış URL’leri listele ve sadece ikinciyi al
        mongo_urls = [url.strip() for url in databases.split(",") if url.strip()]
        if len(mongo_urls) < 2:
            await message.reply_text("⚠️ İkinci database URL bulunamadı.")
            return

        url = mongo_urls[1]  # sadece ikinci database
        try:
            mongo_client = MongoClient(url)

            # Database adı URL’de yoksa ilk DB’yi seç
            db_names = mongo_client.list_database_names()
            if not db_names:
                await message.reply_text("⚠️ Database bağlantısı başarılı ama database bulunamadı.")
                return

            db_name = db_names[0]
            db = mongo_client[db_name]

            # Koleksiyon sayıları
            movies_count = db["movies"].count_documents({})
            tv_count = db["tv"].count_documents({})

            # Kullanılan depolama
            db_stats = db.command("dbstats")
            used_storage_mb = db_stats.get("storageSize", 0) / (1024 * 1024)

            # Mesajı hazırla
            msg = (
                f"Filmler: {movies_count:,}\n"
                f"Diziler: {tv_count:,}\n"
                f"Depolama: {used_storage_mb:.2f} MB"
            )

            await message.reply_text(msg, quote=True)

        except Exception as e:
            await message.reply_text(f"⚠️ Database bağlantı hatası: {e}")

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"Error in /yedek handler: {e}")
