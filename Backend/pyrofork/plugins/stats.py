# /Backend/pyrofork/plugins/stats.py

from pymongo import MongoClient
from pyrogram import filters, Client
from pyrogram.types import Message
import config  # senin config.py

# MongoDB bağlantısı
mongo_uri = config.DATABASE
if not mongo_uri:
    raise RuntimeError("DATABASE URI config.DATABASE içinde tanımlı değil")

mongo_client = MongoClient(mongo_uri)
db = mongo_client.get_default_database()

@Client.on_message(filters.command("stats") & filters.private, group=10)
async def stats_handler(client: Client, message: Message):
    try:
        st = db.command("dbStats", scale=1024 * 1024)  # MB cinsinden
        db_name = st.get("db", None)
        collections = st.get("collections", 0)
        objects = st.get("objects", 0)
        data_mb = st.get("dataSize", 0)
        storage_mb = st.get("storageSize", 0)
        index_size_mb = st.get("indexSize", 0)

        text = (
            f"📊 Veritabanı: `{db_name}`\n"
            f"• Koleksiyon sayısı: {collections}\n"
            f"• Döküman sayısı: {objects}\n"
            f"• Veri boyutu: {data_mb:.2f} MB\n"
            f"• Depolama alanı: {storage_mb:.2f} MB\n"
            f"• İndeks boyutu: {index_size_mb:.2f} MB"
        )

        await message.reply_text(text, quote=True)
    except Exception as e:
        await message.reply_text(f"⚠️ MongoDB bilgisi alınamadı: {e}", quote=True)
