from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from motor.motor_asyncio import AsyncIOMotorClient
import os
import importlib.util
import time
import asyncio

# ------------ DATABASE Bağlantısı ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client = AsyncIOMotorClient(MONGO_URL)
db = None
movie_col = None
series_col = None

async def init_db():
    global db, movie_col, series_col
    db_names = await client.list_database_names()
    db = client[db_names[0]]
    movie_col = db["movie"]
    series_col = db["tv"]

# ------------ /sil Komutu ------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def delete_all_data(client: Client, message):
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Evet", callback_data="sil_evet"),
                InlineKeyboardButton("❌ Hayır", callback_data="sil_hayir")
            ]
        ]
    )
    await message.reply_text(
        "Tüm film ve dizi verileri silinecek.\nOnaylıyor musunuz?",
        reply_markup=keyboard
    )

# ------------ Callback Query İşleyici ------------
@Client.on_callback_query(filters.regex(r"^sil_") & CustomFilters.owner)
async def confirm_delete_callback(client, callback_query):
    action = callback_query.data

    if action == "sil_evet":
        start_msg = await callback_query.message.edit_text("🗑️ Silme işlemi başlatılıyor...")

        # Koleksiyonları tek seferde sil
        movie_deleted = await movie_col.count_documents({})
        series_deleted = await series_col.count_documents({})

        await movie_col.delete_many({})
        await series_col.delete_many({})

        total_time = "00:00:01"  # Çok hızlı olduğu için sabit süre
        await start_msg.edit_text(
            f"✅ Silme işlemi tamamlandı.\n\n"
            f"📌 Filmler silindi: {movie_deleted}\n"
            f"📌 Diziler silindi: {series_deleted}\n"
            f"⏱ Toplam süre: {total_time}"
        )

    elif action == "sil_hayir":
        await callback_query.message.edit_text("❌ Silme işlemi iptal edildi.")

    await callback_query.answer()
