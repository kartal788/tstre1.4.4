from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from motor.motor_asyncio import AsyncIOMotorClient
import os
import importlib.util
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

# ------------ /sil Komutu (Onay Kutulu) ------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def confirm_delete(client: Client, message):
    keyboard = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ Evet, sil", callback_data="confirm_delete"),
            InlineKeyboardButton("❌ Hayır", callback_data="cancel_delete")
        ]]
    )
    await message.reply_text(
        "⚠️ Tüm veriler silinecek! Onaylıyor musunuz?",
        reply_markup=keyboard
    )

# ------------ Callback Query (Onay ve İptal) ------------
@Client.on_callback_query()
async def handle_delete_buttons(client, callback_query):
    data = callback_query.data

    if data == "confirm_delete":
        await callback_query.answer("Siliniyor...")  # Kullanıcıya tepki gösterir
        await init_db()
        movie_count = await movie_col.count_documents({})
        series_count = await series_col.count_documents({})
        await movie_col.delete_many({})
        await series_col.delete_many({})
        await callback_query.message.edit_text(
            f"✅ Silme işlemi tamamlandı.\n\n"
            f"📌 Filmler silindi: {movie_count}\n"
            f"📌 Diziler silindi: {series_count}"
        )

    elif data == "cancel_delete":
        await callback_query.answer("İşlem iptal edildi.", show_alert=True)
        await callback_query.message.edit_text("❌ Silme işlemi iptal edildi.")
