from pyrogram import Client, filters, enums
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
db_name = (await client.list_database_names())[0]  # Async listeleme
db = client[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ------------ Yardımcı Fonksiyonlar ------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

def format_time(seconds):
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

async def delete_collection_progress(collection, name, message):
    total = await collection.count_documents({})
    if total == 0:
        return 0

    done = 0
    start_time = time.time()
    last_update = 0
    BATCH_SIZE = 50

    async for doc in collection.find({}):
        await collection.delete_one({"_id": doc["_id"]})
        done += 1

        # Mesaj güncelleme her BATCH_SIZE veya 5 saniyede bir
        current_time = time.time()
        if done % BATCH_SIZE == 0 or current_time - last_update > 5 or done == total:
            elapsed = current_time - start_time
            rate = done / elapsed if elapsed > 0 else 0
            remaining = total - done
            eta = remaining / rate if rate > 0 else 0

            bar = progress_bar(done, total)
            text = f"{name} siliniyor: {done}/{total}\n{bar}\nKalan: {remaining}\n⏳ ETA: {format_time(eta)}"
            try: await message.edit_text(text)
            except: pass
            last_update = current_time

    return done

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
        start_time = time.time()
        start_msg = await callback_query.message.edit_text("🗑️ Silme işlemi başlatılıyor...")

        movie_deleted = await delete_collection_progress(movie_col, "Filmler", start_msg)
        series_deleted = await delete_collection_progress(series_col, "Diziler", start_msg)

        total_time = format_time(time.time() - start_time)
        await start_msg.edit_text(
            f"✅ Silme işlemi tamamlandı.\n\n"
            f"📌 Filmler silindi: {movie_deleted}\n"
            f"📌 Diziler silindi: {series_deleted}\n"
            f"⏱ Toplam süre: {total_time}"
        )

    elif action == "sil_hayir":
        await callback_query.message.edit_text("❌ Silme işlemi iptal edildi.")

    await callback_query.answer()
