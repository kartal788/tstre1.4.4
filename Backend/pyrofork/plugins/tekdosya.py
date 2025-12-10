import os
import importlib.util
import asyncio
import json
import time
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, UpdateOne

# ---------------- CONFIG ----------------
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

# ---------------- MOTOR (Async) ----------------
MONGO_URL = db_urls[1]
client_motor = AsyncIOMotorClient(MONGO_URL)
db_motor = None
movie_col = None
series_col = None

async def init_db():
    global db_motor, movie_col, series_col
    db_names = await client_motor.list_database_names()
    db_motor = client_motor[db_names[0]]
    movie_col = db_motor["movie"]
    series_col = db_motor["tv"]

# ---------------- VSIL (Sync) ----------------
client_sync = MongoClient(MONGO_URL)
db_sync = client_sync[client_sync.list_database_names()[0]]

# ---------------- ONAY / SIL ----------------
awaiting_confirmation = {}  # user_id -> asyncio.Task

@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client, message: Message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal için **Hayır** yazın.\n"
        "⏱ 60 saniye içinde cevap vermezsen işlem otomatik iptal edilir."
    )
    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id].cancel()

    async def timeout():
        await asyncio.sleep(60)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi iptal edildi.")

    task = asyncio.create_task(timeout())
    awaiting_confirmation[user_id] = task

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def handle_confirmation(client, message: Message):
    user_id = message.from_user.id
    if user_id not in awaiting_confirmation:
        return
    text = message.text.strip().lower()
    awaiting_confirmation[user_id].cancel()
    awaiting_confirmation.pop(user_id, None)

    if text == "evet":
        await message.reply_text("🗑️ Silme işlemi başlatılıyor...")
        await init_db()
        movie_count = await movie_col.count_documents({})
        series_count = await series_col.count_documents({})
        await movie_col.delete_many({})
        await series_col.delete_many({})
        await message.reply_text(
            f"✅ Silme tamamlandı.\n📌 Filmler silindi: {movie_count}\n📌 Diziler silindi: {series_count}"
        )
    elif text == "hayır":
        await message.reply_text("❌ Silme iptal edildi.")

# ---------------- VTINDIR ----------------
flood_wait = 30
last_command_time = {}

@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if not db_urls or len(db_urls) < 2:
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
        return

    movie_data = list(db_sync["movie"].find({}, {"_id": 0}))
    tv_data = list(db_sync["tv"].find({}, {"_id": 0}))
    combined_data = {"movie": movie_data, "tv": tv_data}

    file_path = "/tmp/dizi_ve_film_veritabani.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

    await client.send_document(message.chat.id, file_path, caption="📁 Film ve Dizi Koleksiyonları")

# ---------------- TUR / PLATFORM ----------------
stop_event = asyncio.Event()

@Client.on_callback_query(filters.regex("stop"))
async def stop_callback(client, callback_query):
    stop_event.set()
    await callback_query.answer("İşlem iptal edildi!")

@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    await init_db()
    stop_event.clear()
    start_msg = await message.reply_text(
        "🔄 Tür ve platform güncellemesi başlatıldı…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    genre_map = {
        "Action": "Aksiyon", "Comedy": "Komedi", "Drama": "Dram", "Horror": "Korku", "Romance": "Romantik"
    }
    platform_map = {"Netflix": "Netflix", "Max": "Max"}

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0
    last_update = 0

    for col, name in collections:
        cursor = col.find({})
        bulk_ops = []
        async for doc in cursor:
            if stop_event.is_set():
                break
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            # Tür
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                genres = new_genres
                updated = True

            # Platform
            for t in doc.get("telegram", []):
                for key, p in platform_map.items():
                    if key.lower() in t.get("name", "").lower() and p not in genres:
                        genres.append(p)
                        updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": genres}}))
                total_fixed += 1

            if time.time() - last_update > 5:
                try:
                    await start_msg.edit_text(f"{name}: Güncellenen kayıtlar: {total_fixed}")
                except: pass
                last_update = time.time()

        if bulk_ops:
            col.bulk_write(bulk_ops)

    try:
        await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")
    except: pass

# ---------------- ISTATISTIK ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def istatistik(client: Client, message: Message):
    await init_db()
    movie_count = await movie_col.count_documents({})
    series_count = await series_col.count_documents({})

    total_telegram_movie = 0
    total_telegram_series = 0

    async for doc in movie_col.find({}):
        total_telegram_movie += len(doc.get("telegram", []))

    async for doc in series_col.find({}):
        for season in doc.get("seasons", []):
            for ep in season.get("episodes", []):
                total_telegram_series += len(ep.get("telegram", []))

    await message.reply_text(
        f"📊 **İstatistikler:**\n\n"
        f"🎬 Filmler: {movie_count} (Telegram dosyaları: {total_telegram_movie})\n"
        f"📺 Diziler: {series_count} (Telegram dosyaları: {total_telegram_series})",
        parse_mode=enums.ParseMode.MARKDOWN
    )
