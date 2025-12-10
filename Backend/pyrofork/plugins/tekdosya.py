import os
import importlib.util
import asyncio
import json
from time import time
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from motor.motor_asyncio import AsyncIOMotorClient

# ---------------- CONFIG ----------------
CONFIG_PATH = "/home/debian/dfbot/config.env"
flood_wait = 5
confirmation_wait = 120

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
client_db = AsyncIOMotorClient(MONGO_URL)

db = None
movie_col = None
series_col = None

async def init_db():
    global db, movie_col, series_col
    db_names = await client_db.list_database_names()
    db = client_db[db_names[0]]
    movie_col = db["movie"]
    series_col = db["tv"]

# ---------------- STATE ----------------
last_command_time = {}
awaiting_confirmation = {}  # /sil için
pending_deletes = {}        # /vsil için
stop_event = asyncio.Event() # /tur için

# ---------------- /sil ----------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client: Client, message: Message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n"
        f"⏱ {confirmation_wait} saniye içinde cevap vermezsen işlem iptal edilir."
    )

    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id].cancel()

    async def timeout():
        await asyncio.sleep(confirmation_wait)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi iptal edildi.")

    task = asyncio.create_task(timeout())
    awaiting_confirmation[user_id] = task

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def handle_confirmation(client: Client, message: Message):
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

# ---------------- /vsil ----------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.")
        return
    last_command_time[user_id] = now

    if user_id in pending_deletes:
        await message.reply_text("⚠️ Bir silme işlemi zaten onay bekliyor.")
        return

    if len(message.command) < 2:
        await message.reply_text("⚠️ /vsil <id veya isim>")
        return

    arg = message.command[1]
    await init_db()
    deleted_files = []

    # --- Dosya bul ---
    if arg.isdigit():
        tmdb_id = int(arg)
        movies = await movie_col.find({"tmdb_id": tmdb_id}).to_list(1000)
        series = await series_col.find({"tmdb_id": tmdb_id}).to_list(1000)
    elif arg.lower().startswith("tt"):
        imdb_id = arg
        movies = await movie_col.find({"imdb_id": imdb_id}).to_list(1000)
        series = await series_col.find({"imdb_id": imdb_id}).to_list(1000)
    else:
        movies = await movie_col.find({"$or":[{"telegram.id": arg},{"telegram.name": arg}]}).to_list(1000)
        series = await series_col.find({}).to_list(1000)

    for doc in movies:
        deleted_files += [t.get("name") for t in doc.get("telegram", [])]
    for doc in series:
        for season in doc.get("seasons", []):
            for ep in season.get("episodes", []):
                deleted_files += [t.get("name") for t in ep.get("telegram", [])]

    if not deleted_files:
        await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.")
        return

    pending_deletes[user_id] = {"files": deleted_files, "arg": arg, "time": now}

    if len(deleted_files) > 10:
        file_path = f"/tmp/silinen_dosyalar_{int(time())}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(deleted_files))
        await client.send_document(message.chat.id, file_path, caption=f"⚠️ {len(deleted_files)} dosya silinecek. 'evet' veya 'hayır'")
    else:
        await message.reply_text("\n".join(deleted_files))

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def confirm_delete(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in pending_deletes:
        return
    data = pending_deletes[user_id]
    if time() - data["time"] > confirmation_wait:
        del pending_deletes[user_id]
        await message.reply_text("⏳ Süre doldu, silme iptal edildi.")
        return

    if message.text.lower() == "hayır":
        del pending_deletes[user_id]
        await message.reply_text("❌ Silme iptal edildi.")
        return
    if message.text.lower() != "evet":
        await message.reply_text("⚠️ 'evet' veya 'hayır' yazın.")
        return

    # --- Sil ---
    await init_db()
    arg = data["arg"]
    deleted_files = data["files"]

    if arg.isdigit():
        await movie_col.delete_many({"tmdb_id": int(arg)})
        await series_col.delete_many({"tmdb_id": int(arg)})
    elif arg.lower().startswith("tt"):
        await movie_col.delete_many({"imdb_id": arg})
        await series_col.delete_many({"imdb_id": arg})
    else:
        # Daha karmaşık silme mantığı buraya eklenebilir
        pass

    del pending_deletes[user_id]
    await message.reply_text("✅ Dosyalar başarıyla silindi.")

# ---------------- /vindir ----------------
@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    await init_db()
    data = {"movie": await movie_col.find({}, {"_id": 0}).to_list(10000),
            "tv": await series_col.find({}, {"_id": 0}).to_list(10000)}
    file_path = "/tmp/dizi_ve_film_veritabanı.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    await client.send_document(message.chat.id, file_path, caption="📁 Film ve Dizi Koleksiyonları")

# ---------------- /tur ----------------
@Client.on_callback_query(filters.regex("stop"))
async def stop_callback(client, callback_query):
    stop_event.set()
    await callback_query.answer("İşlem iptal edildi!")

@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    await init_db()
    stop_event.clear()
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    genre_map = {"Action": "Aksiyon", "Comedy": "Komedi"}  # Örnek
    platform_map = {"Netflix": "Netflix"}                 # Örnek

    total_fixed = 0
    async for doc in movie_col.find({}):
        genres = doc.get("genres", [])
        updated = False
        new_genres = [genre_map.get(g, g) for g in genres]
        if new_genres != genres:
            genres = new_genres
            updated = True
        if updated:
            await movie_col.update_one({"_id": doc["_id"]}, {"$set": {"genres": genres}})
            total_fixed += 1

    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")
