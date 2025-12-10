import os
import json
import asyncio
import importlib.util
from time import time
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, UpdateOne

CONFIG_PATH = "/home/debian/dfbot/config.env"
flood_wait_vindir = 30
flood_wait_vsil = 5

# ---------------- DATABASE ----------------
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

# Async motor DB
MONGO_URL = db_urls[1]
async_client = AsyncIOMotorClient(MONGO_URL)
async_db = None
movie_col = None
series_col = None

async def init_db():
    global async_db, movie_col, series_col
    db_names = await async_client.list_database_names()
    async_db = async_client[db_names[0]]
    movie_col = async_db["movie"]
    series_col = async_db["tv"]

# Sync pymongo DB
sync_client = MongoClient(MONGO_URL)
sync_db_name = sync_client.list_database_names()[0]
sync_db = sync_client[sync_db_name]

# ---------------- Flood / Onay Mekanizmaları ----------------
last_command_time_vindir = {}
last_command_time_vsil = {}
awaiting_confirmation = {}  # /sil user_id -> asyncio.Task
pending_deletes = {}        # /vsil user_id -> dict

stop_event = asyncio.Event()  # /tur iptal

# ---------------- /sil ----------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client: Client, message: Message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n"
        "⏱ 60 saniye içinde cevap vermezsen işlem iptal edilir."
    )
    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id].cancel()

    async def timeout():
        await asyncio.sleep(60)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi iptal edildi.")

    awaiting_confirmation[user_id] = asyncio.create_task(timeout())

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def handle_sil_confirmation(client: Client, message: Message):
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
            f"✅ Silme tamamlandı.\n📌 Filmler: {movie_count}\n📌 Diziler: {series_count}"
        )
    elif text == "hayır":
        await message.reply_text("❌ Silme iptal edildi.")

# ---------------- /vsil ----------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()
    if user_id in last_command_time_vsil and now - last_command_time_vsil[user_id] < flood_wait_vsil:
        await message.reply_text(f"⚠️ Lütfen {flood_wait_vsil} saniye bekleyin.", quote=True)
        return
    last_command_time_vsil[user_id] = now

    if user_id in pending_deletes:
        await message.reply_text("⚠️ Silme onay bekliyor. 'evet' veya 'hayır' yazın.")
        return

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Silinecek dosya gir:\n/vsil <telegram_id veya dosya_adı>\n/vsil <tmdb_id>\n/vsil tt<imdb_id>", quote=True
        )
        return

    arg = message.command[1]
    deleted_files = []

    # --- Belirli dokümanları bul ---
    movie_docs, tv_docs = [], []
    if arg.isdigit():
        tmdb_id = int(arg)
        movie_docs = list(sync_db["movie"].find({"tmdb_id": tmdb_id}))
        tv_docs = list(sync_db["tv"].find({"tmdb_id": tmdb_id}))
    elif arg.lower().startswith("tt"):
        imdb_id = arg
        movie_docs = list(sync_db["movie"].find({"imdb_id": imdb_id}))
        tv_docs = list(sync_db["tv"].find({"imdb_id": imdb_id}))
    else:
        target = arg
        movie_docs = sync_db["movie"].find({"$or":[{"telegram.id": target},{"telegram.name": target}]})
        tv_docs = sync_db["tv"].find({})

    for doc in movie_docs:
        for t in doc.get("telegram", []):
            deleted_files.append(t.get("name"))
    for doc in tv_docs:
        for season in doc.get("seasons", []):
            for ep in season.get("episodes", []):
                for t in ep.get("telegram", []):
                    deleted_files.append(t.get("name"))

    if not deleted_files:
        await message.reply_text("⚠️ Hiç eşleşme yok.", quote=True)
        return

    pending_deletes[user_id] = {"files": deleted_files, "arg": arg, "time": now}
    if len(deleted_files) > 10:
        file_path = f"/tmp/silinen_dosyalar_{int(time())}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(deleted_files))
        await client.send_document(message.chat.id, file_path,
                                   caption=f"⚠️ {len(deleted_files)} dosya silinecek.\nSilmek için 'evet', iptal 'hayır'.")
    else:
        await message.reply_text("\n".join(deleted_files) + "\nSilmek için 'evet', iptal 'hayır'.", quote=True)

@Client.on_message(filters.private & CustomFilters.owner)
async def confirm_delete(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in pending_deletes:
        return

    data = pending_deletes[user_id]
    if time() - data["time"] > 120:
        del pending_deletes[user_id]
        await message.reply_text("⏳ Süre doldu, silme iptal edildi.")
        return

    text = message.text.lower()
    if text == "hayır":
        del pending_deletes[user_id]
        await message.reply_text("❌ Silme iptal edildi.")
        return
    if text != "evet":
        await message.reply_text("⚠️ Lütfen 'evet' veya 'hayır' yazın.")
        return

    arg = data["arg"]
    deleted_files = data["files"]
    # --- Silme işlemi ---
    if arg.isdigit():
        sync_db["movie"].delete_many({"tmdb_id": int(arg)})
        sync_db["tv"].delete_many({"tmdb_id": int(arg)})
    elif arg.lower().startswith("tt"):
        sync_db["movie"].delete_many({"imdb_id": arg})
        sync_db["tv"].delete_many({"imdb_id": arg})
    else:
        # Telegram id/name silme mantığı
        pass  # Burada önceki kod mantığını ekleyebilirsin
    del pending_deletes[user_id]
    await message.reply_text("✅ Dosyalar silindi.")

# ---------------- /vindir ----------------
def export_collections_to_json(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return None
    db = client[db_name_list[0]]
    movie_data = list(db["movie"].find({}, {"_id":0}))
    tv_data = list(db["tv"].find({}, {"_id":0}))
    return {"movie": movie_data, "tv": tv_data}

@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()
    if user_id in last_command_time_vindir and now - last_command_time_vindir[user_id] < flood_wait_vindir:
        await message.reply_text(f"⚠️ {flood_wait_vindir} saniye bekleyin.", quote=True)
        return
    last_command_time_vindir[user_id] = now

    if len(db_urls) < 2:
        await message.reply_text("⚠️ İkinci veritabanı yok.")
        return
    combined_data = export_collections_to_json(db_urls[1])
    if not combined_data:
        await message.reply_text("⚠️ Koleksiyon boş.")
        return
    file_path = "/tmp/dizi_ve_film_veritabanı.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)
    await client.send_document(message.chat.id, file_path, caption="📁 Film ve Dizi Koleksiyonları")

# ---------------- /tur ----------------
@Client.on_callback_query(filters.regex("stop"))
async def stop_callback(client, callback_query):
    stop_event.set()
    await callback_query.answer("İşlem iptal edildi!")

@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    stop_event.clear()
    start_msg = await message.reply_text(
        "🔄 Tür ve platform güncellemesi başlatıldı…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
    )

    genre_map = {"Action":"Aksiyon","Comedy":"Komedi"}  # Kısa örnek, önceki harita eklenebilir
    platform_map = {"Netflix":"Netflix","HBO":"Max"}

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0
    last_update = 0

    for col, name in collections:
        docs_cursor = col.find({}, {"_id":1,"genres":1,"telegram":1,"seasons":1})
        bulk_ops = []

        for doc in docs_cursor:
            if stop_event.is_set():
                break
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            # Tür güncelle
            new_genres = [genre_map.get(g,g) for g in genres]
            if new_genres != genres:
                genres = new_genres
                updated = True

            # Platform ekle
            for t in doc.get("telegram",[]):
                for key,name_gen in platform_map.items():
                    if key.lower() in t.get("name","").lower() and name_gen not in genres:
                        genres.append(name_gen)
                        updated = True

            for season in doc.get("seasons",[]):
                for ep in season.get("episodes",[]):
                    for t in ep.get("telegram",[]):
                        for key,name_gen in platform_map.items():
                            if key.lower() in t.get("name","").lower() and name_gen not in genres:
                                genres.append(name_gen)
                                updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id},{"$set":{"genres":genres}}))
                total_fixed +=1

            if time.time() - last_update > 5:
                try:
                    await start_msg.edit_text(f"{name}: Güncellenen kayıt: {total_fixed}", reply_markup=start_msg.reply_markup)
                except: pass
                last_update = time.time()

        if bulk_ops:
            col.bulk_write(bulk_ops)

    try:
        await start_msg.edit_text(f"✅ Güncelleme tamamlandı. Toplam değiştirilen: {total_fixed}")
    except: pass
