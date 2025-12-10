import os
import importlib.util
import asyncio
import json
from time import time
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from Backend.helper.custom_filter import CustomFilters

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

MONGO_URL = db_urls[1]
client_db = AsyncIOMotorClient(MONGO_URL)
db = client_db[await client_db.list_database_names()][0]

movie_col = db["movie"]
series_col = db["tv"]

# ---------------- GLOBALS ----------------
awaiting_confirmation = {}  # /sil için
pending_deletes = {}        # /vsil için
last_command_time = {}      # flood kontrolü
flood_wait = 5
confirmation_wait = 120
stop_event = asyncio.Event()  # /tur için

# ---------------- /sil ----------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client: Client, message: Message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n"
        "⏱ 60 saniye içinde cevap vermezsen işlem otomatik iptal edilir."
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
        movie_count = await movie_col.count_documents({})
        series_count = await series_col.count_documents({})
        await movie_col.delete_many({})
        await series_col.delete_many({})
        await message.reply_text(
            f"✅ Silme tamamlandı.\n📌 Filmler silindi: {movie_count}\n📌 Diziler silindi: {series_count}"
        )
    elif text == "hayır":
        await message.reply_text("❌ Silme iptal edildi.")

# ---------------- /vindir ----------------
def serialize_doc(doc):
    """_id hariç JSON için"""
    new_doc = {k: v for k, v in doc.items() if k != "_id"}
    return new_doc

@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.")
        return
    last_command_time[user_id] = now

    movie_data = [serialize_doc(doc) async for doc in movie_col.find({})]
    tv_data = [serialize_doc(doc) async for doc in series_col.find({})]
    combined_data = {"movie": movie_data, "tv": tv_data}

    if not combined_data["movie"] and not combined_data["tv"]:
        await message.reply_text("⚠️ Koleksiyonlar boş.")
        return

    file_path = f"/tmp/dizi_ve_film_veritabanı_{int(time())}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

    await client.send_document(chat_id=message.chat.id, document=file_path,
                               caption="📁 Film ve Dizi Koleksiyonları")

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
        await message.reply_text(
            "⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n"
            "/vsil <telegram_id veya dosya_adı>\n"
            "/vsil <tmdb_id>\n"
            "/vsil tt<imdb_id>"
        )
        return

    arg = message.command[1]
    deleted_files = []

    # --- Async Mongo işlemleri ---
    if arg.isdigit():
        tmdb_id = int(arg)
        async for doc in movie_col.find({"tmdb_id": tmdb_id}):
            deleted_files += [t.get("name") for t in doc.get("telegram", [])]
        async for doc in series_col.find({"tmdb_id": tmdb_id}):
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    deleted_files += [t.get("name") for t in ep.get("telegram", [])]
    elif arg.lower().startswith("tt"):
        imdb_id = arg
        async for doc in movie_col.find({"imdb_id": imdb_id}):
            deleted_files += [t.get("name") for t in doc.get("telegram", [])]
        async for doc in series_col.find({"imdb_id": imdb_id}):
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    deleted_files += [t.get("name") for t in ep.get("telegram", [])]
    else:
        target = arg
        async for doc in movie_col.find({"$or": [{"telegram.id": target}, {"telegram.name": target}]}):
            telegram_list = doc.get("telegram", [])
            match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
            deleted_files += [t.get("name") for t in match]
        async for doc in series_col.find({}):
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    telegram_list = ep.get("telegram", [])
                    match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                    deleted_files += [t.get("name") for t in match]

    if not deleted_files:
        await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.")
        return

    pending_deletes[user_id] = {"files": deleted_files, "arg": arg, "time": now}

    if len(deleted_files) > 10:
        file_path = f"/tmp/silinen_dosyalar_{int(time())}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(deleted_files))
        await client.send_document(chat_id=message.chat.id, document=file_path,
                                   caption=f"⚠️ {len(deleted_files)} dosya silinecek.\nSilmek için 'evet', iptal için 'hayır' yazın.")
    else:
        await message.reply_text(
            f"⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n"
            + "\n".join(deleted_files)
            + "\n\nSilmek için **evet**, iptal için **hayır** yazın.",
        )

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def confirm_delete(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in pending_deletes:
        return

    data = pending_deletes[user_id]
    text = message.text.lower()
    if text == "hayır":
        del pending_deletes[user_id]
        await message.reply_text("❌ Silme iptal edildi.")
        return
    if text != "evet":
        await message.reply_text("⚠️ Lütfen 'evet' veya 'hayır' yazın.")
        return

    arg = data["arg"]

    # --- Async delete işlemi ---
    if arg.isdigit():
        tmdb_id = int(arg)
        await movie_col.delete_many({"tmdb_id": tmdb_id})
        await series_col.delete_many({"tmdb_id": tmdb_id})
    elif arg.lower().startswith("tt"):
        imdb_id = arg
        await movie_col.delete_many({"imdb_id": imdb_id})
        await series_col.delete_many({"imdb_id": imdb_id})
    else:
        target = arg
        async for doc in movie_col.find({"$or":[{"telegram.id": target},{"telegram.name": target}]}):
            telegram_list = doc.get("telegram", [])
            new_telegram = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
            if not new_telegram:
                await movie_col.delete_one({"_id": doc["_id"]})
            else:
                await movie_col.replace_one({"_id": doc["_id"]}, {**doc, "telegram": new_telegram})
        async for doc in series_col.find({}):
            modified = False
            seasons_to_remove = []
            for season in doc.get("seasons", []):
                episodes_to_remove = []
                for ep in season.get("episodes", []):
                    telegram_list = ep.get("telegram", [])
                    new_telegram = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
                    if new_telegram != telegram_list:
                        if new_telegram:
                            ep["telegram"] = new_telegram
                        else:
                            episodes_to_remove.append(ep)
                        modified = True
                for ep in episodes_to_remove:
                    season["episodes"].remove(ep)
                if not season["episodes"]:
                    seasons_to_remove.append(season)
            for s in seasons_to_remove:
                doc["seasons"].remove(s)
            if modified:
                await series_col.replace_one({"_id": doc["_id"]}, doc)

    del pending_deletes[user_id]
    await message.reply_text("✅ Dosyalar başarıyla silindi.")

# ---------------- /tur ----------------
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    stop_event.clear()
    start_msg = await message.reply_text(
        "🔄 Tür ve platform güncellemesi başlatıldı…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
    )

    genre_map = {
        "Action": "Aksiyon", "Film-Noir": "Kara Film", "Game-Show": "Oyun Gösterisi", "Short": "Kısa",
        "Sci-Fi": "Bilim Kurgu", "Sport": "Spor", "Adventure": "Macera", "Animation": "Animasyon",
        "Biography": "Biyografi", "Comedy": "Komedi", "Crime": "Suç", "Documentary": "Belgesel",
        "Drama": "Dram", "Family": "Aile", "News": "Haberler", "Fantasy": "Fantastik",
        "History": "Tarih", "Horror": "Korku", "Music": "Müzik", "Musical": "Müzikal",
        "Mystery": "Gizem", "Romance": "Romantik", "Science Fiction": "Bilim Kurgu",
        "TV Movie": "TV Filmi", "Thriller": "Gerilim", "War": "Savaş", "Western": "Vahşi Batı",
        "Action & Adventure": "Aksiyon ve Macera", "Kids": "Çocuklar", "Reality": "Gerçeklik",
        "Reality-TV": "Gerçeklik", "Sci-Fi & Fantasy": "Bilim Kurgu ve Fantazi", "Soap": "Pembe Dizi",
    }
    platform_genre_map = {
        "MAX": "Max", "Hbomax": "Max", "TABİİ": "Tabii", "NF": "Netflix", "DSNP": "Disney",
        "Tod": "Tod", "Blutv": "Max", "Tv+": "Tv+", "Exxen": "Exxen", "Gain": "Gain", "HBO": "Max",
        "Tabii": "Tabii", "AMZN": "Amazon",
    }

    total_fixed = 0
    last_update = 0

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]

    for col, name in collections:
        async for doc in col.find({}):
            if stop_event.is_set():
                break
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            # Tür güncelle
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                genres = new_genres
                updated = True

            # Platform ekle
            for t in doc.get("telegram", []):
                name_field = t.get("name", "").lower()
                for key, val in platform_genre_map.items():
                    if key.lower() in name_field and val not in genres:
                        genres.append(val)
                        updated = True
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name", "").lower()
                        for key, val in platform_genre_map.items():
                            if key.lower() in name_field and val not in genres:
                                genres.append(val)
                                updated = True

            if updated:
                await col.update_one({"_id": doc_id}, {"$set": {"genres": genres}})
                total_fixed += 1

            if time() - last_update > 5:
                try:
                    await start_msg.edit_text(
                        f"{name}: Güncellenen kayıtlar: {total_fixed}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
                    )
                except: pass
                last_update = time()

    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}",
                              parse_mode=enums.ParseMode.MARKDOWN)
