import asyncio
import time
import os
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from deep_translator import GoogleTranslator
import psutil
from Backend.helper.custom_filter import CustomFilters

DOWNLOAD_DIR = "/"
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
bot_start_time = time.time()
stop_event = asyncio.Event()

# ================= DATABASE ==========================
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
mongo = MongoClient(MONGO_URL)
db = mongo[mongo.list_database_names()[0]]

movie_col = db["movie"]
series_col = db["tv"]

# ================= UTILS =============================
def translate_safe(text, cache):
    if not text or not text.strip():
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source="en", target="tr").translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

# ================= /IPTAL ===========================
@Client.on_message(filters.command("iptal") & filters.private & CustomFilters.owner)
async def iptal(_, message: Message):
    stop_event.set()
    await message.reply_text("⛔ Çeviri işlemi durduruldu.")

# ================= /CEVIR ===========================
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def cevir(_, message: Message):
    stop_event.clear()
    status = await message.reply_text(
        "🇹🇷 Çeviri başlatıldı...\nDurdurmak için `/iptal` yazın.",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    for col in (movie_col, series_col):
        docs = list(col.find({"cevrildi": {"$ne": True}}))
        cache = {}

        for doc in docs:
            if stop_event.is_set():
                await status.edit_text("⛔ Çeviri iptal edildi.")
                return

            upd = {}

            if doc.get("description"):
                upd["description"] = translate_safe(doc["description"], cache)

            seasons = doc.get("seasons")
            if seasons:
                for s in seasons:
                    for ep in s.get("episodes", []):
                        if ep.get("title"):
                            ep["title"] = translate_safe(ep["title"], cache)
                        if ep.get("overview"):
                            ep["overview"] = translate_safe(ep["overview"], cache)
                upd["seasons"] = seasons

            upd["cevrildi"] = True
            col.update_one({"_id": doc["_id"]}, {"$set": upd})

    await status.edit_text("✅ Çeviri tamamlandı.")

# ================= /TUR ==============================
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(_, message: Message):
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    
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
        "War & Politics": "Savaş ve Politika", "Bilim-Kurgu": "Bilim Kurgu",
        "Aksiyon & Macera": "Aksiyon ve Macera", "Savaş & Politik": "Savaş ve Politika",
        "Bilim Kurgu & Fantazi": "Bilim Kurgu ve Fantazi", "Talk": "Talk-Show"
    }

    platform_genre_map = {
        "MAX": "Max", "Hbomax": "Max", "TABİİ": "Tabii", "NF": "Netflix", "DSNP": "Disney",
        "Tod": "Tod", "Blutv": "Max", "Tv+": "Tv+", "Exxen": "Exxen",
        "Gain": "Gain", "HBO": "Max", "Tabii": "Tabii", "AMZN": "Amazon",
    }

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0

    for col, name in collections:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1, "telegram": 1, "seasons": 1})
        bulk_ops = []

        for doc in docs_cursor:
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            # Tür çevirisi
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                updated = True
            genres = new_genres

            # Platform ekleme
            for t in doc.get("telegram", []):
                name_field = t.get("name", "").lower()
                for key, genre_name in platform_genre_map.items():
                    if key.lower() in name_field and genre_name not in genres:
                        genres.append(genre_name)
                        updated = True

            # Sezonlardaki telegram kontrolü
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name", "").lower()
                        for key, genre_name in platform_genre_map.items():
                            if key.lower() in name_field and genre_name not in genres:
                                genres.append(genre_name)
                                updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": genres}}))
                total_fixed += 1

        if bulk_ops:
            col.bulk_write(bulk_ops)

    await start_msg.edit_text(
        f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}"
    )

# ================= /CEVIRKALDIR =====================
@Client.on_message(filters.command("cevirkaldir") & filters.private & CustomFilters.owner)
async def cevirkaldir(_, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları kaldırılıyor...")
    
    total_updated = 0
    for col in (movie_col, series_col):
        docs_cursor = col.find({"cevrildi": True}, {"_id": 1})
        bulk_ops = [UpdateOne({"_id": doc["_id"]}, {"$unset": {"cevrildi": ""}}) for doc in docs_cursor]
        if bulk_ops:
            result = col.bulk_write(bulk_ops)
            total_updated += result.modified_count

    await status.edit_text(f"✅ 'cevrildi' alanları kaldırıldı.\nToplam güncellenen kayıt: {total_updated}")

# ================= /CEVIREKLE =====================
@Client.on_message(filters.command("cevirekle") & filters.private & CustomFilters.owner)
async def cevirekle(_, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları ekleniyor...")
    
    total_updated = 0
    for col in (movie_col, series_col):
        docs_cursor = col.find({"cevrildi": {"$ne": True}}, {"_id": 1})
        bulk_ops = [UpdateOne({"_id": doc["_id"]}, {"$set": {"cevrildi": True}}) for doc in docs_cursor]
        if bulk_ops:
            result = col.bulk_write(bulk_ops)
            total_updated += result.modified_count

    await status.edit_text(f"✅ 'cevrildi' alanları eklendi.\nToplam güncellenen kayıt: {total_updated}")

# ================= /ISTATISTIK ======================
def get_db_urls():
    return [u.strip() for u in os.getenv("DATABASE", "").split(",") if u.strip()]

def get_db_stats_and_genres(url):
    client = MongoClient(url)
    db = client[client.list_database_names()[0]]

    total_movies = db["movie"].count_documents({})
    total_series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    max_storage_mb = 512
    storage_percent = round((storage_mb / max_storage_mb) * 100, 1)

    genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})

    for d in db["movie"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[d["_id"]]["film"] = d["count"]

    for d in db["tv"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[d["_id"]]["dizi"] = d["count"]

    return total_movies, total_series, storage_mb, storage_percent, genre_stats

def get_system_status():
    cpu = round(psutil.cpu_percent(interval=1), 1)
    ram = round(psutil.virtual_memory().percent, 1)
    disk = psutil.disk_usage(DOWNLOAD_DIR)

    free_disk = round(disk.free / (1024 ** 3), 2)
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}sa {m}dk {s}sn"

    return cpu, ram, free_disk, free_percent, uptime

@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def istatistik(_, message: Message):
    urls = get_db_urls()
    total_movies, total_series, storage_mb, storage_percent, genre_stats = get_db_stats_and_genres(urls[1])
    cpu, ram, free_disk, free_percent, uptime = get_system_status()

    genre_text = "\n".join(
        f"{g:<14} | Film: {c['film']:<4} | Dizi: {c['dizi']:<4}"
        for g, c in sorted(genre_stats.items())
    )

    text = (
        f"⌬ <b>İstatistik</b>\n\n"
        f"┠ Filmler : {total_movies}\n"
        f"┠ Diziler : {total_series}\n"
        f"┖ Depolama: {storage_mb} MB (%{storage_percent})\n\n"
        f"<b>Tür Dağılımı</b>\n<pre>{genre_text}</pre>\n\n"
        f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
        f"┖ RAM → {ram}% | Süre → {uptime}"
    )

    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
