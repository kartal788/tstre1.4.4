import asyncio
import os
import time
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

from pyrogram import Client, filters, enums
from pyrogram.types import Message
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

from Backend.helper.custom_filter import CustomFilters

# ---------------- GLOBAL ----------------
CEVIRME_IPTAL = False
DOWNLOAD_DIR = "/"
bot_start_time = time.time()

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")
db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")
MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]
movie_col = db["movie"]
series_col = db["tv"]

translator = GoogleTranslator(source='en', target='tr')

# ---------------- DYNAMIC CONFIG ----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent_val = psutil.cpu_percent(interval=0.5)
    workers = max(1, min(cpu_count * 2, 16) if cpu_percent_val < 30 else (cpu_count if cpu_percent_val < 60 else 1))
    batch = 80 if ram_percent < 40 else 40 if ram_percent < 60 else 20 if ram_percent < 75 else 10
    return workers, batch

# ---------------- SAFE TRANSLATOR ----------------
def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = translator.translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr

# ---------------- PROGRESS BAR ----------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ---------------- BATCH TRANSLATOR ----------------
def translate_batch_worker(batch):
    CACHE = {}
    results = []
    for doc in batch:
        _id = doc.get("_id")
        upd = {}

        # Film ve bölüm açıklamaları
        desc = doc.get("description")
        if desc and not doc.get("cevrildi", False):
            upd["description"] = translate_text_safe(desc, CACHE)
        
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if not ep.get("cevrildi", False):
                        if "title" in ep and ep["title"]:
                            ep["title"] = translate_text_safe(ep["title"], CACHE)
                            modified = True
                        if "overview" in ep and ep["overview"]:
                            ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                            modified = True
                        ep["cevrildi"] = True
            if modified:
                upd["seasons"] = seasons

        # Film çevrildi flag
        if not doc.get("cevrildi", False):
            upd["cevrildi"] = True

        results.append((_id, upd))
    return results

# ---------------- PARALLEL COLLECTION PROCESSOR ----------------
async def process_collection_parallel(collection, name, message):
    global CEVIRME_IPTAL
    loop = asyncio.get_event_loop()
    ids_cursor = collection.find({"cevrildi": {"$ne": True}}, {"_id":1})
    ids = [d["_id"] for d in ids_cursor]

    total = len(ids)
    if total == 0:
        await message.edit_text(f"{name}: Tüm içerikler çevrilmiş ✅")
        return 0, 0, 0, 0

    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0
    idx = 0
    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        if CEVIRME_IPTAL:
            break
        batch_ids = ids[idx: idx + batch_size]
        batch_docs = list(collection.find({"_id":{"$in":batch_ids}}))
        if not batch_docs:
            break
        try:
            future = loop.run_in_executor(pool, translate_batch_worker, batch_docs)
            results = await future
        except Exception:
            errors += len(batch_docs)
            idx += len(batch_ids)
            await asyncio.sleep(1)
            continue

        for _id, upd in results:
            try:
                if upd:
                    collection.update_one({"_id": _id}, {"$set": upd})
                done += 1
            except Exception:
                errors += 1

        idx += len(batch_ids)

        if time.time() - last_update > 5 or idx >= len(ids):
            try:
                await message.edit_text(
                    f"{name}: {done}/{total}\n{progress_bar(done, total)}\nKalan: {total-done}, Hatalar: {errors}\n\n" +
                    ("⛔ İptal edildi!" if CEVIRME_IPTAL else "")
                )
            except:
                pass
            last_update = time.time()

    pool.shutdown(wait=False)
    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

# ---------------- /CEVIR COMMAND ----------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    global CEVIRME_IPTAL
    CEVIRME_IPTAL = False
    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor.\nİlerleme tek mesajda gösterilecektir.\n\n⛔ /iptal ile durdurabilirsiniz.",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(movie_col, "Filmler", start_msg)
    series_total, series_done, series_errors, series_time = await process_collection_parallel(series_col, "Diziler", start_msg)

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)

    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours)}s {int(minutes)}d {int(seconds)}s"

    summary = (
        "🎉 Türkçe Çeviri Sonuçları\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {eta_str}\n"
    )
    try:
        await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# ---------------- /IPTAL COMMAND ----------------
@Client.on_message(filters.command("iptal") & filters.private & CustomFilters.owner)
async def cevir_iptal(client: Client, message: Message):
    global CEVIRME_IPTAL
    CEVIRME_IPTAL = True
    await message.reply_text("⛔ Çeviri işlemi durduruldu.", quote=True)

# ---------------- /TUR COMMAND ----------------
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    genre_map = {
        "Action": "Aksiyon", "Film-Noir": "Kara Film", "Game-Show": "Oyun Gösterisi", "Short": "Kısa",
        "Sci-Fi": "Bilim Kurgu", "Sport": "Spor", "Adventure": "Macera", "Animation": "Animasyon",
        "Biography": "Biyografi", "Comedy": "Komedi", "Crime": "Suç", "Documentary": "Belgesel",
        "Drama": "Dram", "Family": "Aile", "News": "Haberler", "Fantasy": "Fantastik",
        "History": "Tarih", "Horror": "Korku", "Music": "Müzik", "Musical": "Müzikal",
        "Mystery": "Gizem", "Romance": "Romantik", "Science Fiction": "Bilim Kurgu",
        "TV Movie": "TV Filmi", "Thriller": "Gerilim", "War": "Savaş", "Western": "Vahşi Batı",
    }
    platform_map = {
        "MAX": "Max", "NF": "Netflix", "DSNP": "Disney", "Tv+": "Tv+", "Exxen": "Exxen", "AMZN": "Amazon",
    }

    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0
    for col, name in collections:
        docs_cursor = col.find({}, {"_id":1, "genres":1, "telegram":1, "seasons":1})
        bulk_ops = []
        for doc in docs_cursor:
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                updated = True
            genres = new_genres
            for t in doc.get("telegram", []):
                name_field = t.get("name","").lower()
                for key, genre_name in platform_map.items():
                    if key.lower() in name_field and genre_name not in genres:
                        genres.append(genre_name)
                        updated = True
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name","").lower()
                        for key, genre_name in platform_map.items():
                            if key.lower() in name_field and genre_name not in genres:
                                genres.append(genre_name)
                                updated = True
            if updated:
                bulk_ops.append(UpdateOne({"_id":doc_id}, {"$set":{"genres":genres}}))
                total_fixed += 1
        if bulk_ops:
            col.bulk_write(bulk_ops)
    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ---------------- /ISTATISTIK COMMAND ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        total_movies = movie_col.count_documents({})
        total_series = series_col.count_documents({})
        translated_movies = movie_col.count_documents({"cevrildi": True})
        translated_series = series_col.count_documents({"cevrildi": True})
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage(DOWNLOAD_DIR)
        free_disk = round(disk.free / (1024**3), 2)
        free_percent = round((disk.free/disk.total)*100,1)
        uptime_sec = int(time.time()-bot_start_time)
        h, rem = divmod(uptime_sec,3600)
        m, s = divmod(rem,60)
        uptime = f"{h}s {m}d {s}s"

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies} (Çevrilen: {translated_movies})\n"
            f"┠ Diziler: {total_series} (Çevrilen: {translated_series})\n"
            f"┖ Disk Boş: {free_disk}GB [{free_percent}%]\n"
            f"┟ CPU → {cpu}% | RAM → {ram}%\n"
            f"┖ Süre → {uptime}"
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
