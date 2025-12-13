import asyncio
import time
import math
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

from pyrogram import Client, filters, enums
from pyrogram.types import Message
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

from Backend.helper.custom_filter import CustomFilters  # Owner filtresi için

# ----------------- DATABASE -----------------
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
bot_start_time = time.time()
DOWNLOAD_DIR = "/"

# ----------------- Yardımcı Fonksiyonlar -----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent_val = psutil.cpu_percent(interval=0.5)

    if cpu_percent_val < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent_val < 60:
        workers = max(1, cpu_count)
    else:
        workers = 1

    if ram_percent < 40:
        batch = 80
    elif ram_percent < 60:
        batch = 40
    elif ram_percent < 75:
        batch = 20
    else:
        batch = 10

    return workers, batch

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

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ----------------- /tur Komutu -----------------
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

@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0
    last_update = 0

    for col, name in collections:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1, "telegram": 1, "seasons": 1})
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
                name_field = t.get("name", "").lower()
                for key, genre_name in platform_genre_map.items():
                    if key.lower() in name_field and genre_name not in genres:
                        genres.append(genre_name)
                        updated = True

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

            if time.time() - last_update > 5:
                try:
                    await start_msg.edit_text(f"{name}: Güncellenen kayıtlar: {total_fixed}")
                except:
                    pass
                last_update = time.time()

        if bulk_ops:
            col.bulk_write(bulk_ops)

    try:
        await start_msg.edit_text(
            f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}",
            parse_mode=enums.ParseMode.MARKDOWN
        )
    except:
        pass

# ----------------- /cevir Komutu -----------------
def translate_batch_worker(batch):
    CACHE = {}
    results = []

    for doc in batch:
        _id = doc.get("_id")
        upd = {}
        already_translated = doc.get("cevrildi", False)
        is_tv = doc.get("media_type") == "tv"

        if not is_tv and already_translated:
            continue

        desc = doc.get("description")
        if desc and (not already_translated):
            upd["description"] = translate_text_safe(desc, CACHE)

        seasons = doc.get("seasons", [])
        if seasons:
            modified = False
            for season in seasons:
                for ep in season.get("episodes", []):
                    if ep.get("cevrildi", False):
                        continue
                    if "title" in ep and ep["title"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
                    if modified:
                        ep["cevrildi"] = True
            if modified:
                upd["seasons"] = seasons

        if not is_tv and upd:
            upd["cevrildi"] = True

        if upd:
            results.append((_id, upd))
    return results

async def process_collection_parallel(collection, name, message):
    loop = asyncio.get_event_loop()
    total = collection.count_documents({})
    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0
    ids_cursor = collection.find({}, {"_id": 1})
    ids = [d["_id"] for d in ids_cursor]
    idx = 0
    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        batch_ids = ids[idx: idx + batch_size]
        batch_docs = list(collection.find({"_id": {"$in": batch_ids}}))
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
        elapsed = time.time() - start_time
        speed = done / elapsed if elapsed > 0 else 0
        remaining = total - done
        eta = remaining / speed if speed > 0 else float("inf")
        eta_str = time.strftime("%H:%M:%S", time.gmtime(eta)) if math.isfinite(eta) else "∞"
        cpu = psutil.cpu_percent(interval=None)
        ram_percent = psutil.virtual_memory().percent
        sys_info = f"CPU: {cpu}% | RAM: %{ram_percent}"
        if time.time() - last_update > 30 or idx >= len(ids):
            text = (
                f"{name}: {done}/{total}\n"
                f"{progress_bar(done, total)}\n\n"
                f"Kalan: {remaining}, Hatalar: {errors}\n"
                f"Süre: {eta_str}\n"
                f"{sys_info}"
            )
            try:
                await message.edit_text(text)
            except:
                pass
            last_update = time.time()
    pool.shutdown(wait=False)
    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    start_msg = await message.reply_text("🇹🇷 Türkçe çeviri hazırlanıyor.\nİlerleme tek mesajda gösterilecektir.",
                                         parse_mode=enums.ParseMode.MARKDOWN)
    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(movie_col, "Filmler", start_msg)
    series_total, series_done, series_errors, series_time = await process_collection_parallel(series_col, "Diziler", start_msg)

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)
    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

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

# ----------------- /istatistik Komutu -----------------
def get_db_stats_and_genres(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0, 0.0, {}
    db_local = client[db_name_list[0]]
    total_movies = db_local["movie"].count_documents({})
    total_series = db_local["tv"].count_documents({})
    stats = db_local.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024*1024), 2)
    max_storage_mb = 512
    storage_percent = round((storage_mb / max_storage_mb) * 100, 1)
    genre_stats = defaultdict(lambda: {"film":0, "dizi":0})
    for doc in db_local["movie"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[doc["_id"]]["film"] = doc["count"]
    for doc in db_local["tv"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[doc["_id"]]["dizi"] = doc["count"]
    client.close()
    return total_movies, total_series, storage_mb, storage_percent, genre_stats

def get_system_status():
    cpu = round(psutil.cpu_percent(interval=None),1)
    ram = round(psutil.virtual_memory().percent,1)
    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free/(1024**3),2)
    free_percent = round((disk.free/disk.total)*100,1)
    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec,3600)
    m, s = divmod(rem,60)
    uptime = f"{h}h {m}m {s}s"
    return cpu, ram, free_disk, free_percent, uptime

@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        if len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return
        total_movies, total_series, storage_mb, storage_percent, genre_stats = get_db_stats_and_genres(db_urls[1])
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        genre_lines = []
        for genre, counts in sorted(genre_stats.items(), key=lambda x:x[0]):
            genre_lines.append(f"{genre:<12} | Film: {counts['film']:<3} | Dizi: {counts['dizi']:<3}")
        genre_text = "\n".join(genre_lines)
        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies}\n"
            f"┠ Diziler: {total_series}\n"
            f"┖ Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"<b>Tür Bazlı:</b>\n"
            f"<pre>{genre_text}</pre>\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
