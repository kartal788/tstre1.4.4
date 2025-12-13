import asyncio
import time
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from deep_translator import GoogleTranslator
import psutil

# ---------------- CONFIG ----------------
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
stop_event = asyncio.Event()
DOWNLOAD_DIR = "/"

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
MONGO_URL = db_urls[1] if len(db_urls) > 1 else db_urls[0]

client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]
movie_col = db["movie"]
series_col = db["tv"]

bot_start_time = time.time()

# ---------------- UTILS ----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    workers = max(1, min(cpu_count, 4))
    batch_size = 50 if ram_percent < 50 else 25 if ram_percent < 75 else 10
    return workers, batch_size

def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {min(percent,100):.2f}%"

def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"
    total_seconds = int(total_seconds)
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text(
            "⛔ İşlem **iptal edildi**!",
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# ---------------- /CEVIR ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    global stop_event
    if stop_event.is_set():
        await message.reply_text("⛔ Zaten devam eden bir işlem var.")
        return
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": movie_col.count_documents({}), "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "total": series_col.count_documents({}), "done": 0, "errors": 0}
    ]

    start_time = time.time()
    last_update = 0
    update_interval = 15
    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    def translate_batch_worker(batch_data):
        batch_docs = batch_data["docs"]
        stop_flag_set = batch_data["stop_flag_set"]
        if stop_flag_set:
            return []
        CACHE = {}
        results = []

        for doc in batch_docs:
            if stop_flag_set:
                break
            _id = doc.get("_id")
            upd = {}
            cevrildi = doc.get("cevrildi", False)

            if cevrildi:
                continue

            if doc.get("description"):
                upd["description"] = translate_text_safe(doc["description"], CACHE)

            seasons = doc.get("seasons")
            if seasons:
                for s in seasons:
                    for ep in s.get("episodes", []):
                        if ep.get("cevrildi", False):
                            continue
                        if ep.get("title"):
                            ep["title"] = translate_text_safe(ep["title"], CACHE)
                        if ep.get("overview"):
                            ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        ep["cevrildi"] = True
                upd["seasons"] = seasons

            upd["cevrildi"] = True
            results.append((_id, upd))
        return results

    try:
        for c in collections:
            col = c["col"]
            ids = [d["_id"] for d in col.find({}, {"_id":1})]
            idx = 0

            while idx < len(ids):
                if stop_event.is_set():
                    break

                batch_ids = ids[idx: idx+batch_size]
                batch_docs = list(col.find({"_id":{"$in": batch_ids}}))
                worker_data = {"docs": batch_docs, "stop_flag_set": stop_event.is_set()}
                loop = asyncio.get_event_loop()
                results = await loop.run_in_executor(pool, translate_batch_worker, worker_data)

                for _id, upd in results:
                    try:
                        if upd:
                            col.update_one({"_id":_id}, {"$set":upd})
                        c["done"] += 1
                    except:
                        c["errors"] += 1
                idx += len(batch_ids)

                if time.time() - last_update > update_interval or idx >= len(ids) or stop_event.is_set():
                    text = ""
                    for col_summary in collections:
                        rem = col_summary["total"] - col_summary["done"]
                        text += f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n"
                        text += f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
                        text += f"Hatalar: `{col_summary['errors']}` | Kalan: {rem}\n\n"
                    elapsed = time.time() - start_time
                    total_done = sum(x["done"] for x in collections)
                    total_all = sum(x["total"] for x in collections)
                    rem_all = total_all - total_done
                    eta_seconds = rem_all / (total_done / elapsed) if total_done > 0 else -1
                    text += f"Süre: `{format_time_custom(elapsed)}` (`{format_time_custom(eta_seconds)}`)\n"
                    text += f"CPU: `{psutil.cpu_percent()}%` | RAM: `{psutil.virtual_memory().percent}%`"
                    try:
                        await start_msg.edit_text(
                            text,
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                        )
                    except:
                        pass
                    last_update = time.time()
    finally:
        pool.shutdown(wait=False)

    # Final ekran
    final_text = "🎉 **Türkçe Çeviri Sonuçları**\n\n"
    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    errors_all = sum(c["errors"] for c in collections)
    rem_all = total_all - done_all
    total_time = format_time_custom(time.time() - start_time)

    for c in collections:
        final_text += f"📌 **{c['name']}**: {c['done']}/{c['total']}\n"
        final_text += f"{progress_bar(c['done'], c['total'])}\n"
        final_text += f"Hatalar: `{c['errors']}`\n\n"
    final_text += f"📊 **Genel Özet**\nToplam içerik: `{total_all}`\nBaşarılı: `{done_all-errors_all}`\nHatalı: `{errors_all}`\nKalan: `{rem_all}`\nToplam süre: `{total_time}`"

    try:
        await start_msg.edit_text(final_text, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# ---------------- /CEVIRKALDIR ----------------
@Client.on_message(filters.command("cevirkaldir") & filters.private & filters.user(OWNER_ID))
async def cevirkaldir(client: Client, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları kaldırılıyor...")
    total_updated = 0
    for col in (movie_col, series_col):
        docs_cursor = col.find({"cevrildi": True}, {"_id":1})
        bulk_ops = [UpdateOne({"_id":doc["_id"]},{"$unset":{"cevrildi":""}}) for doc in docs_cursor]
        if bulk_ops:
            res = col.bulk_write(bulk_ops)
            total_updated += res.modified_count
    await status.edit_text(f"✅ 'cevrildi' alanları kaldırıldı.\nToplam güncellenen kayıt: {total_updated}")

# ---------------- /CEVIREKLE ----------------
@Client.on_message(filters.command("cevirekle") & filters.private & filters.user(OWNER_ID))
async def cevirekle(client: Client, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları ekleniyor...")
    total_updated = 0
    for col in (movie_col, series_col):
        docs_cursor = col.find({"cevrildi":{"$ne":True}}, {"_id":1})
        bulk_ops = [UpdateOne({"_id":doc["_id"]},{"$set":{"cevrildi":True}}) for doc in docs_cursor]
        if bulk_ops:
            res = col.bulk_write(bulk_ops)
            total_updated += res.modified_count
    await status.edit_text(f"✅ 'cevrildi' alanları eklendi.\nToplam güncellenen kayıt: {total_updated}")

# ---------------- /TUR ----------------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_ve_platform_duzelt(client: Client, message: Message):
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

    platform_map = {
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

            # Türleri güncelle
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                updated = True
            genres = new_genres

            # Telegram alanı üzerinden platform ekle
            for t in doc.get("telegram", []):
                name_field = t.get("name", "").lower()
                for key, val in platform_map.items():
                    if key.lower() in name_field and val not in genres:
                        genres.append(val)
                        updated = True

            # Sezonlardaki telegram kontrolleri
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name", "").lower()
                        for key, val in platform_map.items():
                            if key.lower() in name_field and val not in genres:
                                genres.append(val)
                                updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": genres}}))
                total_fixed += 1

        if bulk_ops:
            col.bulk_write(bulk_ops)

    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ---------------- /ISTATISTIK ----------------
def get_db_stats_and_genres(url):
    client = MongoClient(url)
    db = client[client.list_database_names()[0]]

    total_movies = db["movie"].count_documents({})
    total_series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize",0)/(1024*1024),2)
    storage_percent = round((storage_mb/512)*100,1)

    genre_stats=defaultdict(lambda:{"film":0,"dizi":0})
    for d in db["movie"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[d["_id"]]["film"]=d["count"]
    for d in db["tv"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[d["_id"]]["dizi"]=d["count"]
    return total_movies,total_series,storage_mb,storage_percent,genre_stats

def get_system_status():
    cpu = round(psutil.cpu_percent(interval=1),1)
    ram = round(psutil.virtual_memory().percent,1)
    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free/(1024**3),2)
    free_percent = round((disk.free/disk.total)*100,1)
    
    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}sa {m}dk {s}sn"

    return cpu, ram, free_disk, free_percent, uptime

@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def istatistik(client: Client, message: Message):
    total_movies,total_series,storage_mb,storage_percent,genre_stats=get_db_stats_and_genres(MONGO_URL)
    cpu,ram,free_disk,free_percent,uptime=get_system_status()

    genre_text="\n".join(f"{g:<14} | Film: {c['film']:<4} | Dizi: {c['dizi']:<4}" for g,c in sorted(genre_stats.items()))

    text=(
        f"⌬ <b>İstatistik</b>\n\n"
        f"┠ Filmler : {total_movies}\n"
        f"┠ Diziler : {total_series}\n"
        f"┖ Depolama: {storage_mb} MB (%{storage_percent})\n\n"
        f"<b>Tür Dağılımı</b>\n<pre>{genre_text}</pre>\n\n"
        f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
        f"┖ RAM → {ram}% | Süre → {uptime}"
    )

    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# ---------------- CALLBACK QUERY ----------------
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data=="stop":
        await handle_stop(query)
