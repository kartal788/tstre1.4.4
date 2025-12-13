import asyncio
import time
import os
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

OWNER_ID = int(os.getenv("OWNER_ID", 12345))  # Owner ID ortam değişkeni
DOWNLOAD_DIR = "/"
bot_start_time = time.time()
stop_event = asyncio.Event()  # /iptal için global event

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

# ---------------- Dinamik config ----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    workers = max(1, min(cpu_count, 4))
    batch = 50 if ram_percent < 50 else 25 if ram_percent < 75 else 10
    return workers, batch

# ---------------- Güvenli çeviri ----------------
def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr

# ---------------- Progress Bar ----------------
def progress_bar(current, total, bar_length=12):
    percent = min((current/total)*100 if total>0 else 0, 100.0)
    filled_length = int(bar_length*current//total) if total>0 else 0
    bar = "⬢"*filled_length + "⬡"*(bar_length-filled_length)
    return f"[{bar}] {percent:.2f}%"

# ---------------- Zaman formatı ----------------
def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"
    total_seconds = int(total_seconds)
    h, rem = divmod(total_seconds,3600)
    m, s = divmod(rem,60)
    return f"{h}s{m}d{s:02}s"

# ---------------- Worker ----------------
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
        # Eğer "cevrildi" true ise atla
        if doc.get("cevrildi", False):
            continue
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)
        seasons = doc.get("seasons")
        if seasons:
            modified = False
            for season in seasons:
                eps = season.get("episodes",[]) or []
                for ep in eps:
                    if stop_flag_set:
                        break
                    if "title" in ep and ep["title"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
            if modified:
                upd["seasons"] = seasons
        results.append((_id, upd))
    return results

# ---------------- /iptal callback ----------------
async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text("⛔ İşlem iptal edildi!", parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass
    try:
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# ---------------- /cevir komutu ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    global stop_event
    if stop_event.is_set():
        await message.reply_text("⛔ Başka bir işlem devam ediyor.")
        return
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri başlatıldı...",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("/iptal", callback_data="iptal")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": movie_col.count_documents({}), "done":0, "errors":0},
        {"col": series_col, "name": "Diziler", "total": 0, "done":0, "errors":0}
    ]
    # Dizilerde toplam bölüm sayısı
    total_eps = 0
    for doc in series_col.find({}):
        total_eps += sum(len(s.get("episodes",[])) for s in doc.get("seasons",[]))
    collections[1]["total"] = total_eps

    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)
    start_time = time.time()
    last_update = 0

    try:
        for c in collections:
            col = c["col"]
            name = c["name"]
            done = c["done"]
            errors = c["errors"]

            if c["total"]==0:
                continue

            ids_cursor = list(col.find({}))
            # Dizilerde id yerine episode bazlı list
            ids_list = []
            if name=="Filmler":
                ids_list = [{'_id':d['_id']} for d in ids_cursor if not d.get("cevrildi",False)]
            else:
                for d in ids_cursor:
                    if d.get("cevrildi",False):
                        continue
                    for s in d.get("seasons",[]):
                        for ep in s.get("episodes",[]):
                            ids_list.append({'_id':d['_id']})  # episode bazlı sayım için

            idx = 0
            while idx < len(ids_list):
                if stop_event.is_set():
                    break
                batch_ids = ids_list[idx:idx+batch_size]
                batch_docs = [col.find_one({"_id":b["_id"]}) for b in batch_ids]
                worker_data = {"docs": batch_docs, "stop_flag_set": stop_event.is_set()}
                loop = asyncio.get_event_loop()
                results = await loop.run_in_executor(pool, translate_batch_worker, worker_data)

                for _id, upd in results:
                    try:
                        if upd:
                            col.update_one({"_id":_id},{"$set":upd})
                        done +=1
                    except:
                        errors +=1

                idx += len(batch_ids)
                c["done"] = done
                c["errors"] = errors

                # İlerleme güncellemesi 15 saniye
                if time.time()-last_update>15 or idx>=len(ids_list):
                    elapsed = time.time()-start_time
                    total_done = sum(x["done"] for x in collections)
                    total_all = sum(x["total"] for x in collections)
                    remaining = total_all-total_done
                    speed = total_done/elapsed if elapsed>0 else 0
                    eta = remaining/speed if speed>0 else -1

                    text = ""
                    for col_summary in collections:
                        text += f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n{progress_bar(col_summary['done'],col_summary['total'])}\n\n"
                    text += f"Süre: `{format_time_custom(elapsed)}` (`{format_time_custom(eta)}`)"

                    if text != start_msg.text:
                        await start_msg.edit_text(
                            text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("/iptal", callback_data="iptal")]])
                        )
                    last_update = time.time()
    finally:
        pool.shutdown(wait=False)

    final_text = "🎉 Türkçe çeviri tamamlandı!\n\n"
    for col_summary in collections:
        final_text += f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n{progress_bar(col_summary['done'],col_summary['total'])}\nHatalar: `{col_summary['errors']}`\n\n"
    total_elapsed = time.time()-start_time
    final_text += f"Toplam süre: `{format_time_custom(total_elapsed)}`"
    try:
        await start_msg.edit_text(final_text)
    except:
        pass

# ---------------- /cevrekle ve /cevirkaldir ----------------
@Client.on_message(filters.command("cevrekle") & filters.private & filters.user(OWNER_ID))
async def cevrekle(client: Client, message: Message):
    movie_col.update_many({}, {"$set":{"cevrildi":True}})
    series_col.update_many({}, {"$set":{"cevrildi":True}})
    await message.reply_text("Tüm içeriklere 'cevrildi: true' eklendi.")

@Client.on_message(filters.command("cevirkaldir") & filters.private & filters.user(OWNER_ID))
async def cevirkaldir(client: Client, message: Message):
    movie_col.update_many({}, {"$unset":{"cevrildi":""}})
    series_col.update_many({}, {"$unset":{"cevrildi":""}})
    await message.reply_text("'cevrildi' alanı tüm içeriklerden kaldırıldı.")

# ---------------- /tur komutu ----------------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_ve_platform_duzelt(client: Client, message: Message):
    genre_map = {
        "Action": "Aksiyon", "Comedy": "Komedi", "Drama": "Dram", "Horror": "Korku"
        # Buraya diğer türleri ekleyin
    }
    platform_map = {"Netflix":"Netflix","Max":"Max"}

    collections = [(movie_col,"Filmler"),(series_col,"Diziler")]
    total_fixed = 0
    for col,name in collections:
        bulk_ops = []
        for doc in col.find({}):
            genres = doc.get("genres",[])
            updated = False
            new_genres = [genre_map.get(g,g) for g in genres]
            if new_genres != genres:
                updated=True
                genres=new_genres
            if updated:
                bulk_ops.append(UpdateOne({"_id":doc["_id"]},{"$set":{"genres":genres}}))
                total_fixed+=1
        if bulk_ops:
            col.bulk_write(bulk_ops)
    await message.reply_text(f"Tür ve platform güncellemesi tamamlandı. Toplam değiştirilen kayıt: {total_fixed}")

# ---------------- /istatistik komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def send_statistics(client: Client, message: Message):
    total_movies = movie_col.count_documents({})
    total_series = series_col.count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize",0)/(1024*1024),2)
    genre_stats = defaultdict(lambda: {"film":0,"dizi":0})
    for doc in movie_col.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[doc["_id"]]["film"]=doc["count"]
    for doc in series_col.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[doc["_id"]]["dizi"]=doc["count"]
    genre_text="\n".join(f"{k:<12} | Film: {v['film']:<3} | Dizi: {v['dizi']:<3}" for k,v in genre_stats.items())
    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free/(1024**3),2)
    free_percent = round((disk.free/disk.total)*100,1)
    uptime_sec = int(time.time()-bot_start_time)
    h, rem = divmod(uptime_sec,3600)
    m, s = divmod(rem,60)
    uptime=f"{h}s {m}d {s}s"

    text=(
        f"⌬ <b>İstatistik</b>\n\n"
        f"┠ Filmler: {total_movies}\n"
        f"┠ Diziler: {total_series}\n"
        f"┖ Depolama: {storage_mb} MB\n\n"
        f"<b>Tür Bazlı:</b>\n<pre>{genre_text}</pre>\n\n"
        f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
        f"┖ RAM → {ram}% | Süre → {uptime}"
    )
    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# ---------------- Callback query ----------------
@Client.on_callback_query()
async def callback_query_handler(client: Client, query: CallbackQuery):
    if query.data=="iptal":
        await handle_stop(query)
