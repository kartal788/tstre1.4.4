import asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import psutil
import time
import math
import os
from pyrogram.errors import FloodWait

from Backend.helper.custom_filter import CustomFilters

stop_event = asyncio.Event()

# DATABASE
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

# DYNAMIC CONFIG
def dynamic_config(collection_type="general"):
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=None)
    if cpu_percent < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent < 60:
        workers = max(1, cpu_count)
    else:
        workers = 1
    if collection_type == "Filmler":
        batch = 40 if ram_percent < 60 else 20
    elif collection_type == "Diziler":
        batch = 20 if ram_percent < 60 else 10
    else:
        batch = 20
    return workers, batch, cpu_percent, ram_percent

# SAFE TRANSLATE
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

# PROGRESS BAR
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# BATCH WORKER
def translate_batch_worker(batch, stop_flag):
    CACHE = {}
    results = []
    for doc in batch:
        if stop_flag.is_set():
            break
        _id = doc.get("_id")
        upd = {}
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if stop_flag.is_set():
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

# SAFE EDIT
async def safe_edit_message(message, text, reply_markup=None):
    while True:
        try:
            await message.edit_text(text, reply_markup=reply_markup)
            break
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            break

# GENERATE PROGRESS TEXT
def generate_progress_text(progress_data, elapsed_str):
    text = "🇹🇷 Türkçe çeviri ilerlemesi\n\n"
    for name, data in progress_data.items():
        speed = data['done'] / max(1, sum(int(x)*t for x,t in zip([3600,60,1], map(int, elapsed_str.split(":")))))
        progress_line = f"{progress_bar(data['done'], data['total'])}\n"
        text += (
            f"📌 {name}: {data['done']}/{data['total']}\n"
            f"{progress_line}"
            f"Kalan: {data['total']-data['done']}, Hatalar: {data['errors']}\n"
            f"ETA: {data['eta']} | Hız: {speed:.2f} items/sec\n\n"
        )
    text += f"Süre: {elapsed_str}\n"
    text += f"CPU: {progress_data['Filmler']['cpu']}% | RAM: {progress_data['Filmler']['ram']}% | Workers: {progress_data['Filmler']['workers']} | Batch: {progress_data['Filmler']['batch']}"
    return text

# FINAL SUMMARY
def generate_final_summary(progress_data, elapsed_seconds):
    hours, rem = divmod(int(elapsed_seconds), 3600)
    minutes, seconds = divmod(rem, 60)
    elapsed_str = f"{hours}h {minutes}m {seconds}s"

    total_all = sum(progress_data[name]["total"] for name in progress_data)
    done_all = sum(progress_data[name]["done"] for name in progress_data)
    errors_all = sum(progress_data[name]["errors"] for name in progress_data)
    remaining_all = total_all - done_all

    text = "🎉 Türkçe Çeviri Sonuçları\n\n"
    for name in ["Filmler","Diziler"]:
        data = progress_data[name]
        text += (
            f"📌 {name}: {data['done']}/{data['total']}\n"
            f"{progress_bar(data['done'], data['total'])}\n"
            f"Kalan: {data['total']-data['done']}, Hatalar: {data['errors']}\n\n"
        )

    text += (
        "📊 Genel Özet\n"
        f"Toplam içerik : {total_all}\n"
        f"Başarılı     : {done_all - errors_all}\n"
        f"Hatalı       : {errors_all}\n"
        f"Kalan        : {remaining_all}\n"
        f"Toplam süre  : {elapsed_str}\n"
    )
    return text

# PROCESS COLLECTION
async def process_collection_parallel(collection, name, message, progress_data, last_text_holder, start_time):
    loop = asyncio.get_event_loop()
    total = collection.count_documents({})
    done = 0
    errors = 0
    last_update = 0
    ids = [d["_id"] for d in collection.find({}, {"_id":1})]

    workers, batch_size, cpu_percent, ram_percent = dynamic_config(name)
    progress_data[name].update({"cpu":cpu_percent,"ram":ram_percent,"workers":workers,"batch":batch_size})

    idx = 0
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        if stop_event.is_set():
            break
        batch_ids = ids[idx: idx+batch_size]
        batch_docs = list(collection.find({"_id":{"$in":batch_ids}}))
        if not batch_docs:
            break
        try:
            future = loop.run_in_executor(pool, translate_batch_worker, batch_docs, stop_event)
            results = await future
        except Exception:
            errors += len(batch_docs)
            idx += len(batch_ids)
            await asyncio.sleep(1)
            continue

        for _id, upd in results:
            try:
                if stop_event.is_set():
                    break
                if upd:
                    collection.update_one({"_id":_id}, {"$set":upd})
                done += 1

                # Update every 5 items + 15-20s
                update_now = False
                if done % 5 == 0 or idx + len(batch_ids) >= len(ids):
                    elapsed_since_last = time.time() - last_update
                    if elapsed_since_last >= 15 or elapsed_since_last > 20:
                        update_now = True

                if update_now:
                    elapsed = time.time() - start_time
                    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                    remaining = total - done
                    eta = remaining / (done/elapsed) if done>0 else float("inf")
                    eta_str = time.strftime("%H:%M:%S", time.gmtime(eta)) if math.isfinite(eta) else "∞"

                    progress_data[name].update({
                        "done": done,
                        "total": total,
                        "errors": errors,
                        "eta": eta_str
                    })

                    new_text = generate_progress_text(progress_data, elapsed_str)
                    if new_text != last_text_holder["text"]:
                        await safe_edit_message(message, new_text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]))
                        last_text_holder["text"] = new_text
                        last_update = time.time()
            except Exception:
                errors += 1
        idx += len(batch_ids)

    pool.shutdown(wait=False)
    return total, done, errors

# HANDLE STOP
async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text("⛔ İşlem iptal edildi!")
    except:
        pass
    try:
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# /cevir COMMAND
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    stop_event.clear()

    workers_film, batch_film, cpu_film, ram_film = dynamic_config("Filmler")
    workers_series, batch_series, cpu_series, ram_series = dynamic_config("Diziler")

    progress_data = {
        "Filmler":{"done":0,"total":movie_col.count_documents({}),"errors":0,"eta":"∞","cpu":cpu_film,"ram":ram_film,"workers":workers_film,"batch":batch_film},
        "Diziler":{"done":0,"total":series_col.count_documents({}),"errors":0,"eta":"∞","cpu":cpu_series,"ram":ram_series,"workers":workers_series,"batch":batch_series}
    }
    last_text_holder = {"text":""}
    start_time = time.time()

    start_msg = await message.reply_text(
        generate_progress_text(progress_data, "00:00:00"),
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    await process_collection_parallel(movie_col, "Filmler", start_msg, progress_data, last_text_holder, start_time)
    await process_collection_parallel(series_col, "Diziler", start_msg, progress_data, last_text_holder, start_time)

    # Final summary
    elapsed = time.time() - start_time
    final_text = generate_final_summary(progress_data, elapsed)
    await safe_edit_message(start_msg, final_text)

# CALLBACK
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data == "stop":
        await handle_stop(query)
