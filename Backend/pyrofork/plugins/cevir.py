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

# GLOBAL STOP EVENT
stop_event = asyncio.Event()

# ------------ DATABASE Bağlantısı ------------
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

# ------------ Dinamik Worker & Batch Ayarı ------------
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

    # Filmler ve Diziler için ayrı batch
    if collection_type == "Filmler":
        if ram_percent < 60:
            batch = 40
        else:
            batch = 20
    elif collection_type == "Diziler":
        if ram_percent < 60:
            batch = 20
        else:
            batch = 10
    else:
        if ram_percent < 40:
            batch = 40
        elif ram_percent < 60:
            batch = 20
        else:
            batch = 10

    return workers, batch, cpu_percent, ram_percent

# ------------ Güvenli Çeviri Fonksiyonu ------------
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

# ------------ Progress Bar (her zaman gösterilecek) ------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ------------ Worker: batch çevirici ------------
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

# ------------ FloodWait güvenli mesaj güncelleme ------------
async def safe_edit_message(message, text, reply_markup=None):
    while True:
        try:
            await message.edit_text(text, reply_markup=reply_markup)
            break
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            break

# ------------ İlerleme mesajı oluşturma ------------
def generate_progress_text(progress_data):
    text = "🇹🇷 Türkçe çeviri ilerlemesi\n\n"
    for name, data in progress_data.items():
        elapsed_sec = max(1, sum(int(x) * t for x, t in zip([3600, 60, 1], map(int, data['elapsed'].split(":")))))
        speed = data['done'] / elapsed_sec if elapsed_sec > 0 else 0
        progress_line = f"{progress_bar(data['done'], data['total'])}\n"
        text += (
            f"📌 {name}: {data['done']}/{data['total']}\n"
            f"{progress_line}"
            f"Kalan: {data['total'] - data['done']}, Hatalar: {data['errors']}\n"
            f"Süre: {data['elapsed']} | ETA: {data['eta']} | Hız: {speed:.2f} items/sec\n"
            f"CPU: {data['cpu']}% | RAM: {data['ram']}% | Workers: {data['workers']} | Batch: {data['batch']}\n\n"
        )
    return text

# ------------ Paralel koleksiyon işleyici (15s garantili güncelleme) ------------
async def process_collection_parallel(collection, name, message, progress_data):
    loop = asyncio.get_event_loop()
    total = collection.count_documents({})
    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    ids_cursor = collection.find({}, {"_id": 1})
    ids = [d["_id"] for d in ids_cursor]

    idx = 0
    workers, batch_size, _, _ = dynamic_config(name)
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        if stop_event.is_set():
            break

        batch_ids = ids[idx: idx + batch_size]
        batch_docs = list(collection.find({"_id": {"$in": batch_ids}}))
        if not batch_docs:
            break

        batch_start = time.time()
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
                    collection.update_one({"_id": _id}, {"$set": upd})
                done += 1
            except Exception:
                errors += 1

            # 15 saniyede bir mesaj güncelle
            if time.time() - last_update > 15:
                elapsed = time.time() - start_time
                remaining = total - done
                eta = remaining / (done / elapsed) if done > 0 else float("inf")
                eta_str = time.strftime("%H:%M:%S", time.gmtime(eta)) if math.isfinite(eta) else "∞"
                elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                _, _, cpu_percent, ram_percent = dynamic_config(name)
                progress_data[name] = {
                    "done": done,
                    "total": total,
                    "errors": errors,
                    "elapsed": elapsed_str,
                    "eta": eta_str,
                    "cpu": cpu_percent,
                    "ram": ram_percent,
                    "workers": workers,
                    "batch": batch_size
                }
                text = generate_progress_text(progress_data)
                await safe_edit_message(message, text,
                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]))
                last_update = time.time()

        idx += len(batch_ids)

    pool.shutdown(wait=False)
    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

# ------------ Callback: iptal butonu ------------
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

# ------------ /cevir Komutu ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    stop_event.clear()

    progress_data = {
        "Filmler": {"done": 0, "total": movie_col.count_documents({}), "errors": 0, "elapsed": "00:00:00", "eta": "∞",
                    "cpu": 0, "ram": 0, "workers": 0, "batch": 0},
        "Diziler": {"done": 0, "total": series_col.count_documents({}), "errors": 0, "elapsed": "00:00:00", "eta": "∞",
                    "cpu": 0, "ram": 0, "workers": 0, "batch": 0}
    }

    start_msg = await message.reply_text(
        generate_progress_text(progress_data),
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(
        movie_col, "Filmler", start_msg, progress_data
    )

    series_total, series_done, series_errors, series_time = await process_collection_parallel(
        series_col, "Diziler", start_msg, progress_data
    )

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)

    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    total_time_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

    summary = (
        "🎉 Türkçe Çeviri Sonuçları\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {total_time_str}\n"
    )
    try:
        await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# ------------ Callback query handler ------------
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data == "stop":
        await handle_stop(query)
