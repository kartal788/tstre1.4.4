import asyncio
import os
import time
import psutil
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from pyrogram import filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from pymongo import MongoClient
from deep_translator import GoogleTranslator

from Backend.helper.custom_filter import CustomFilters
from Backend import StreamBot  # Bot instance’ınız

# GLOBAL STOP EVENT (multiprocessing uyumlu)
stop_event = multiprocessing.Event()

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
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=0.5)

    if cpu_percent < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent < 60:
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

# ------------ Güvenli Çeviri Fonksiyonu ------------
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

# ------------ Progress Bar ------------
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
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if stop_flag.is_set():
                        break
                    if "title" in ep and ep["title"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
            # Sezonları her durumda ekle ki update çalışsın
            upd["seasons"] = seasons

        results.append((_id, upd))

    return results

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

# ------------ /cevir Komutu (Sadece owner) ------------
@StreamBot.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: StreamBot, message: Message):
    global stop_event
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": 0, "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "total": 0, "done": 0, "errors": 0}
    ]

    for c in collections:
        c["total"] = c["col"].count_documents({})

    start_time = time.time()
    last_update = 0
    update_interval = 3  # daha sık güncelleme

    for c in collections:
        col = c["col"]
        name = c["name"]
        total = c["total"]
        done = 0
        errors = 0

        ids_cursor = col.find({}, {"_id": 1})
        ids = [d["_id"] for d in ids_cursor]

        idx = 0
        workers, batch_size = dynamic_config()
        batch_size = min(batch_size, 20)
        pool = multiprocessing.get_context("spawn").Pool(workers)

        while idx < len(ids):
            if stop_event.is_set():
                break

            batch_ids = ids[idx: idx + batch_size]
            batch_docs = list(col.find({"_id": {"$in": batch_ids}}))

            try:
                results = pool.apply(translate_batch_worker, args=(batch_docs, stop_event))
            except Exception:
                errors += len(batch_docs)
                idx += len(batch_ids)
                await asyncio.sleep(1)
                continue

            for _id, upd in results:
                try:
                    if upd:
                        col.update_one({"_id": _id}, {"$set": upd})
                    done += 1  # her doküman için done artır
                except:
                    errors += 1

            idx += len(batch_ids)
            c["done"] = done
            c["errors"] = errors

            # Tek Mesaj Güncellemesi
            if time.time() - last_update > update_interval or idx >= len(ids):
                text = ""
                total_done = 0
                total_all = 0

                cpu = psutil.cpu_percent(interval=None)
                ram_percent = psutil.virtual_memory().percent

                for col_summary in collections:
                    text += (
                        f"📌 {col_summary['name']}: {col_summary['done']}/{col_summary['total']}\n"
                        f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
                        f"Kalan: {col_summary['total'] - col_summary['done']}\n\n"
                    )
                    total_done += col_summary['done']
                    total_all += col_summary['total']

                remaining_all = total_all - total_done
                elapsed_time = time.time() - start_time

                text += (
                    f"⏱ Süre: {round(elapsed_time,2)} sn | Kalan toplam: {remaining_all}\n"
                    f"💻 CPU: {cpu}% | RAM: {ram_percent}%"
                )

                try:
                    await start_msg.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                    )
                except:
                    pass
                last_update = time.time()

        pool.close()
        pool.join()

    # ------------ SONUÇ EKRANI ------------
    final_text = "🎉 Türkçe Çeviri Sonuçları\n\n"

    for col_summary in collections:
        final_text += (
            f"📌 {col_summary['name']}: {col_summary['done']}/{col_summary['total']}\n"
            f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
            f"Kalan: {col_summary['total'] - col_summary['done']}, Hatalar: {col_summary['errors']}\n\n"
        )

    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    errors_all = sum(c["errors"] for c in collections)
    remaining_all = total_all - done_all

    total_time = round(time.time() - start_time)
    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours)}s{int(minutes)}d{int(seconds)}s"

    final_text += (
        f"📊 Genel Özet\n"
        f"Toplam içerik : {total_all}\n"
        f"Başarılı     : {done_all - errors_all}\n"
        f"Hatalı       : {errors_all}\n"
        f"Kalan        : {remaining_all}\n"
        f"Toplam süre  : {eta_str}"
    )

    try:
        await start_msg.edit_text(final_text)
    except:
        pass

# ------------ Callback query handler ------------
@StreamBot.on_callback_query()
async def _cb(client: StreamBot, query: CallbackQuery):
    if query.data == "stop":
        await handle_stop(query)
