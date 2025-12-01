from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import os
import importlib.util
import time
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import psutil
from datetime import datetime, timedelta

# ------------ DATABASE Bağlantısı ------------
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
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

movie_col = db["movie"]
series_col = db["tv"]
control_col = db["translate_control"]  # iptal için ortak kontrol koleksiyonu

translator = GoogleTranslator(source='en', target='tr')

# ------------ GLOBAL AYAR: Otomatik Konfigürasyon (VPS'e göre) ------------
def auto_config():
    cpu_count = multiprocessing.cpu_count()
    ram_gb = psutil.virtual_memory().total / (1024**3)

    # worker heuristic (local process workers)
    if ram_gb < 0.7:
        workers = 1
    elif ram_gb < 1.5:
        workers = max(1, cpu_count)
    elif ram_gb < 3:
        workers = max(1, cpu_count * 2)
    else:
        workers = max(1, cpu_count * 2)

    workers = max(1, min(workers, 16))

    # batch boyutu RAM'e göre
    if ram_gb <= 0.6:
        batch = 5
    elif ram_gb <= 1:
        batch = 15
    elif ram_gb <= 2:
        batch = 40
    elif ram_gb <= 4:
        batch = 80
    else:
        batch = 120

    return workers, batch

# ------------ Kontrol: Tüm VPS'ler için stop flag (DB tabanlı) ------------
def set_global_stop():
    control_col.update_one({"_id": "global"}, {"$set": {"stop": True, "ts": datetime.utcnow()}}, upsert=True)

def clear_global_stop():
    control_col.update_one({"_id": "global"}, {"$set": {"stop": False, "ts": datetime.utcnow()}}, upsert=True)

def check_global_stop():
    doc = control_col.find_one({"_id": "global"})
    return bool(doc and doc.get("stop", False))

# ------------ Güvenli Çeviri Fonksiyonu (aynı) ------------
def translate_text_safe(text):
    if not text or str(text).strip() == "":
        return ""
    try:
        return translator.translate(str(text))
    except Exception:
        return str(text)

# ------------ Progress bar (aynı) ------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ------------ Worker: process içinde çalışacak batch çevirici ------------
def translate_batch_worker(batch):
    # küçük local cache (process başına)
    CACHE = {}
    def fast_translate(t):
        if not t:
            return ""
        if t in CACHE:
            return CACHE[t]
        try:
            tr = GoogleTranslator(source='en', target='tr').translate(t)
        except Exception:
            tr = t
        CACHE[t] = tr
        return tr

    results = []
    for doc in batch:
        _id = doc.get("_id")
        upd = {}
        # description
        desc = doc.get("description")
        if desc:
            upd["description"] = fast_translate(desc)
        # seasons/episodes
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if "title" in ep and ep["title"]:
                        ep["title"] = fast_translate(ep["title"])
                        modified = True
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = fast_translate(ep["overview"])
                        modified = True
            if modified:
                upd["seasons"] = seasons
        # genres
        genres = doc.get("genres")
        if genres and isinstance(genres, list):
            upd["genres"] = [fast_translate(g) for g in genres]
        results.append((_id, upd))
    return results

# ------------ Yeni paralel koleksiyon işleyici (senin process_collection_interactive yerine) ------------
async def process_collection_parallel(collection, name, message):
    # otomatik worker ve batch
    workers, batch_size = auto_config()
    loop = __import__("asyncio").get_event_loop()
    pool = ProcessPoolExecutor(max_workers=workers)

    total = collection.count_documents({})
    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    # fetch ids once (we'll read docs individually to minimize mem)
    ids_cursor = collection.find({}, {"_id": 1})
    ids = [d["_id"] for d in ids_cursor]
    idx = 0

    try:
        while idx < len(ids):
            if check_global_stop():
                # iptal edilmiş
                try:
                    await message.edit_text("⛔ İşlem global olarak iptal edildi!")
                except:
                    pass
                break

            # build batch of documents to translate
            batch_ids = ids[idx: idx + batch_size]
            batch_docs = list(collection.find({"_id": {"$in": batch_ids}}))
            if not batch_docs:
                break

            # run translate in process pool
            try:
                future = loop.run_in_executor(pool, translate_batch_worker, batch_docs)
                results = await future
            except Exception as e:
                errors += len(batch_docs)
                idx += len(batch_ids)
                # slight delay to avoid tight loop on failure
                await __import__("asyncio").sleep(1)
                continue

            # apply results to DB
            for _id, upd in results:
                try:
                    if upd:
                        collection.update_one({"_id": _id}, {"$set": upd})
                    done += 1
                except Exception:
                    errors += 1

            idx += len(batch_ids)

            # progress hesapla
            elapsed = time.time() - start_time
            speed = done / elapsed if elapsed > 0 else 0
            remaining = total - done
            eta = remaining / speed if speed > 0 else float("inf")
            eta_str = time.strftime("%H:%M:%S", time.gmtime(eta)) if math.isfinite(eta) else "∞"

            # sistem bilgisi
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory()
            sys_info = f"CPU: {cpu}% | RAM: {ram.used // (1024*1024)}MB / {ram.total // (1024*1024)}MB (%{ram.percent})"

            if time.time() - last_update > 5 or idx >= len(ids):
                text = (
                    f"{name}: {done}/{total}\n"
                    f"{progress_bar(done, total)}\n\n"
                    f"Kalan: {remaining}, Hatalar: {errors}\n"
                    f"ETA: {eta_str}\n"
                    f"{sys_info}"
                )
                try:
                    await message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et (Tüm VPS)", callback_data="global_stop")]])
                    )
                except Exception:
                    pass
                last_update = time.time()

    finally:
        pool.shutdown(wait=False)

    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

# ------------ Callback: global stop butonu ------------
# Bu handler'ı Pyrogram app'ine eklemen lazım (aşağıda @Client.on_callback_query ekledim)
async def handle_global_stop(callback_query: CallbackQuery):
    set_global_stop()
    try:
        await callback_query.message.edit_text("⛔ Tüm VPS'lere iptal komutu gönderildi (DB üzerinden).")
    except:
        pass
    try:
        await callback_query.answer("Durdurma talimatı gönderildi.")
    except:
        pass

# ------------ /cevir Komutu (ana) ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    # temizle (önceki stop varsa kaldır)
    clear_global_stop()

    start_msg = await message.reply_text(
        "🇹🇷 Film ve dizi açıklamaları Türkçeye çevriliyor…\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et (Tüm VPS)", callback_data="global_stop")]])
    )

    # Filmler
    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(
        movie_col, "Filmler", start_msg
    )

    if check_global_stop():
        return

    # Diziler
    series_total, series_done, series_errors, series_time = await process_collection_parallel(
        series_col, "Diziler", start_msg
    )

    # -------- Özet --------
    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)

    # Saat/dakika/saniye formatı
    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours)}h{int(minutes)}m{int(seconds)}s"

    summary = (
        "🎉 *Film & Dizi Türkçeleştirme Sonuçları*\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {eta_str}\n"
    )

    try:
        await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# ------------ Callback query handler ekleme ------------
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data == "global_stop":
        await handle_global_stop(query)

# not: mevcut main/app başlatma kodun varsa aynı şekilde kullan

