import asyncio
import time
import os
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

# Kütüphane İçe Aktarımları
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import psutil

# NOT: Bu kısımlar sizin ortamınıza göre ayarlanmalıdır.
# OWNER_ID'yi ortam değişkeninden veya yapılandırmadan alın.
OWNER_ID = int(os.getenv("OWNER_ID", 12345)) 

# GLOBAL STOP EVENT
stop_event = asyncio.Event()

# ------------ DATABASE Bağlantısı ------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    MONGO_URL = db_urls[0] 
else:
    MONGO_URL = db_urls[1] 

try:
    client_db = MongoClient(MONGO_URL)
    db_name = client_db.list_database_names()[0]
    db = client_db[db_name]
    movie_col = db["movie"]
    series_col = db["tv"]
except Exception as e:
    raise Exception(f"MongoDB bağlantı hatası: {e}")

# ------------ Dinamik Worker & Batch Ayarı (Optimizasyon) ------------
def dynamic_config():
    """Çeviri hızını artırmak ve takılmayı azaltmak için optimize edildi."""
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    
    workers = max(1, min(cpu_count, 4)) 

    if ram_percent < 50:
        batch = 50
    elif ram_percent < 75:
        batch = 25
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
        tr = GoogleTranslator(source='en', target='tr').translate(text)
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
    percent_display = min(percent, 100.00)
    return f"[{bar}] {percent_display:.2f}%"

# ------------ ETA Formatlayıcı ------------
def format_time(seconds):
    """Saniye cinsinden süreyi hh:mm:ss formatına dönüştürür."""
    if seconds is None or seconds < 0:
        return "Hesaplanıyor..."
    
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    
    return f"{hours:02d}s {minutes:02d}d {seconds:02d}sn"

# ------------ Worker: batch çevirici ------------
def translate_batch_worker(batch_data):
    """
    Çoklu süreçte (multiprocessing) çalıştırılacak işçi fonksiyonu.
    Girdi: (batch_docs, stop_flag_state)
    Çıktı: [(id, update_dict), ...]
    """
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

        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
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

# ------------ Callback: iptal butonu ------------
async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text("⛔ İşlem **iptal edildi**! Lütfen yeni bir komut başlatmadan önce bir süre bekleyin.", 
                                               parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        pass
    try:
        await callback_query.answer("Durdurma talimatı alındı.")
    except Exception:
        pass

# ------------ /cevir Komutu (Sadece owner) ------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID)) 
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    
    if stop_event.is_set():
         await message.reply_text("⛔ Şu anda devam eden bir işlem var. Lütfen bitmesini veya tamamen iptal olmasını bekleyin.")
         return
         
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.\n\n_İlk çeviri toplu işi (batch) tamamlanana kadar ilerleme %0.00 ve ETA 'Hesaplanıyor...' görünecektir._",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": 0, "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "total": 0, "done": 0, "errors": 0}
    ]

    for c in collections:
        c["total"] = c["col"].count_documents({})
        if c["total"] == 0:
            c["done"] = c["total"] 

    start_time = time.time()
    last_update = 0
    update_interval = 4 

    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)
    
    try:
        for c in collections:
            col = c["col"]
            name = c["name"]
            total = c["total"]
            done = c["done"]
            errors = c["errors"]

            if total == 0:
                continue

            ids_cursor = col.find({}, {"_id": 1})
            ids = [d["_id"] for d in ids_cursor]

            idx = 0
            
            while idx < len(ids):
                if stop_event.is_set():
                    break

                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))

                worker_data = {
                    "docs": batch_docs,
                    "stop_flag_set": stop_event.is_set()
                }

                try:
                    loop = asyncio.get_event_loop()
                    future = loop.run_in_executor(pool, translate_batch_worker, worker_data)
                    results = await future 
                except Exception as e:
                    print(f"Worker Hatası ({name}): {e}")
                    errors += len(batch_docs)
                    idx += len(batch_ids)
                    c["errors"] = errors
                    c["done"] = done
                    await asyncio.sleep(1)
                    continue

                for _id, upd in results:
                    if stop_event.is_set():
                        break
                    
                    try:
                        if upd:
                            col.update_one({"_id": _id}, {"$set": upd})
                        done += 1
                    except Exception as e:
                        print(f"DB Yazma Hatası: {e}")
                        errors += 1

                idx += len(batch_ids)
                c["done"] = done
                c["errors"] = errors
                
                # İlerleme güncellemesi
                if time.time() - last_update > update_interval or idx >= len(ids) or stop_event.is_set():
                    
                    text = ""
                    total_done = 0
                    total_all = 0
                    total_errors = 0

                    cpu = psutil.cpu_percent(interval=None)
                    ram_percent = psutil.virtual_memory().percent

                    for col_summary in collections:
                        text += (
                            f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n"
                            f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
                            f"Kalan: {col_summary['total'] - col_summary['done']}\n\n"
                        )
                        total_done += col_summary['done']
                        total_all += col_summary['total']
                        total_errors += col_summary['errors']

                    elapsed_time = time.time() - start_time
                    remaining_all = total_all - total_done

                    # ETA Hesaplama
                    if total_done > 0:
                        speed = total_done / elapsed_time # Öğe/saniye
                        eta_seconds = remaining_all / speed
                        eta_str = format_time(eta_seconds)
                    else:
                        eta_str = "Hesaplanıyor..."
                        
                    # Geçen süreyi formatla
                    elapsed_str = format_time(elapsed_time)

                    text += (
                        f"⏱ Geçen Süre: `{elapsed_str}`\n"
                        f"⏳ **Tahmini Kalan Süre (ETA)**: `{eta_str}`\n"
                        f"💻 CPU: `{cpu}%` | RAM: `{ram_percent}%`"
                    )

                    try:
                        await start_msg.edit_text(
                            text,
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                        )
                    except Exception as e:
                        print(f"Telegram Mesaj Güncelleme Hatası: {e}")
                        pass
                    
                    last_update = time.time()

    finally:
        pool.shutdown(wait=False)

    # ------------ SONUÇ EKRANI ------------
    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    errors_all = sum(c["errors"] for c in collections)
    remaining_all = total_all - done_all

    total_time = round(time.time() - start_time)
    total_time_str = format_time(total_time)

    final_text = "🎉 **Türkçe Çeviri Sonuçları**\n\n"
    for col_summary in collections:
        final_text += (
            f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n"
            f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
            f"Hatalar: `{col_summary['errors']}`\n\n"
        )

    final_text += (
        f"📊 **Genel Özet**\n"
        f"Toplam içerik: `{total_all}`\n"
        f"Başarılı    : `{done_all - errors_all}`\n"
        f"Hatalı      : `{errors_all}`\n"
        f"Kalan       : `{remaining_all}`\n"
        f"Toplam süre  : `{total_time_str}`"
    )

    try:
        await start_msg.edit_text(final_text, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# ------------ Callback query handler ------------
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data == "stop":
        await handle_stop(query)
