import asyncio
import time
import os
import multiprocessing
import json # JSON dönüşümü için eklendi
from concurrent.futures import ProcessPoolExecutor, TimeoutError, BrokenProcessPool, Future

# Kütüphane İçe Aktarımları
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
import pymongo
from deep_translator import GoogleTranslator
import psutil
import traceback 
from bson.objectid import ObjectId # MongoDB ID'leri için
from datetime import datetime # Tarih/Zaman nesneleri için

# ------------ ÖZEL FİLTRE İÇE AKTARIMI ------------
try:
    from Backend.helper.custom_filter import CustomFilters 
except ImportError:
    print("UYARI: CustomFilters içe aktarılamadı. Basit OWNER_ID filtresi kullanılıyor.")
    OWNER_ID = int(os.getenv("OWNER_ID", 12345))
    class CustomFilters:
        @staticmethod
        async def owner_filter(client, message):
            user = message.from_user or message.sender_chat
            uid = user.id
            return uid == OWNER_ID
        owner = filters.create(owner_filter)
# -------------------------------------------------

# Sabit Çeviri Durumu Etiketi
TRANSLATED_STATUS_FIELD = "translated_status"
TRANSLATED_STATUS_VALUE = "cevrildi"

# GLOBAL STOP EVENT
stop_event = asyncio.Event()

# ------------ DATABASE Bağlantısı ------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]

if len(db_urls) < 2:
    if not db_urls:
         raise Exception("DATABASE ortam değişkeninde geçerli URI bulunamadı!")
    MONGO_URL = db_urls[0] 
else:
    MONGO_URL = db_urls[1] 

try:
    client_db = MongoClient(MONGO_URL.strip()) 
    db_name = client_db.list_database_names()[0]
    db = client_db[db_name]
    movie_col = db["movie"]
    series_col = db["tv"]
except Exception as e:
    raise Exception(f"MongoDB bağlantı hatası ({MONGO_URL.strip()} için): {e}")

# ------------ Dinamik Worker & Batch Ayarı ------------
def dynamic_config():
    """Çeviri hızını artırmak için dinamik ayarlar."""
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
    """Deep Translator ile güvenli çeviri, önbellek kullanarak tekrarları engeller."""
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

# ------------ BSON/ObjectId Temizleyici (KRİTİK) ------------
def clean_bson_types(doc):
    """
    MongoDB dökümanlarını worker'a göndermeden önce JSON/Python uyumlu tiplere dönüştürür.
    (Multiprocessing serileştirme hatasını engeller)
    """
    if isinstance(doc, dict):
        new_doc = {}
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                new_doc[k] = str(v)
            elif isinstance(v, (datetime, bytes)):
                new_doc[k] = str(v)
            elif isinstance(v, (list, dict)):
                new_doc[k] = clean_bson_types(v)
            else:
                new_doc[k] = v
        return new_doc
    elif isinstance(doc, list):
        return [clean_bson_types(item) for item in doc]
    else:
        return doc

# ------------ Progress Bar ve Zaman Formatlama Yardımcı Fonksiyonları ------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    percent_display = min(percent, 100.00)
    return f"[{bar}] {percent_display:.2f}%"

def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"

    total_seconds = int(total_seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    
    return f"{int(hours)}s{int(minutes)}d{int(seconds):02}s"

# ------------ Hata Loglama Fonksiyonu (Telegram'a Gönderim) ------------
async def log_error_to_telegram(client: Client, media_type: str, item_id, error_message: str):
    """Hata detaylarını Telegram'a log mesajı olarak gönderir."""
    
    log_chat_id = OWNER_ID
    error_msg_limited = error_message[:1000]
    
    message = (
        f"🚨 **KRİTİK ÇEVİRİ İŞLEME HATASI**\n\n"
        f"**Tip**: `{media_type.upper()}`\n"
        f"**ID**: `{item_id}`\n"
        f"**Hata**: `{error_msg_limited}`\n\n"
        f"Bu hata büyük ihtimalle döküman yapısından veya Worker/Multiprocessing hatasından kaynaklanmaktadır."
    )
    
    try:
        await client.send_message(log_chat_id, message, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        print(f"Telegram'a log gönderme hatası (OWNER_ID: {log_chat_id}): {e}")

# ------------ Worker: batch çevirici (Hata Kaydı Eklendi) ------------
def translate_batch_worker(batch_data):
    """
    Çoklu süreçte çalıştırılacak işçi fonksiyonu.
    Hata durumunda döküman ID'sini ve hata mesajını döndürür.
    """
    batch_docs = batch_data["docs"]
    stop_flag_set = batch_data["stop_flag_set"]
    
    if stop_flag_set:
        return {"results": [], "error_details": []}

    CACHE = {}
    results = []
    error_details = []

    for doc in batch_docs:
        # Bu aşamada dökümanlar temizlenmiş, yani sadece yapısal hatalar kalmış olmalı.
        _id = doc.get("_id")
        upd = {}
        needs_update = False
        media_type = doc.get('media_type', 'movie')
        
        try:
            # 1. Film Çevirisi VEYA Dizi Ana Açıklaması
            if doc.get(TRANSLATED_STATUS_FIELD) != TRANSLATED_STATUS_VALUE:
                desc = doc.get("description")
                if desc:
                    upd["description"] = translate_text_safe(desc, CACHE)
                    needs_update = True
            
            # 2. Sezon/Bölüm Çevirisi (Diziler için)
            seasons = doc.get("seasons")
            
            if media_type == 'tv' and seasons and isinstance(seasons, list):
                modified = False
                for season in seasons:
                    # Sezon numarası kontrolü (zorunlu olmasa da temiz bir yapı için)
                    if not isinstance(season, dict): continue
                    
                    eps = season.get("episodes", []) or []
                    if not isinstance(eps, list): continue # episodes listesi yoksa atla
                        
                    for ep in eps:
                        if stop_flag_set:
                            break
                        
                        if not isinstance(ep, dict): continue
                        
                        if ep.get(TRANSLATED_STATUS_FIELD) != TRANSLATED_STATUS_VALUE:
                            
                            if "title" in ep and ep["title"]:
                                ep["title"] = translate_text_safe(ep["title"], CACHE)
                                modified = True
                            if "overview" in ep and ep["overview"]:
                                ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                                modified = True
                                
                            if modified:
                                ep[TRANSLATED_STATUS_FIELD] = TRANSLATED_STATUS_VALUE
                                
                if modified:
                    upd["seasons"] = seasons
                    needs_update = True

            if doc.get(TRANSLATED_STATUS_FIELD) != TRANSLATED_STATUS_VALUE and needs_update:
                upd[TRANSLATED_STATUS_FIELD] = TRANSLATED_STATUS_VALUE

            if needs_update:
                results.append((_id, upd))
            
        except Exception as e:
            # İşleme hatası durumunda döküman ID'si ve hata mesajı kaydedilir
            error_details.append({
                "media_type": media_type,
                "id": _id,
                "error": str(e) + "\n" + traceback.format_exc() # Detaylı traceback eklendi
            })
            continue

    return {"results": results, "error_details": error_details}

# ------------ Diğer Yardımcı Fonksiyonlar ve Ana İşleyiciler (Değişmedi) ------------

async def get_translation_count():
    movie_count = movie_col.count_documents({TRANSLATED_STATUS_FIELD: {"$ne": TRANSLATED_STATUS_VALUE}})
    
    series_count = series_col.aggregate([
        {"$unwind": "$seasons"},
        {"$unwind": "$seasons.episodes"},
        {"$match": {f"seasons.episodes.{TRANSLATED_STATUS_FIELD}": {"$ne": TRANSLATED_STATUS_VALUE}}},
        {"$group": {"_id": "$_id"}},
        {"$count": "count"}
    ])
    
    series_to_translate_count = next(series_count, {"count": 0})["count"]

    return movie_count, series_to_translate_count

async def bulk_status_update(collection, action):
    if collection.name == "movie":
        if action == "ekle":
            update_op = {"$set": {TRANSLATED_STATUS_FIELD: TRANSLATED_STATUS_VALUE}}
            msg_action = "etiketlendi"
        else:
            update_op = {"$unset": {TRANSLATED_STATUS_FIELD: ""}}
            msg_action = "etiketi kaldırıldı"
        
        update_result = collection.update_many({}, update_op)
        return f"✅ **{collection.name}** koleksiyonundaki {update_result.modified_count} içerik çevrilmiş olarak {msg_action}."

    elif collection.name == "tv":
        if action == "ekle":
            update_op = {"$set": {
                TRANSLATED_STATUS_FIELD: TRANSLATED_STATUS_VALUE, 
                f"seasons.$[].episodes.$[].{TRANSLATED_STATUS_FIELD}": TRANSLATED_STATUS_VALUE
            }}
            msg_action = "etiketlendi"
        else:
            update_op = {"$unset": {
                TRANSLATED_STATUS_FIELD: "", 
                f"seasons.$[].episodes.$[].{TRANSLATED_STATUS_FIELD}": ""
            }}
            msg_action = "etiketi kaldırıldı"
            
        update_result = collection.update_many({}, update_op)
        return f"✅ **{collection.name}** koleksiyonundaki {update_result.modified_count} içerik çevrilmiş olarak {msg_action} (Bölümler dahil)."
    else:
        return "Geçersiz koleksiyon adı."

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

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner) 
async def turkce_icerik_main(client: Client, message: Message):
    command_parts = message.text.split()
    
    if len(command_parts) == 1:
        await start_translation(client, message)
        return
        
    sub_command = command_parts[1].lower()
    
    if sub_command == "ekle":
        await message.reply_text("⏳ Tüm içeriklere 'çevrildi' etiketi ekleniyor (Filmler ve Bölümler)...")
        movie_msg = await bulk_status_update(movie_col, "ekle")
        series_msg = await bulk_status_update(series_col, "ekle")
        await message.reply_text(f"{movie_msg}\n{series_msg}")
        return
        
    elif sub_command == "kaldir":
        await message.reply_text("⏳ Tüm içeriklerden 'çevrildi' etiketi kaldırılıyor (Filmler ve Bölümler)...")
        movie_msg = await bulk_status_update(movie_col, "kaldir")
        series_msg = await bulk_status_update(series_col, "kaldir")
        await message.reply_text(f"{movie_msg}\n{series_msg}")
        return
        
    elif sub_command == "sayi":
        await message.reply_text("⏳ Çevrilecek içerik sayısı hesaplanıyor...")
        m_count, t_count = await get_translation_count()
        
        await message.reply_text(
            f"📊 **Çeviri Durumu Özeti (Etiket: `{TRANSLATED_STATUS_FIELD}`)**\n\n"
            f"🎬 **Filmler**: `{m_count}` adet (Ana açıklama çevrilmemiş)\n"
            f"📺 **Diziler**: `{t_count}` adet (En az bir bölümü çevrilmemiş)\n\n"
            f"Toplam çevrilecek içerik sayısı: `{m_count + t_count}`"
        )
        return
        
    else:
        await message.reply_text("Geçersiz alt komut. Kullanım: `/cevir`, `/cevir ekle`, `/cevir kaldir`, `/cevir sayi`")


# ------------ Ana Çeviri İşlemi (Güncellendi) ------------
async def start_translation(client: Client, message: Message):
    global stop_event
    
    if stop_event.is_set():
        await message.reply_text("⛔ Şu anda devam eden bir işlem var. Lütfen bitmesini veya tamamen iptal olmasını bekleyin.")
        return
        
    stop_event.clear()

    m_count, t_count = await get_translation_count()
    
    if m_count + t_count == 0:
         await message.reply_text("✅ Çevrilmesi gereken yeni içerik bulunamadı. Tüm içerikler zaten etiketlenmiş.")
         return

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri başlıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": m_count, "query": {TRANSLATED_STATUS_FIELD: {"$ne": TRANSLATED_STATUS_VALUE}}, "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "total": t_count, "query": {"$or": [{TRANSLATED_STATUS_FIELD: {"$ne": TRANSLATED_STATUS_VALUE}}, {f"seasons.episodes.{TRANSLATED_STATUS_FIELD}": {"$ne": TRANSLATED_STATUS_VALUE}}]}, "done": 0, "errors": 0}
    ]
    
    collections = [c for c in collections if c["total"] > 0]
    
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
            
            ids_cursor = col.find(c["query"], {"_id": 1})
            ids = [d["_id"] for d in ids_cursor]

            idx = 0
            
            while idx < len(ids):
                if stop_event.is_set():
                    break

                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}})) 
                
                if not batch_docs and batch_ids:
                    print(f"UYARI: {name} koleksiyonundan {len(batch_ids)} ID çekildi ancak dökümanlar bulunamadı. Atlanıyor.")
                    idx += len(batch_ids)
                    continue
                
                # KRİTİK ADIM: BSON nesnelerini temizle
                cleaned_docs = clean_bson_types(batch_docs)

                worker_data = {
                    "docs": cleaned_docs, # Temizlenmiş dökümanları gönder
                    "stop_flag_set": stop_event.is_set()
                }

                try:
                    loop = asyncio.get_event_loop()
                    future = loop.run_in_executor(pool, translate_batch_worker, worker_data)
                    
                    # 5 saniyelik timeout, worker'ın anında ölmesini yakalamak için
                    worker_output = await asyncio.wait_for(future, timeout=5.0) 
                    
                    results = worker_output["results"]
                    
                    if worker_output["error_details"]:
                        c["errors"] += len(worker_output["error_details"])
                        for error_detail in worker_output["error_details"]:
                            await log_error_to_telegram(
                                client, 
                                error_detail["media_type"], 
                                error_detail["id"], 
                                error_detail["error"]
                            )

                except TimeoutError:
                    error_msg = f"{name} worker 5 saniyede cevap vermedi. Serileştirme veya worker başlatma hatası olabilir."
                    print(f"KRİTİK HATA: {error_msg}")
                    await log_error_to_telegram(client, name, "TIMEOUT_ERROR", error_msg)
                    c["errors"] += len(batch_docs)
                    idx += len(batch_ids)
                    continue
                except (BrokenProcessPool, Exception) as e:
                    # En kritik hata yakalama noktası
                    error_trace = traceback.format_exc()
                    error_msg = f"Worker Görev Başlatma/Tamamlama Hatası: {e}"
                    print(f"KRİTİK HATA: {error_msg}\n{error_trace}")
                    
                    await log_error_to_telegram(client, name, "ASYNC_WORKER_ERROR", error_msg + "\n\nTraceback: " + error_trace)
                    
                    c["errors"] += len(batch_docs)
                    idx += len(batch_ids)
                    # Havuzu yeniden başlatmayı veya tamamen kapatmayı deneyin
                    pool.shutdown(wait=False)
                    pool = ProcessPoolExecutor(max_workers=workers)
                    await asyncio.sleep(1)
                    continue

                # SONUÇLARI VERİTABANINA YAZ (Toplu Yazma)
                update_requests = []
                # Başarılı sayılan dökümanları bulmak için hata ID'lerini kullan
                error_ids = {d.get('id') for d in worker_output.get("error_details", [])}
                
                processed_in_batch = set()

                for _id, upd in results:
                    processed_in_batch.add(_id)
                    if stop_event.is_set():
                        break
                    
                    if upd:
                        # Eğer ID MongoDB ObjectId ise, pymongo'ya göndermeden önce tekrar ObjectId'e dönüştürün
                        try:
                             final_id = ObjectId(_id)
                        except:
                             final_id = _id
                             
                        update_requests.append(
                            pymongo.UpdateOne({"_id": final_id}, {"$set": upd})
                        )
                
                # Batch'teki her döküman, hata listesinde yoksa başarılıdır
                for doc in batch_docs:
                    doc_id = str(doc.get("_id"))
                    if doc_id not in error_ids:
                        c["done"] += 1
                
                # Hata listesindeki dökümanlar için done sayısını geri al
                # Bu kısım, yukarıdaki logic ile zaten halledildi, sadece koruma amaçlı
                # c["done"] -= len(error_ids)

                if update_requests:
                    try:
                        col.bulk_write(update_requests, ordered=False)
                    except Exception as e:
                        print(f"Toplu DB Yazma Hatası: {e}")
                        # Toplu yazma hatasında, batch'teki tüm başarılı sayılanları geri al
                        c["errors"] += len(update_requests)
                        c["done"] -= len(update_requests)
                        await log_error_to_telegram(client, name, "BULK_WRITE_ERROR", str(e))

                # İndeks ilerletme
                idx += len(batch_ids)
                
                # İlerleme mesajı güncelleme... (önceki kodla aynı)
                if time.time() - last_update > update_interval or idx >= len(ids) or stop_event.is_set():
                    
                    text = ""
                    total_done = sum(c_item['done'] for c_item in collections)
                    total_all = sum(c_item['total'] for c_item in collections)
                    total_errors = sum(c_item['errors'] for c_item in collections)
                    remaining_all = total_all - total_done - total_errors

                    for c_item in collections:
                        text += (
                            f"📌 **{c_item['name']}**: {c_item['done']}/{c_item['total']}\n"
                            f"{progress_bar(c_item['done'], c_item['total'])}\n"
                            f"Hatalar: `{c_item['errors']}`\n\n"
                        )
                    
                    cpu = psutil.cpu_percent(interval=None)
                    ram_percent = psutil.virtual_memory().percent

                    elapsed_time = time.time() - start_time
                    
                    if total_done > 0 and elapsed_time > 0:
                        speed = total_done / elapsed_time 
                        eta_seconds = remaining_all / speed if remaining_all > 0 else 0
                    else:
                        eta_seconds = -1 

                    elapsed_time_str = format_time_custom(elapsed_time)
                    eta_str = format_time_custom(eta_seconds)

                    text += (
                        f" Süre: `{elapsed_time_str}` (`{eta_str}`)\n"
                        f" CPU: `{cpu}%` | RAM: `{ram_percent}%`"
                    )

                    try:
                        await start_msg.edit_text(
                            text,
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                        )
                    except Exception as e:
                        pass
                    
                    last_update = time.time()

    finally:
        pool.shutdown(wait=False)

    # ------------ SONUÇ EKRANI ------------
    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    errors_all = sum(c["errors"] for c in collections)
    remaining_all = total_all - done_all - errors_all

    total_time = round(time.time() - start_time)
    final_time_str = format_time_custom(total_time)

    final_text = "🎉 **Türkçe Çeviri Sonuçları**\n\n"
    for col_summary in collections:
        final_text += (
            f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n"
            f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
            f"Hatalar: `{col_summary['errors']}`\n\n"
        )

    final_text += (
        f"📊 **Genel Özet**\n"
        f"Toplam işlenen içerik: `{total_all}`\n"
        f"Başarılı çeviri: `{done_all - errors_all}`\n"
        f"Hatalı çeviri: `{errors_all}`\n"
        f"Kalan: `{remaining_all}`\n"
        f"Toplam süre: `{final_time_str}`"
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
