import asyncio
import time
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

# Kütüphane İçe Aktarımları
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
import pymongo
from deep_translator import GoogleTranslator
import psutil

# ------------ ÖZEL FİLTRE İÇE AKTARIMI ------------
# Botunuzun özel filtre yapısını kullanır.
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

# ------------ DATABASE Bağlantısı (InvalidURI Hata Giderme) ------------
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
        # Çeviri API hatası durumunda orijinal metni döndür
        tr = text
    cache[text] = tr
    return tr

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

# ------------ Worker: batch çevirici (Güçlendirilmiş Hata Yönetimi) ------------
def translate_batch_worker(batch_data):
    """
    Çoklu süreçte çalıştırılacak işçi fonksiyonu.
    Her döküman için ayrı hata yakalama bloğu eklenmiştir.
    """
    batch_docs = batch_data["docs"]
    stop_flag_set = batch_data["stop_flag_set"]
    
    if stop_flag_set:
        return {"results": [], "error_ids": []}

    CACHE = {}
    results = []
    error_ids = []

    for doc in batch_docs:
        if stop_flag_set:
            break

        _id = doc.get("_id")
        upd = {}
        needs_update = False
        
        # Döküman bazında hata yönetimi
        try:
            # 1. Film Çevirisi VEYA Dizi Ana Açıklaması
            if doc.get(TRANSLATED_STATUS_FIELD) != TRANSLATED_STATUS_VALUE:
                desc = doc.get("description")
                if desc:
                    upd["description"] = translate_text_safe(desc, CACHE)
                    needs_update = True
            
            # 2. Sezon/Bölüm Çevirisi (Diziler için)
            seasons = doc.get("seasons")
            is_tv_show = doc.get("media_type") == "tv"
            
            if seasons and is_tv_show and isinstance(seasons, list):
                modified = False
                # Sezon döngüsü
                for season in seasons:
                    eps = season.get("episodes", []) or []
                    # Bölüm döngüsü
                    for ep in eps:
                        if stop_flag_set:
                            break
                        
                        # Sadece çevrilmemişse işle
                        if ep.get(TRANSLATED_STATUS_FIELD) != TRANSLATED_STATUS_VALUE:
                            
                            # Başlık ve Özet çevirisi
                            if "title" in ep and ep["title"]:
                                ep["title"] = translate_text_safe(ep["title"], CACHE)
                                modified = True
                            if "overview" in ep and ep["overview"]:
                                ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                                modified = True
                                
                            if modified:
                                ep[TRANSLATED_STATUS_FIELD] = TRANSLATED_STATUS_VALUE
                                
                if modified:
                    # Sezonlar listesi tamamen güncellendi
                    upd["seasons"] = seasons
                    needs_update = True

            if doc.get(TRANSLATED_STATUS_FIELD) != TRANSLATED_STATUS_VALUE and needs_update:
                upd[TRANSLATED_STATUS_FIELD] = TRANSLATED_STATUS_VALUE

            if needs_update:
                results.append((_id, upd))
            
        except Exception as e:
            # İşleme hatası durumunda (örneğin döküman yapısı beklenenden farklıysa)
            print(f"[{doc.get('media_type', 'unknown')}] {_id} dökümanında işleme hatası: {e}")
            error_ids.append(_id)
            continue

    return {"results": results, "error_ids": error_ids}

# ------------ Yardımcı Fonksiyon: Çevrilecek Sayıyı Hesapla ------------
async def get_translation_count():
    movie_count = movie_col.count_documents({TRANSLATED_STATUS_FIELD: {"$ne": TRANSLATED_STATUS_VALUE}})
    
    # Diziler için, en az bir çevrilmemiş bölümü olan ana belgeleri bul
    series_count = series_col.aggregate([
        {"$unwind": "$seasons"},
        {"$unwind": "$seasons.episodes"},
        {"$match": {f"seasons.episodes.{TRANSLATED_STATUS_FIELD}": {"$ne": TRANSLATED_STATUS_VALUE}}},
        {"$group": {"_id": "$_id"}},
        {"$count": "count"}
    ])
    
    series_to_translate_count = next(series_count, {"count": 0})["count"]

    return movie_count, series_to_translate_count

# ------------ Yardımcı Fonksiyon: Toplu Durum Güncelleme (WriteError Giderildi) ------------
async def bulk_status_update(collection, action):
    """Koleksiyon tipine göre (movie/tv) farklı güncelleme komutları kullanır."""
    
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

# ------------ /cevir Komutu (Ana İşleyici) ------------
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

# ------------ Ana Çeviri İşlemi ------------
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
                
                # Çekilen döküman kontrolü (0/1 hatası için kritik)
                if not batch_docs and batch_ids:
                    print(f"UYARI: {name} koleksiyonundan {len(batch_ids)} ID çekildi ancak dökümanlar bulunamadı. Atlanıyor.")
                    idx += len(batch_ids)
                    continue

                worker_data = {
                    "docs": batch_docs,
                    "stop_flag_set": stop_event.is_set()
                }

                try:
                    loop = asyncio.get_event_loop()
                    future = loop.run_in_executor(pool, translate_batch_worker, worker_data)
                    worker_output = await future
                    results = worker_output["results"]
                    # Worker'da işlenemeyen (hata veren) dökümanları hata sayacına ekle
                    c["errors"] += len(worker_output["error_ids"])
                except Exception as e:
                    print(f"Worker Görev Başlatma/Tamamlama Hatası ({name}): {e}")
                    c["errors"] += len(batch_docs)
                    idx += len(batch_ids)
                    await asyncio.sleep(1)
                    continue

                # SONUÇLARI VERİTABANINA YAZ (Toplu Yazma)
                update_requests = []
                for _id, upd in results:
                    if stop_event.is_set():
                        break
                    
                    if upd:
                        update_requests.append(
                            pymongo.UpdateOne({"_id": _id}, {"$set": upd})
                        )
                    # Başarıyla işlendi (güncelleme olsa da olmasa da)
                    c["done"] += 1 

                if update_requests:
                    try:
                        col.bulk_write(update_requests, ordered=False)
                    except Exception as e:
                        print(f"Toplu DB Yazma Hatası: {e}")
                        c["errors"] += len(update_requests)
                        c["done"] -= len(update_requests) 

                idx += len(batch_ids)
                
                if time.time() - last_update > update_interval or idx >= len(ids) or stop_event.is_set():
                    
                    text = ""
                    total_done = sum(c_item['done'] for c_item in collections)
                    total_all = sum(c_item['total'] for c_item in collections)
                    total_errors = sum(c_item['errors'] for c_item in collections)
                    remaining_all = total_all - total_done

                    for c_item in collections:
                        remaining_current = max(0, c_item['total'] - c_item['done'])
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
                        eta_seconds = remaining_all / speed
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
    remaining_all = total_all - done_all

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
