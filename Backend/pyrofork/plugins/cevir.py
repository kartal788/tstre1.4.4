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

# NOT: 'Backend.helper.custom_filter' modülüne erişimim olmadığı için,
# 'CustomFilters.owner' yerine basitleştirilmiş bir owner ID kontrolü kullanacağım.
# Gerçek ortamınızda 'CustomFilters.owner' kullanımına devam edin.
OWNER_ID = int(os.getenv("OWNER_ID", 12345)) # Ortam değişkeni veya varsayılan ID

# GLOBAL STOP EVENT
stop_event = asyncio.Event()

# ------------ DATABASE Bağlantısı ------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    # Bu kontrolü basitleştiriyoruz, ikinci URL'ye odaklanalım
    MONGO_URL = db_urls[0] # İkinci URL yoksa ilkini kullan
else:
    MONGO_URL = db_urls[1] # İkinci URL'yi kullan

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
    cpu_percent = psutil.cpu_percent(interval=0.5)

    # Worker sayısı: CPU'yu aşırı yüklememek için limitlendi
    workers = max(1, min(cpu_count, 4)) 

    # Batch boyutu: Daha sık güncelleme için genel olarak küçültüldü
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
        # Her worker kendi çeviricisini yaratmalı
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
    # Yüzdeyi 100.00'ü geçmeyecek şekilde sınırla
    percent_display = min(percent, 100.00)
    return f"[{bar}] {percent_display:.2f}%"

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
        # Döngü içinde stop kontrolü
        if stop_flag_set:
            break

        _id = doc.get("_id")
        upd = {}

        # 1. Açıklama Çevirisi
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        # 2. Sezon/Bölüm Çevirisi (Diziler için)
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if stop_flag_set:
                        break
                    
                    # Başlık ve Özet çevirisi
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
# Owner filtresinin kodunuzdaki gibi tanımlı olduğunu varsayıyorum.
# Eğer tanımlı değilse, Pyrogram filters ile değiştirilmelidir.
# @Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner) 
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID)) 
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    
    # Eğer önceden başlatılmış bir işlem varsa uyarı ver
    if stop_event.is_set():
        await message.reply_text("⛔ Şu anda devam eden bir işlem var. Lütfen bitmesini veya tamamen iptal olmasını bekleyin.")
        return
        
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.\n\n_İlk çeviri toplu işi (batch) tamamlanana kadar ilerleme %0.00 görünebilir._",
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
            # İşlenecek belge yoksa atla
            c["done"] = c["total"] 

    start_time = time.time()
    last_update = 0
    update_interval = 4 # Güncelleme aralığı 4 saniyeye düşürüldü

    # ProcessPoolExecutor'ı koleksiyonlar döngüsünün dışında başlat
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

                # BATCH İŞLEME
                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))

                # Worker'a gönderilecek veri: Belgeler ve durdurma durumu
                worker_data = {
                    "docs": batch_docs,
                    "stop_flag_set": stop_event.is_set()
                }

                try:
                    loop = asyncio.get_event_loop()
                    future = loop.run_in_executor(pool, translate_batch_worker, worker_data)
                    # Worker'ın bitmesini bekle
                    results = await future 
                except Exception as e:
                    # Worker hatası yakalandı
                    print(f"Worker Hatası ({name}): {e}")
                    errors += len(batch_docs)
                    idx += len(batch_ids)
                    # Hata durumunda bile güncelleme yapıp beklemeye devam et
                    c["errors"] = errors
                    c["done"] = done
                    await asyncio.sleep(1)
                    continue

                # SONUÇLARI VERİTABANINA YAZ
                for _id, upd in results:
                    if stop_event.is_set():
                        break
                    
                    try:
                        if upd:
                            # Sadece bir güncelleme varsa yaz
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

                    remaining_all = total_all - total_done
                    elapsed_time = time.time() - start_time

                    # ANLIK SÜRE GÜNCELLEMESİ (Tam sayıya yuvarlandı)
                    # Burada sadece saniye gösteriliyor (önceki isteğe göre).
                    text += (
                        f" Süre: `{int(elapsed_time)}` sn | Kalan: `{remaining_all}`\n"
                        f" CPU: `{cpu}%` | RAM: `{ram_percent}%`"
                    )

                    try:
                        await start_msg.edit_text(
                            text,
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                        )
                    except Exception as e:
                        # Pyrogram limit hataları bu blokta yakalanır
                        print(f"Telegram Mesaj Güncelleme Hatası: {e}")
                        pass
                    
                    last_update = time.time()

    finally:
        # Hata olsa bile havuzu kapat
        pool.shutdown(wait=False)

    # ------------ SONUÇ EKRANI ------------
    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    errors_all = sum(c["errors"] for c in collections)
    remaining_all = total_all - done_all

    total_time = round(time.time() - start_time)
    
    # İSTENEN SÜRE FORMATI DÜZENLEMESİ: GGs Dkm Ss (G: Gün, Dk: Dakika, S: Saniye)
    # total_time saniyedir.
    days = total_time // (24 * 3600)
    total_time %= (24 * 3600)
    hours = total_time // 3600
    total_time %= 3600
    minutes = total_time // 60
    seconds = total_time % 60
    
    # Format: 00h 00m 00s (Saat, Dakika, Saniye) olarak düzenlenmiştir.
    # Eğer isterseniz 'd' (dakika) yerine 'm' (minute) kullanabiliriz.
    # Örnek: '0h 10m 00s'
    # İstenen formata en yakın ve mantıklı format (Saat:Dakika:Saniye)
    eta_str = f"{int(hours):02}s {int(minutes):02}d {int(seconds):02}s" # 00s 00d 00s şeklinde görünmesi için

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
        f"Başarılı    : `{done_all - errors_all}`\n"
        f"Hatalı      : `{errors_all}`\n"
        f"Kalan       : `{remaining_all}`\n"
        f"Toplam süre  : `{eta_str}`"
    )

    try:
        await start_msg.edit_text(final_text, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        # Sonuç ekranı güncellenemezse yut
        pass

# ------------ Callback query handler ------------
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data == "stop":
        await handle_stop(query)
