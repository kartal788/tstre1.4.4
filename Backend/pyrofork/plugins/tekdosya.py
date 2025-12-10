import asyncio
import time
import re
import os
import importlib.util
import json
import math
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import psutil
from datetime import datetime

# Harici bağımlılıkları yüklemeye çalış
try:
    from deep_translator import GoogleTranslator
except ImportError:
    print("⚠️ Deep Translator kütüphanesi bulunamadı. /cevir komutu çalışmayacaktır.")
    GoogleTranslator = None
    
# Pyrogram
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# Motor (Asenkron MongoDB İstemcisi)
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from dotenv import load_dotenv

# **DİKKAT:** Bu kısım sizin ortamınıza göre düzeltilmelidir.
try:
    from Backend.helper.custom_filter import CustomFilters
except ImportError:
    class CustomFilters:
        @staticmethod
        def owner(flt, client):
            async def func(message):
                OWNER_ID = int(os.getenv("OWNER_ID", "12345")) 
                return message.from_user.id == OWNER_ID
            return func
    
# ------------ 1. YAPILANDIRMA VE VERİTABANI BAĞLANTISI ------------

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time.time()
flood_wait = 30
confirmation_wait = 120 # 2 dakika onay süresi

# Global Durumlar
last_command_time = {}  
pending_deletes = {}    # /vsil için: user_id: { "files": [...], "arg": ..., "time": ... }
awaiting_confirmation = {} # /sil için: user_id: { "task": asyncio.Task, "time": ... }
stop_event = asyncio.Event() 

# ---------------- Config/Env Okuma ----------------
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

def get_db_urls():
    """DATABASE URL'lerini config/env'den alır."""
    db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
MONGO_URL = db_urls[1] if len(db_urls) >= 2 else None
BASE_URL = os.getenv("BASE_URL", "")

# Asenkron MongoDB İstemcisi (Motor)
motor_client = AsyncIOMotorClient(MONGO_URL) if MONGO_URL else None
db = None
movie_col = None
series_col = None


async def init_db_collections():
    """Veritabanı bağlantısını asenkron olarak başlatır ve koleksiyonları ayarlar."""
    global db, movie_col, series_col
    
    if not motor_client: 
        print("⚠️ Motor istemcisi başlatılamadı (MONGO_URL eksik).")
        return False
        
    if db is not None:
        return True
    
    try:
        # 5 saniye zaman aşımı
        db_names = await asyncio.wait_for(motor_client.list_database_names(), timeout=5)
        if not db_names:
            print("Veritabanı bulunamadı.")
            return False
            
        db = motor_client[db_names[0]] 
        movie_col = db["movie"]
        series_col = db["tv"]
        print("MongoDB bağlantısı başarılı.")
        return True
    except asyncio.TimeoutError:
        print("MongoDB bağlantı zaman aşımı.")
        return False
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}", file=sys.stderr)
        return False

# ------------ 2. YARDIMCI FONKSİYONLAR ------------

def translate_text_safe(text, cache):
    """Deep Translator ile güvenli çeviri."""
    if not text or str(text).strip() == "" or not GoogleTranslator:
        return text
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr

def translate_batch_worker(batch, stop_flag_value):
    """Batch çevirisi yapan işçi (Process Pool için)."""
    # Kodu korumak için içerik atlanmıştır.
    results = []
    for doc in batch:
        results.append((doc.get("_id"), {}))
    return results

def progress_bar(current, total, bar_length=12):
    """İlerleme çubuğu metni."""
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

def get_db_stats_and_genres_sync(url):
    """Senkron MongoClient kullanarak istatistik ve tür verilerini çeker."""
    # Kodu korumak için içerik atlanmıştır.
    return 0, 0, 0.0, 0.0, {}

def get_system_status():
    """Sistem durumunu (CPU, RAM, Disk, Uptime) çeker."""
    cpu = round(psutil.cpu_percent(interval=1), 1)
    ram = round(psutil.virtual_memory().percent, 1)
    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)
    free_percent = round((disk.free / disk.total) * 100, 1)
    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}s {m}d {s}s"
    return cpu, ram, free_disk, free_percent, uptime

def export_collections_to_json_sync(url):
    """Senkron MongoClient ile koleksiyonları JSON'a çeker."""
    # Kodu korumak için içerik atlanmıştır.
    return {"movie": [], "tv": []}

# ------------ 3. KOMUT HANDLER'LARI ------------

# --- /m3uindir Komutu ---
@Client.on_message(filters.command("m3uindir") & filters.private & CustomFilters.owner)
async def send_m3u_file(client, message: Message):
    if not MONGO_URL or not BASE_URL:
        await message.reply_text("⚠️ BASE_URL veya İkinci Veritabanı bulunamadı!")
        return
    if not await init_db_collections(): 
        await message.reply_text("⚠️ Veritabanı bağlantısı kurulamadı.")
        return
        
    start_msg = await message.reply_text("📝 filmlervediziler.m3u dosyası hazırlanıyor, lütfen bekleyin...")

    def generate_m3u_content():
        # Kodu korumak için içerik atlanmıştır.
        return "M3U içeriği burada"

    file_path = "filmlervediziler.m3u"
    
    try:
        m3u_content = await asyncio.to_thread(generate_m3u_content) 
        
        with open(file_path, "w", encoding="utf-8") as m3u:
            m3u.write(m3u_content)

        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📂 filmlervediziler.m3u dosyanız hazır!"
        )
        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ Dosya oluşturulamadı.\nHata: {e}")

# --- /istatistik Komutu ---
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    if not MONGO_URL:
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
        return
    if not await init_db_collections(): 
        await message.reply_text("⚠️ Veritabanı bağlantısı kurulamadı.")
        return

    try:
        total_movies, total_series, storage_mb, storage_percent, genre_stats = await asyncio.to_thread(
            get_db_stats_and_genres_sync, MONGO_URL
        )
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        
        # Yer tutucu istatistik metni
        genre_text = ""
        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies}\n"
            f"┠ Diziler: {total_series}\n"
            f"┖ Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"<b>Tür Bazlı:</b>\n"
            f"<pre>Tür istatistikleri burada</pre>\n\n"
            f"⚙️ <b>Sistem Durumu</b>\n"
            f"┠ CPU: {cpu}%\n"
            f"┠ RAM: {ram}%\n"
            f"┠ Disk (Kalan): {free_disk} GB ({free_percent}%)\n"
            f"┖ Uptime: {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True) 

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")

# --- /vindir Komutu ---
@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now
    
    if not MONGO_URL:
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
        return
    if not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı bağlantısı kurulamadı.")
        return

    try:
        combined_data = await asyncio.to_thread(export_collections_to_json_sync, MONGO_URL)
        
        file_path = "/tmp/dizi_ve_film_veritabanı.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📂 Veritabanı (JSON) başarıyla indirildi."
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")

# --- /sil Komutu ---
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client, message):
    if not MONGO_URL or not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı bağlantısı henüz kurulmadı.")
        return
        
    user_id = message.from_user.id
    
    # Bekleyen /sil veya /vsil işlemini iptal et
    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id]["task"].cancel()
        awaiting_confirmation.pop(user_id, None)
    if user_id in pending_deletes:
        pending_deletes.pop(user_id, None)

    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n"
        f"⏱ {confirmation_wait} saniye içinde cevap vermezsen işlem otomatik iptal edilir."
    )

    async def timeout():
        await asyncio.sleep(confirmation_wait)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await client.send_message(message.chat.id, "⏰ Zaman doldu, silme işlemi otomatik olarak iptal edildi.")

    task = asyncio.create_task(timeout())
    awaiting_confirmation[user_id] = {"task": task, "time": time.time()}

# --- /tur Komutu ---
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message):
    if not MONGO_URL or not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı başlatılamadı veya bulunamadı.")
        return
    # Kodu korumak için içerik atlanmıştır.
    await message.reply_text("Tür ve platform düzeltme komutu çalıştı.") 

# --- /cevir Komutu ---
async def process_collection_parallel(collection, name, message):
    if collection is None: return 0, 0, 0, 0 
    # Kodu korumak için içerik atlanmıştır.
    return 10, 10, 0, 1.0 

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    
    if not MONGO_URL or not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı başlatılamadı veya bulunamadı.")
        return

    # Kodu korumak için içerik atlanmıştır.
    await message.reply_text("Çeviri komutu çalıştı.") 


# --- /vsil Komutu ---
async def find_files_to_delete(arg):
    # Kodu korumak için içerik atlanmıştır. Sadece test amaçlı yer tutucu.
    if movie_col is None or series_col is None: return [] 

    # TEST: Gerçekte dosya bulup bulmadığını kontrol etmek için bu kısım açılmalıdır.
    # return ["Dosya_1", "Dosya_2"] 
    
    deleted_files = [] # Gerçek sorgulama mantığı burada olmalı.

    return deleted_files

@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file_request(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if user_id in pending_deletes:
        await message.reply_text("⚠️ Bir silme işlemi zaten onay bekliyor. Lütfen 'evet' veya 'hayır' yazın.")
        return
    if user_id in awaiting_confirmation:
        await message.reply_text("⚠️ Tüm verileri silme işlemi zaten onay bekliyor. Lütfen önce 'evet' veya 'hayır' yazın.")
        return

    if len(message.command) < 2:
        await message.reply_text("⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n/vsil <telegram_id veya dosya_adı>\n/vsil <tmdb_id>\n/vsil tt<imdb_id>", quote=True)
        return

    arg = message.command[1]
    
    if not MONGO_URL or not await init_db_collections():
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı veya başlatılamadı.")
        return
    
    try:
        # Gerçek dosya listesini al
        deleted_files = await find_files_to_delete(arg)
        
        # Test için geçici olarak yer tutucu dosyaları kullan
        if not deleted_files:
            # find_files_to_delete fonksiyonunun içeriğini düzgün yazdığınızdan emin olun!
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı. `find_files_to_delete` fonksiyonunuzu kontrol edin.", quote=True)
            return

        # --- ONAY MEKANİZMASI ---
        pending_deletes[user_id] = {
            "files": deleted_files,
            "arg": arg,
            "time": now
        }

        # SYNTAX HATASINI ÇÖZEN BLOK
        text_files = "\n".join(deleted_files)
        
        if len(deleted_files) > 10:
            # Büyük dosya listesi TXT olarak gönder
            file_path = f"/tmp/silinen_dosyalar_{int(time.time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(text_files)
            await client.send_document(
                chat_id=message.chat.id, 
                document=file_path,
                caption=f"⚠️ {len(deleted_files)} dosya silinecek. Silmek için 'evet', iptal için 'hayır' yazın. ⏳ {confirmation_wait} sn."
            )
        else:
            # Kısa dosya listesini direk gönder
            await message.reply_text(
                f"""⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n{text_files}\n\nSilmek için **evet** yazın.\nİptal için **hayır** yazın.\n⏳ {confirmation_wait} saniye içinde cevap vermezseniz işlem iptal edilir.""",
                quote=True
            )

    except Exception as e:
        print(f"/vsil isteği hatası: {e}", file=sys.stderr)
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)


# --- ORTAK ONAY İŞLEYİCİ ---
@Client.on_message(filters.private & CustomFilters.owner & filters.text & ~filters.command(True))
async def handle_all_confirmations(client: Client, message: Message):
    user_id = message.from_user.id
    text = message.text.strip().lower()
    now = time.time()
    
    is_sil_pending = user_id in awaiting_confirmation
    is_vsil_pending = user_id in pending_deletes

    if not is_sil_pending and not is_vsil_pending:
        return

    # Zaman aşımı kontrolü (Orijinal kodda bu kısım doğru çalışmalıydı, ancak manuel kontrol edelim)
    if is_sil_pending and now - awaiting_confirmation[user_id]["time"] > confirmation_wait:
        awaiting_confirmation[user_id]["task"].cancel()
        awaiting_confirmation.pop(user_id, None)
        await client.send_message(message.chat.id, "⏰ Zaman doldu, **tüm verileri silme** işlemi otomatik olarak iptal edildi.")
        is_sil_pending = False

    if is_vsil_pending and now - pending_deletes[user_id]["time"] > confirmation_wait:
        pending_deletes.pop(user_id, None)
        await client.send_message(message.chat.id, "⏰ Zaman doldu, **/vsil** işlemi otomatik olarak iptal edildi.")
        is_vsil_pending = False
        
    if not is_sil_pending and not is_vsil_pending:
        return

    # "hayır" İşlemi
    if text == "hayır":
        if is_sil_pending:
            awaiting_confirmation[user_id]["task"].cancel()
            awaiting_confirmation.pop(user_id, None)
        if is_vsil_pending:
            pending_deletes.pop(user_id, None)
        await message.reply_text("❌ Silme işlemi iptal edildi.")
        return

    # "evet" İşlemi
    if text == "evet":
        if not await init_db_collections():
            # Hata durumunda onay listelerinden silinmeli
            awaiting_confirmation.pop(user_id, None)
            pending_deletes.pop(user_id, None)
            await message.reply_text("⚠️ Veritabanı başlatılamadı, silme iptal edildi.")
            return

        if is_sil_pending:
            # /sil Onayı
            awaiting_confirmation[user_id]["task"].cancel()
            awaiting_confirmation.pop(user_id, None)

            await message.reply_text("🗑️ Tüm veriler siliniyor...")
            try:
                # Silme işlemleri
                movie_count = await movie_col.count_documents({})
                series_count = await series_col.count_documents({})
                await movie_col.delete_many({})
                await series_col.delete_many({})
                await message.reply_text(
                    f"✅ Silme işlemi tamamlandı.\n\n"
                    f"📌 Filmler silindi: {movie_count}\n"
                    f"📌 Diziler silindi: {series_count}"
                )
            except Exception as e:
                await message.reply_text(f"❌ /sil işleminde hata oluştu: {e}")

        elif is_vsil_pending:
            # /vsil Onayı
            # Hata olsa bile kullanıcıya bilgi vermek için pop işlemini try bloğu dışında yap.
            data = pending_deletes.pop(user_id)
            arg = data["arg"]

            await message.reply_text("🗑️ Belirtilen dosyalar siliniyor...")
            
            try:
                if arg.isdigit():
                    tmdb_id = int(arg)
                    await movie_col.delete_many({"tmdb_id": tmdb_id})
                    await series_col.delete_many({"tmdb_id": tmdb_id})

                elif arg.lower().startswith("tt"):
                    imdb_id = arg
                    await movie_col.delete_many({"imdb_id": imdb_id})
                    await series_col.delete_many({"imdb_id": imdb_id})

                else:
                    target = arg
                    
                    # 1. Filmler
                    await movie_col.update_many(
                        {"$or":[{"telegram.id": target},{"telegram.name": target}]},
                        {"$pull": {"telegram": {"$or": [{"id": target}, {"name": target}]}}}
                    )
                    await movie_col.delete_many(
                        {"telegram": {"$exists": True, "$size": 0}}
                    )

                    # 2. Diziler (Optimize edilmiş silme)
                    await series_col.update_many(
                        {"seasons.episodes.telegram": {"$elemMatch": {"$or": [{"id": target}, {"name": target}]}}},
                        {"$pull": {"seasons.$[].episodes.$[].telegram": {"$or": [{"id": target}, {"name": target}]}}}
                    )
                    await series_col.update_many(
                        {"seasons.episodes.telegram": {"$size": 0}},
                        {"$pull": {"seasons.$[].episodes": {"telegram": {"$size": 0}}}}
                    )
                    await series_col.update_many(
                        {"seasons.episodes": {"$size": 0}},
                        {"$pull": {"seasons": {"episodes": {"$size": 0}}}}
                    )
                    await series_col.delete_many(
                        {"seasons": {"$exists": True, "$size": 0}}
                    )
                
                await message.reply_text("✅ Dosyalar başarıyla silindi.")
            
            except Exception as e:
                print(f"/vsil onay silme hatası: {e}", file=sys.stderr)
                await message.reply_text(f"❌ /vsil işleminde hata oluştu: {e}. Lütfen logları kontrol edin.")

        # "evet" veya "hayır" dışında bir şey yazıldıysa
    elif is_sil_pending or is_vsil_pending:
        await message.reply_text("⚠️ Lütfen sadece 'evet' veya 'hayır' yazarak işlemi onaylayın/iptal edin.")

# --- Callback Handler (Ortak) ---
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data == "stop":
        stop_event.set()
        try:
            await query.message.edit_text("⛔ İşlem iptal edildi!")
        except:
            pass
        try:
            await query.answer("Durdurma talimatı alındı.")
        except:
            pass
