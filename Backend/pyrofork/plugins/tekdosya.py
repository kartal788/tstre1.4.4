import asyncio
import time
import re
import os
import importlib.util
import json
import math
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import psutil
from deep_translator import GoogleTranslator

# Pyrogram
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# Motor (Asenkron MongoDB İstemcisi)
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne # Senkron, sadece UpdateOne objesi için
from dotenv import load_dotenv

# Bu dosyanın dışarıdan import edildiğini varsayarak (Backend.helper.custom_filter)
# Eğer bu filtreye sahip değilseniz, "CustomFilters.owner" kısımlarını kendi owner filtresiyle değiştirin veya kaldırın.
from Backend.helper.custom_filter import CustomFilters 

# ------------ 1. YAPILANDIRMA VE VERİTABANI BAĞLANTISI ------------

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/" # İstatistik için disk kullanımı
bot_start_time = time.time()
flood_wait = 30
confirmation_wait = 120

# Global Durumlar
last_command_time = {}  # kullanıcı_id : zaman (vsil, vindir için)
pending_deletes = {}    # user_id: { "files": [...], "arg": ..., "time": ... } (vsil için)
awaiting_confirmation = {} # user_id -> asyncio.Task (sil için)
stop_event = asyncio.Event() # cevir, tur için

# ---------------- Config/Env Okuma ----------------
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

def read_config():
    """Config.env dosyasını okur."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return config

config = read_config()

def get_db_urls():
    """DATABASE URL'lerini config/env'den alır."""
    db_raw = getattr(config, "DATABASE", "") or os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()

if len(db_urls) < 2:
    # Bu durumda bot başlatılırken hata fırlatılır.
    # Ancak Pyrogram plugin'leri genellikle bu hatayı yutabilir.
    print("⚠️ İkinci DATABASE URL'si bulunamadı!")
    MONGO_URL = None
else:
    MONGO_URL = db_urls[1]
    BASE_URL = getattr(config, "BASE_URL", "") or os.getenv("BASE_URL", "")
    if not BASE_URL:
        print("⚠️ BASE_URL config veya env'de bulunamadı!")

# Asenkron MongoDB İstemcisi (Motor)
if MONGO_URL:
    motor_client = AsyncIOMotorClient(MONGO_URL)
    db = None
    movie_col = None
    series_col = None
else:
    motor_client = None


async def init_db_collections():
    """Veritabanı bağlantısını asenkron olarak başlatır."""
    global db, movie_col, series_col
    if not motor_client:
        return False
    try:
        db_names = await motor_client.list_database_names()
        if not db_names:
            print("Veritabanı bulunamadı.")
            return False
        db = motor_client[db_names[0]]
        movie_col = db["movie"]
        series_col = db["tv"]
        return True
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}")
        return False

# ------------ 2. YARDIMCI FONKSİYONLAR ------------

# --- Çeviri için İşlem Havuzu Fonksiyonları ---
# Bu kısımlar '/cevir' komutu için kullanılır ve Pyrogram'ın thread'ini bloklamamak adına ayrı bir süreçte çalışır.

def translate_text_safe(text, cache):
    """Deep Translator ile güvenli çeviri."""
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except Exception:
        tr = text # Hata durumunda orijinal metni döndür
    cache[text] = tr
    return tr

def translate_batch_worker(batch, stop_flag):
    """Batch çevirisi yapan işçi (Process Pool için)."""
    CACHE = {}
    results = []

    for doc in batch:
        if stop_flag.is_set():
            break

        _id = doc.get("_id")
        upd = {}
        
        # Açıklama çevirisi
        desc = doc.get("description")
        if desc and desc.strip() and desc.strip().lower() not in ["null", "none"]:
            upd["description"] = translate_text_safe(desc, CACHE)

        # Sezon / bölüm çevirisi
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if stop_flag.is_set():
                        break
                    # Başlık çevirisi
                    if "title" in ep and ep["title"] and ep["title"].strip() and ep["title"].strip().lower() not in ["null", "none"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    # Özet çevirisi
                    if "overview" in ep and ep["overview"] and ep["overview"].strip() and ep["overview"].strip().lower() not in ["null", "none"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
            if modified:
                upd["seasons"] = seasons

        results.append((_id, upd))

    return results

def progress_bar(current, total, bar_length=12):
    """İlerleme çubuğu metni."""
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

def dynamic_config():
    """Sistem durumuna göre worker ve batch ayarı yapar."""
    cpu_count = multiprocessing.cpu_count()
    cpu_percent = psutil.cpu_percent(interval=0.5)

    if cpu_percent < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent < 60:
        workers = max(1, cpu_count)
    else:
        workers = 1

    # Batch boyutu
    batch = 50 
    return workers, batch

# ---------------- Veri Çekme Fonksiyonları (Blocking) ----------------
# Bu fonksiyonlar senkron pymongo kullanmak yerine,
# /istatistik ve /vindir komutlarında asyncio.to_thread içinde çağrılacaktır.

def get_db_stats_and_genres_sync(url):
    """Senkron MongoClient kullanarak istatistik ve tür verilerini çeker."""
    # Sadece istatistikler ve türler için geçici senkron bağlantı
    from pymongo import MongoClient 
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0, 0.0, {}

    db = client[db_name_list[0]]

    total_movies = db["movie"].count_documents({})
    total_series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    max_storage_mb = 512
    storage_percent = round((storage_mb / max_storage_mb) * 100, 1)

    genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})

    # Aggregation işlemleri
    for doc in db["movie"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[doc["_id"]]["film"] = doc["count"]

    for doc in db["tv"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[doc["_id"]]["dizi"] = doc["count"]
        
    client.close()
    return total_movies, total_series, storage_mb, storage_percent, genre_stats

def get_system_status():
    """Sistem durumunu (CPU, RAM, Disk, Uptime) çeker."""
    cpu = round(psutil.cpu_percent(interval=1), 1)
    ram = round(psutil.virtual_memory().percent, 1)

    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}s {m}d {s}s"

    return cpu, ram, free_disk, free_percent, uptime

def export_collections_to_json_sync(url):
    """Senkron MongoClient ile koleksiyonları JSON'a çeker."""
    from pymongo import MongoClient
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return None

    db = client[db_name_list[0]]

    # _id hariç tüm dokümanlar
    movie_data = list(db["movie"].find({}, {"_id": 0}))
    tv_data = list(db["tv"].find({}, {"_id": 0}))
    
    client.close()
    return {"movie": movie_data, "tv": tv_data}

# ------------ 3. KOMUT HANDLER'LARI ------------

# --- /m3uindir Komutu (Senkron MongoDB Kullanıyor - İyileştirildi) ---
@Client.on_message(filters.command("m3uindir") & filters.private & CustomFilters.owner)
async def send_m3u_file(client, message: Message):
    if not MONGO_URL or not BASE_URL:
        await message.reply_text("⚠️ BASE_URL veya İkinci Veritabanı bulunamadı!")
        return
        
    start_msg = await message.reply_text("📝 filmlervediziler.m3u dosyası hazırlanıyor, lütfen bekleyin...")

    # Senkron MongoDB işlemlerini ayrı bir thread'de çalıştır
    def generate_m3u_content():
        # Senkron MongoClient sadece bu fonksiyonun scope'unda kullanılır
        from pymongo import MongoClient
        client_db_sync = MongoClient(MONGO_URL)
        db_name = client_db_sync.list_database_names()[0]
        db_sync = client_db_sync[db_name]
        
        m3u_lines = ["#EXTM3U\n"]

        # FILMLER
        for movie in db_sync["movie"].find({}):
            logo = movie.get("poster", "")
            telegram_files = movie.get("telegram", [])
            genres = movie.get("genres", [])

            for tg in telegram_files:
                file_id = tg.get("id")
                name = tg.get("name")
                if not file_id or not name:
                    continue

                url = f"{BASE_URL}/dl/{file_id}/video.mkv"

                year_match = re.search(r"\b(19\d{2}|20\d{2})\b", name)
                year_group = "Filmler"
                if year_match:
                    year = int(year_match.group(1))
                    if year < 1950: year_group = "1940’lar ve Öncesi Filmleri"
                    elif 1950 <= year <= 1959: year_group = "1950’ler Filmleri"
                    # ... (diğer yıl grupları)
                    elif 2020 <= year <= 2029: year_group = "2020’ler Filmleri"
                
                m3u_lines.append(
                    f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{year_group}",{name}\n'
                )
                m3u_lines.append(f"{url}\n")

                if genres:
                    for genre in genres:
                        genre_group = f"{genre} Filmleri"
                        m3u_lines.append(
                            f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre_group}",{name}\n'
                        )
                        m3u_lines.append(f"{url}\n")

        # DİZİLER
        for tv in db_sync["tv"].find({}):
            logo_tv = tv.get("poster", "")
            seasons = tv.get("seasons", [])

            for season in seasons:
                episodes = season.get("episodes", [])

                for ep in episodes:
                    logo = ep.get("episode_backdrop") or logo_tv
                    telegram_files = ep.get("telegram", [])

                    for tg in telegram_files:
                        file_id = tg.get("id")
                        name = tg.get("name")
                        if not file_id or not name:
                            continue

                        url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                        file_name_lower = name.lower()
                        group = "Diziler"

                        if "dsnp" in file_name_lower: group = "Disney Dizileri"
                        elif "nf" in file_name_lower: group = "Netflix Dizileri"
                        # ... (diğer platform kontrolleri)
                        elif "hbo" in file_name_lower or "hbomax" in file_name_lower or "blutv" in file_name_lower: group = "Hbo Dizileri"

                        m3u_lines.append(
                            f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{group}",{name}\n'
                        )
                        m3u_lines.append(f"{url}\n")

        client_db_sync.close()
        return "".join(m3u_lines)

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

# --- /istatistik Komutu (Senkron MongoDB Kullanıyor - İyileştirildi) ---
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    if not MONGO_URL:
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
        return

    try:
        # DB istatistiklerini ayrı bir thread'de çek (Blocking işlemi)
        total_movies, total_series, storage_mb, storage_percent, genre_stats = await asyncio.to_thread(
            get_db_stats_and_genres_sync, MONGO_URL
        )
        
        # Sistem durumunu çek (Blocking değil)
        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        genre_lines = []
        for genre, counts in sorted(genre_stats.items(), key=lambda x: x[0]):
            genre_lines.append(f"{genre:<12} | Film: {counts['film']:<3} | Dizi: {counts['dizi']:<3}")

        genre_text = "\n".join(genre_lines)

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies}\n"
            f"┠ Diziler: {total_series}\n"
            f"┖ Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"<b>Tür Bazlı:</b>\n"
            f"<pre>{genre_text}</pre>\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")

# --- /vindir Komutu (Senkron MongoDB Kullanıyor - İyileştirildi) ---
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

    try:
        # Blocking işlemi ayrı bir thread'de çalıştır
        combined_data = await asyncio.to_thread(export_collections_to_json_sync, MONGO_URL)
        
        if combined_data is None:
            await message.reply_text("⚠️ Koleksiyonlar boş veya bulunamadı.")
            return

        file_path = "/tmp/dizi_ve_film_veritabanı.json"

        # JSON yazarken datetime ve diğer serialize edilemeyen tipleri string yap
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📁 Film ve Dizi Koleksiyonları"
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")

# --- /sil Komutu (Asenkron Motor Kullanıyor - Düzeltildi) ---
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client, message):
    if not motor_client:
        await message.reply_text("⚠️ Veritabanı bağlantısı henüz kurulmadı.")
        return
        
    user_id = message.from_user.id
    
    # DB'yi başlat
    if not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı başlatılamadı.")
        return

    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n"
        "⏱ 60 saniye içinde cevap vermezsen işlem otomatik iptal edilir."
    )

    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id].cancel()

    async def timeout():
        await asyncio.sleep(60)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi otomatik olarak iptal edildi.")

    task = asyncio.create_task(timeout())
    awaiting_confirmation[user_id] = task

@Client.on_message(filters.private & CustomFilters.owner & filters.text & ~filters.command("sil"))
async def handle_confirmation(client, message):
    user_id = message.from_user.id
    if user_id not in awaiting_confirmation:
        return 

    text = message.text.strip().lower()
    
    # Timeout task'ını iptal et
    awaiting_confirmation[user_id].cancel()
    awaiting_confirmation.pop(user_id, None)

    if text == "evet":
        await message.reply_text("🗑️ Silme işlemi başlatılıyor...")
        
        # Asenkron Motor işlemleri
        movie_count = await movie_col.count_documents({})
        series_count = await series_col.count_documents({})
        
        await movie_col.delete_many({})
        await series_col.delete_many({})
        
        await message.reply_text(
            f"✅ Silme işlemi tamamlandı.\n\n"
            f"📌 Filmler silindi: {movie_count}\n"
            f"📌 Diziler silindi: {series_count}"
        )
    elif text == "hayır":
        await message.reply_text("❌ Silme işlemi iptal edildi.")

# --- /tur Komutu (Asenkron Motor Kullanıyor - Düzeltildi) ---
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message):
    if not motor_client:
        await message.reply_text("⚠️ Veritabanı bağlantısı henüz kurulmadı.")
        return
        
    if not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı başlatılamadı.")
        return
        
    stop_event.clear()
    
    start_msg = await message.reply_text(
        "🔄 Tür ve platform güncellemesi başlatıldı…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
    )
    
    genre_map = {
        "Action": "Aksiyon", "Film-Noir": "Kara Film", "Game-Show": "Oyun Gösterisi", "Short": "Kısa",
        "Sci-Fi": "Bilim Kurgu", "Sport": "Spor", "Adventure": "Macera", "Animation": "Animasyon",
        "Biography": "Biyografi", "Comedy": "Komedi", "Crime": "Suç", "Documentary": "Belgesel",
        "Drama": "Dram", "Family": "Aile", "News": "Haberler", "Fantasy": "Fantastik",
        "History": "Tarih", "Horror": "Korku", "Music": "Müzik", "Musical": "Müzikal",
        "Mystery": "Gizem", "Romance": "Romantik", "Science Fiction": "Bilim Kurgu",
        "TV Movie": "TV Filmi", "Thriller": "Gerilim", "War": "Savaş", "Western": "Vahşi Batı",
        "Action & Adventure": "Aksiyon ve Macera", "Kids": "Çocuklar", "Reality": "Gerçeklik",
        "Reality-TV": "Gerçeklik", "Sci-Fi & Fantasy": "Bilim Kurgu ve Fantazi", "Soap": "Pembe Dizi",
        "War & Politics": "Savaş ve Politika", "Bilim-Kurgu": "Bilim Kurgu",
        "Aksiyon & Macera": "Aksiyon ve Macera", "Savaş & Politik": "Savaş ve Politika",
        "Bilim Kurgu & Fantazi": "Bilim Kurgu ve Fantazi", "Talk": "Talk-Show"
    }

    platform_genre_map = {
        "MAX": "Max", "Hbomax": "Max", "TABİİ": "Tabii", "NF": "Netflix", "DSNP": "Disney",
        "Tod": "Tod", "Blutv": "Max", "Tv+": "Tv+", "Exxen": "Exxen",
        "Gain": "Gain", "HBO": "Max", "Tabii": "Tabii", "AMZN": "Amazon",
    }

    collections_data = [
        (movie_col, "Filmler"),
        (series_col, "Diziler")
    ]

    total_fixed = 0
    last_update = 0

    for col, name in collections_data:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1, "telegram": 1, "seasons": 1})
        bulk_ops = []

        # Cursor asenkron olarak döngüye alınmalı
        async for doc in docs_cursor:
            if stop_event.is_set():
                break

            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            # --- Tür güncellemesi ---
            new_genres = []
            for g in genres:
                mapped_genre = genre_map.get(g, g)
                if mapped_genre != g:
                    updated = True
                new_genres.append(mapped_genre)
            genres = list(set(new_genres)) # Tekrar edenleri kaldır

            # --- Platform güncellemesi (Filmler için) ---
            if name == "Filmler":
                for t in doc.get("telegram", []):
                    name_field = t.get("name", "").lower()
                    for key, genre_name in platform_genre_map.items():
                        if key.lower() in name_field and genre_name not in genres:
                            genres.append(genre_name)
                            updated = True
            
            # --- Platform güncellemesi (Diziler için - Sezon/Bölüm) ---
            if name == "Diziler":
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        for t in ep.get("telegram", []):
                            name_field = t.get("name", "").lower()
                            for key, genre_name in platform_genre_map.items():
                                if key.lower() in name_field and genre_name not in genres:
                                    genres.append(genre_name)
                                    updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": genres}}))
                total_fixed += 1

            if time.time() - last_update > 5:
                try:
                    await start_msg.edit_text(
                        f"{name}: Güncellenen kayıtlar: {total_fixed}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
                    )
                except:
                    pass
                last_update = time.time()

        if stop_event.is_set():
            break

        if bulk_ops:
            try:
                await col.bulk_write(bulk_ops) # Asenkron bulk_write
            except Exception as e:
                print(f"Bulk Write Hatası ({name}): {e}")

    final_text = (
        f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}" 
        if not stop_event.is_set() else f"❌ İşlem iptal edildi. Toplam değiştirilen kayıt: {total_fixed}"
    )
    try:
        await start_msg.edit_text(final_text, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# --- /cevir Komutu (Asenkron Motor & Process Pool Kullanıyor - Düzeltildi) ---
async def process_collection_parallel(collection, name, message):
    """Koleksiyonu paralel işlem havuzu kullanarak çevirir."""
    if not collection: return 0, 0, 0, 0
    
    loop = asyncio.get_event_loop()
    total = await collection.count_documents({})
    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    ids_cursor = collection.find({}, {"_id": 1})
    ids = [d["_id"] async for d in ids_cursor] # Asenkron cursor'dan ID'leri çek
    idx = 0

    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        if stop_event.is_set():
            break

        batch_ids = ids[idx: idx + batch_size]
        # Toplu dokümanları asenkron olarak çek
        batch_docs = [d async for d in collection.find({"_id": {"$in": batch_ids}})]
        if not batch_docs:
            break

        try:
            # Blocking çeviri işini Process Pool'da çalıştır
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
                    await collection.update_one({"_id": _id}, {"$set": upd}) # Asenkron update
                done += 1
            except Exception:
                errors += 1

        idx += len(batch_ids)

        # İlerleme güncellemesi
        elapsed = time.time() - start_time
        speed = done / elapsed if elapsed > 0 else 0
        remaining = total - done
        eta = remaining / speed if speed > 0 else float("inf")
        eta_str = time.strftime("%H:%M:%S", time.gmtime(eta)) if math.isfinite(eta) else "∞"

        cpu = psutil.cpu_percent(interval=None)
        ram_percent = psutil.virtual_memory().percent
        sys_info = f"CPU: {cpu}% | RAM: %{ram_percent}"

        if time.time() - last_update > 30 or idx >= len(ids):
            text = (
                f"{name}: {done}/{total}\n"
                f"{progress_bar(done, total)}\n\n"
                f"Kalan: {remaining}, Hatalar: {errors}\n"
                f"Süre: {eta_str}\n"
                f"{sys_info}"
            )
            try:
                await message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                )
            except Exception:
                pass
            last_update = time.time()

    pool.shutdown(wait=False)
    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    
    if not motor_client:
        await message.reply_text("⚠️ Veritabanı bağlantısı henüz kurulmadı.")
        return

    if not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı başlatılamadı.")
        return
        
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor.\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(
        movie_col, "Filmler", start_msg
    )

    series_total, series_done, series_errors, series_time = await process_collection_parallel(
        series_col, "Diziler", start_msg
    )

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)

    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours)}s{int(minutes)}d{int(seconds)}s"

    summary = (
        "🎉 Türkçe Çeviri Sonuçları\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {eta_str}\n"
    )
    try:
        await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass


# --- /vsil Komutu (Asenkron Motor Kullanıyor - Düzeltildi) ---

# Yardımcı fonksiyon: Senkron find işlemlerini asenkron motor'a taşır.
async def find_files_to_delete(db, arg):
    deleted_files = []
    
    movie_col_sync = db["movie"]
    tv_col_sync = db["tv"]

    if arg.isdigit():
        tmdb_id = int(arg)
        movie_docs = [doc async for doc in movie_col_sync.find({"tmdb_id": tmdb_id})]
        for doc in movie_docs:
            deleted_files += [t.get("name") for t in doc.get("telegram", [])]

        tv_docs = [doc async for doc in tv_col_sync.find({"tmdb_id": tmdb_id})]
        for doc in tv_docs:
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    deleted_files += [t.get("name") for t in episode.get("telegram", [])]

    elif arg.lower().startswith("tt"):
        imdb_id = arg
        movie_docs = [doc async for doc in movie_col_sync.find({"imdb_id": imdb_id})]
        for doc in movie_docs:
            deleted_files += [t.get("name") for t in doc.get("telegram", [])]

        tv_docs = [doc async for doc in tv_col_sync.find({"imdb_id": imdb_id})]
        for doc in tv_docs:
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    deleted_files += [t.get("name") for t in episode.get("telegram", [])]

    else:
        target = arg
        movie_docs = [doc async for doc in movie_col_sync.find({"$or":[{"telegram.id": target},{"telegram.name": target}]})]
        for doc in movie_docs:
            telegram_list = doc.get("telegram", [])
            match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
            deleted_files += [t.get("name") for t in match]

        tv_docs = [doc async for doc in tv_col_sync.find({})]
        for doc in tv_docs:
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    telegram_list = episode.get("telegram", [])
                    match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                    deleted_files += [t.get("name") for t in match]
                    
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

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n"
            "/vsil <telegram_id veya dosya_adı>\n"
            "/vsil <tmdb_id>\n"
            "/vsil tt<imdb_id>", quote=True)
        return

    arg = message.command[1]
    
    if not MONGO_URL or not await init_db_collections():
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı veya başlatılamadı.")
        return
    
    try:
        deleted_files = await find_files_to_delete(db, arg)

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        # --- ONAY MEKANİZMASI ---
        pending_deletes[user_id] = {
            "files": deleted_files,
            "arg": arg,
            "time": now
        }

        if len(deleted_files) > 10:
            file_path = f"/tmp/silinen_dosyalar_{int(time.time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(chat_id=message.chat.id, document=file_path,
                                       caption=f"⚠️ {len(deleted_files)} dosya silinecek.\nSilmek için 'evet', iptal için 'hayır' yazın. ⏳ {confirmation_wait} sn.")
        else:
            text = "\n".join(deleted_files)
            await message.reply_text(
                f"⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n"
                f"{text}\n\n"
                f"Silmek için **evet** yazın.\n"
                f"İptal için **hayır** yazın.\n"
                f"⏳ {confirmation_wait} saniye içinde cevap vermezseniz işlem iptal edilir.",
                quote=True
            )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)


# --- Onay Mesajlarını Dinleme (vsil için) ---
@Client.on_message(filters.private & CustomFilters.owner & ~filters.command(["vsil", "sil", "cevir", "tur", "vindir", "istatistik", "m3uindir"]))
async def confirm_delete_vsil(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()

    if user_id not in pending_deletes:
        # Sil komutu onayı bekleniyorsa sil.py'nin handler'ı işler.
        # İki handler'ın çakışmaması için burada diğer komutları filtreledik.
        return

    data = pending_deletes[user_id]

    if now - data["time"] > confirmation_wait:
        del pending_deletes[user_id]
        await message.reply_text(f"⏳ Süre doldu, silme işlemi iptal edildi.")
        return

    text = message.text.lower()

    if text == "hayır":
        del pending_deletes[user_id]
        await message.reply_text("❌ Silme işlemi iptal edildi.")
        return

    if text != "evet":
        await message.reply_text("⚠️ Lütfen 'evet' veya 'hayır' yazın.")
        return

    arg = data["arg"]
    
    # DB'yi başlat
    if not await init_db_collections():
        await message.reply_text("⚠️ Veritabanı başlatılamadı.")
        del pending_deletes[user_id]
        return

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
            
            # Filmler
            movie_docs = [doc async for doc in movie_col.find({"$or":[{"telegram.id": target},{"telegram.name": target}]})]
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                new_telegram = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
                
                if not new_telegram:
                    await movie_col.delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new_telegram
                    await movie_col.replace_one({"_id": doc["_id"]}, doc)
            
            # Diziler
            tv_docs = [doc async for doc in series_col.find({})]
            for doc in tv_docs:
                modified = False
                
                seasons_to_remove = []
                for season in doc.get("seasons", []):
                    episodes_to_remove = []
                    for episode in season.get("episodes", []):
                        telegram_list = episode.get("telegram", [])
                        match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                        if match:
                            new_telegram = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
                            if new_telegram:
                                episode["telegram"] = new_telegram
                            else:
                                episodes_to_remove.append(episode)
                            modified = True
                            
                    for ep in episodes_to_remove:
                        season["episodes"].remove(ep)
                        
                    if not season["episodes"]:
                        seasons_to_remove.append(season)
                        
                for s in seasons_to_remove:
                    doc["seasons"].remove(s)
                    
                if modified:
                    await series_col.replace_one({"_id": doc["_id"]}, doc)


        del pending_deletes[user_id]
        await message.reply_text("✅ Dosyalar başarıyla silindi.")

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")

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
