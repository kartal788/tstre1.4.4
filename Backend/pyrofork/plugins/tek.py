import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, UpdateOne
from collections import defaultdict
import psutil
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from deep_translator import GoogleTranslator
import os

# ---------------- CONFIG ----------------
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
stop_event = asyncio.Event()
DOWNLOAD_DIR = "/"

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
MONGO_URL = db_urls[1] if len(db_urls) > 1 else db_urls[0]

client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]
movie_col = db["movie"]
series_col = db["tv"]

bot_start_time = time.time()

# ---------------- UTILS ----------------
def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {min(percent,100):.2f}%"

def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"
    total_seconds = int(total_seconds)
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text(
            "⛔ İşlem **iptal edildi**!",
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# ---------------- TRANSLATE WORKER (güncellenmiş) ----------------
def translate_batch_worker(batch_data):
    """
    Verilen batch belgelerini çevirir ve sonuçları döndürür.
    Diziler için tüm bölümleri kontrol eder ve günceller.
    """
    batch_docs = batch_data["docs"]
    CACHE = {}
    results = []
    errors = []
    translated_episode_count = 0

    for doc in batch_docs:
        _id = doc.get("_id")
        upd = {}
        title_main = doc.get("title") or doc.get("name") or "İsim yok"
        is_series = bool(doc.get("seasons"))

        try:
            # 1. description çevirisi
            if doc.get("description"):
                upd["description"] = translate_text_safe(doc["description"], CACHE)
            else:
                if not is_series:
                    errors.append(f"ID: {_id} | Film: {title_main} | Neden: 'description' alanı boş")

            # 2. Dizi bölümleri çevirisi
            if is_series:
                seasons = doc.get("seasons", [])
                updated = False
                for season in seasons:
                    for ep in season.get("episodes", []):
                        if not ep.get("cevrildi", False) and ep.get("description"):
                            ep["description"] = translate_text_safe(ep["description"], CACHE)
                            ep["cevrildi"] = True
                            translated_episode_count += 1
                            updated = True
                if updated:
                    upd["seasons"] = seasons

            # 3. Film için üst seviye cevrildi bayrağı
            if not is_series:
                upd["cevrildi"] = True

            if upd:
                results.append((_id, upd))

        except Exception as e:
            errors.append(f"ID: {_id} | {'Dizi' if is_series else 'Film'}: {title_main} | Hata: {str(e)}")

    return results, errors, translated_episode_count

# ---------------- /cevir ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    start_msg = await message.reply_text("🇹🇷 Türkçe çeviri başlatılıyor...")

    start_time = time.time()

    # -------- Çevrilecek sayılar --------
    movie_to_translate = movie_col.count_documents({"cevrildi": {"$ne": True}})
    pipeline = [
        {"$unwind": "$seasons"},
        {"$unwind": "$seasons.episodes"},
        {"$match": {"seasons.episodes.cevrildi": {"$ne": True}}},
        {"$count": "total"}
    ]
    res = list(series_col.aggregate(pipeline))
    series_to_translate = res[0]["total"] if res else 0
    TOTAL_TO_TRANSLATE = movie_to_translate + series_to_translate

    # -------- Koleksiyonlar --------
    collections = [
        {
            "col": movie_col,
            "name": "Filmler",
            "ids": [d["_id"] for d in movie_col.find({"cevrildi": {"$ne": True}}, {"_id": 1})],
            "translated_now": 0,
            "errors_list": []
        },
        {
            "col": series_col,
            "name": "Diziler",
            "ids": [d["_id"] for d in series_col.find({"seasons.episodes.cevrildi": {"$ne": True}}, {"_id": 1})],
            "translated_now": 0,
            "errors_list": []
        }
    ]

    batch_size = 50
    pool = ThreadPoolExecutor(max_workers=4)
    loop = asyncio.get_event_loop()
    last_update = 0

    try:
        for c in collections:
            col = c["col"]
            ids = c["ids"]
            idx = 0

            while idx < len(ids):
                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))

                # Worker fonksiyonunu çalıştır
                results, errors, ep_count = await loop.run_in_executor(
                    pool,
                    translate_batch_worker,
                    {"docs": batch_docs}  # artık stop_event yok
                )

                c["errors_list"].extend(errors)

                for _id, upd in results:
                    if upd:
                        col.update_one({"_id": _id}, {"$set": upd})

                # Çevrilen sayısını güncelle
                if c["name"] == "Filmler":
                    c["translated_now"] += len(results)
                else:
                    c["translated_now"] += ep_count

                idx += len(batch_ids)

                # İlerleme güncellemesi
                elapsed = time.time() - start_time
                total_done = sum(x["translated_now"] for x in collections)
                remaining = TOTAL_TO_TRANSLATE - total_done
                eta = int((remaining * elapsed / total_done)) if total_done else 0

                if time.time() - last_update >= 10:
                    last_update = time.time()
                    cpu = psutil.cpu_percent(0.1)
                    ram = psutil.virtual_memory().percent

                    await start_msg.edit_text(
                        f"🇹🇷 Türkçe çeviri devam ediyor...\n\n"
                        f"Toplam içerik: {TOTAL_TO_TRANSLATE}\n"
                        f"Çevrilen: {total_done}\n"
                        f"Kalan: {remaining}\n"
                        f"{progress_bar(total_done, TOTAL_TO_TRANSLATE)}\n\n"
                        f"Süre: `{int(elapsed)}s` | ETA: `{eta}s`\n"
                        f"CPU: `{cpu}%` | RAM: `{ram}%`",
                        parse_mode=enums.ParseMode.MARKDOWN
                    )
    finally:
        pool.shutdown(wait=False)

    # -------- Genel Özet --------
    total_done = sum(c["translated_now"] for c in collections)
    total_errors = sum(len(c["errors_list"]) for c in collections)

    await start_msg.edit_text(
        f"📊 **Genel Özet**\n\n"
        f"Toplam: {TOTAL_TO_TRANSLATE}\n"
        f"Çevrilen: {total_done}\n"
        f"Kalan: {TOTAL_TO_TRANSLATE - total_done}\n"
        f"Hatalı: {total_errors}",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    # -------- Hata log dosyası --------
    hata_icerigi = []
    for c in collections:
        if c.get("errors_list"):
            hata_icerigi.append(f"*** {c['name']} Hataları ***")
            hata_icerigi.extend(c["errors_list"])
            hata_icerigi.append("")

    if hata_icerigi:
        import os
        log_path = os.path.join(os.getcwd(), "cevir_hatalari.txt")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(hata_icerigi))

        try:
            await client.send_document(
                chat_id=OWNER_ID,
                document=log_path,
                caption="⛔ Çeviri sırasında oluşan hatalar"
            )
        except Exception as e:
            print("Telegram gönderim hatası:", e)


# ---------------- /cevirekle ----------------
@Client.on_message(filters.command("cevirekle") & filters.private & filters.user(OWNER_ID))
async def cevirekle(client: Client, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları ekleniyor...")
    total_updated = 0

    # 1. Filmler için Üst Seviye 'cevrildi: true' ekleme
    col = movie_col
    docs_cursor = col.find({"cevrildi": {"$ne": True}}, {"_id": 1})
    bulk_ops = [UpdateOne({"_id": doc["_id"]}, {"$set": {"cevrildi": True}}) for doc in docs_cursor]
    
    if bulk_ops:
        res = col.bulk_write(bulk_ops)
        total_updated += res.modified_count

    # 2. Diziler için SADECE BÖLÜMLERE 'cevrildi: true' ekleme
    col = series_col
    bulk_ops = []
    docs_cursor = col.find({"seasons.episodes.cevrildi": {"$ne": True}}, {"_id": 1})
    for doc in docs_cursor:
        bulk_ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"seasons.$[].episodes.$[].cevrildi": True}}
            )
        )

    if bulk_ops:
        res = col.bulk_write(bulk_ops)
        total_updated += res.modified_count

    await status.edit_text(f"✅ 'cevrildi' alanları eklendi.\nToplam güncellenen kayıt: {total_updated}")

@Client.on_message(filters.command("cevirkaldir") & filters.private & filters.user(OWNER_ID))
async def cevirkaldir(client: Client, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları kaldırılıyor...")
    total_updated = 0

    # 1. FİLMLER için 'cevrildi' alanlarını kaldır
    # 'cevrildi' bayrağı olan tüm filmleri bul
    docs_cursor = movie_col.find({"cevrildi": True}, {"_id": 1})
    
    # Her film için $unset işlemi oluştur
    bulk_ops = [
        UpdateOne({"_id": doc["_id"]}, {"$unset": {"cevrildi": ""}}) 
        for doc in docs_cursor
    ]

    if bulk_ops:
        res = movie_col.bulk_write(bulk_ops)
        total_updated += res.modified_count

    # 2. DİZİLER için 'seasons.episodes.cevrildi' alanlarını kaldır
    bulk_ops = []
    
    # 'cevrildi' bayrağı olan bölümleri içeren tüm dizileri bul
    docs_cursor = series_col.find({"seasons.episodes.cevrildi": True}, {"_id": 1})
    
    # Her dizi için tüm bölümlerdeki 'cevrildi' alanını kaldıran $unset işlemi oluştur
    for doc in docs_cursor:
        bulk_ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$unset": {"seasons.$[].episodes.$[].cevrildi": ""}}
            )
        )

    if bulk_ops:
        res = series_col.bulk_write(bulk_ops)
        total_updated += res.modified_count

    await status.edit_text(f"✅ 'cevrildi' alanları kaldırıldı.\nToplam güncellenen kayıt: {total_updated}")


# ---------------- /TUR ----------------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_ve_platform_duzelt(client: Client, message: Message):
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")

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

    platform_map = {
        "MAX": "Max", "Hbomax": "Max", "TABİİ": "Tabii", "NF": "Netflix", "DSNP": "Disney",
        "Tod": "Tod", "Blutv": "Max", "Tv+": "Tv+", "Exxen": "Exxen",
        "Gain": "Gain", "HBO": "Max", "Tabii": "Tabii", "AMZN": "Amazon",
    }

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0

    for col, name in collections:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1, "telegram": 1, "seasons": 1})
        bulk_ops = []

        for doc in docs_cursor:
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            # Türleri güncelle
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                updated = True
            genres = new_genres

            # Telegram alanı üzerinden platform ekle
            for t in doc.get("telegram", []):
                name_field = t.get("name", "").lower()
                for key, val in platform_map.items():
                    if key.lower() in name_field and val not in genres:
                        genres.append(val)
                        updated = True

            # Sezonlardaki telegram kontrolleri
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name", "").lower()
                        for key, val in platform_map.items():
                            if key.lower() in name_field and val not in genres:
                                genres.append(val)
                                updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": genres}}))
                total_fixed += 1

        if bulk_ops:
            col.bulk_write(bulk_ops)

    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ---------------- /ISTATISTIK ----------------
def get_db_stats_and_genres(url):
    client = MongoClient(url)
    db = client[client.list_database_names()[0]]

    total_movies = db["movie"].count_documents({})
    total_series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize",0)/(1024*1024),2)
    storage_percent = round((storage_mb/512)*100,1)

    genre_stats=defaultdict(lambda:{"film":0,"dizi":0})
    for d in db["movie"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[d["_id"]]["film"]=d["count"]
    for d in db["tv"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[d["_id"]]["dizi"]=d["count"]
    return total_movies,total_series,storage_mb,storage_percent,genre_stats

def get_system_status():
    cpu = round(psutil.cpu_percent(interval=1),1)
    ram = round(psutil.virtual_memory().percent,1)
    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free/(1024**3),2)
    free_percent = round((disk.free/disk.total)*100,1)
    
    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}sa {m}dk {s}sn"

    return cpu, ram, free_disk, free_percent, uptime

@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def istatistik(client: Client, message: Message):
    total_movies,total_series,storage_mb,storage_percent,genre_stats=get_db_stats_and_genres(MONGO_URL)
    cpu,ram,free_disk,free_percent,uptime=get_system_status()

    genre_text="\n".join(f"{g:<14} | Film: {c['film']:<4} | Dizi: {c['dizi']:<4}" for g,c in sorted(genre_stats.items()))

    text=(
        f"⌬ <b>İstatistik</b>\n\n"
        f"┠ Filmler : {total_movies}\n"
        f"┠ Diziler : {total_series}\n"
        f"┖ Depolama: {storage_mb} MB (%{storage_percent})\n\n"
        f"<b>Tür Dağılımı</b>\n<pre>{genre_text}</pre>\n\n"
        f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
        f"┖ RAM → {ram}% | Süre → {uptime}"
    )

    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# ---------------- CALLBACK QUERY ----------------
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
    if query.data=="stop":
        await handle_stop(query)
