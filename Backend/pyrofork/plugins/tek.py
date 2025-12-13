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
    batch_docs = batch_data["docs"]
    stop_flag_set = batch_data["stop_flag_set"]
    if stop_flag_set:
        return [], []  # artık hatalar da döndürülüyor

    CACHE = {}
    results = []
    errors = []  # Hatalı veya çevrilemeyen içerikler için

    for doc in batch_docs:
        if stop_flag_set:
            break
        _id = doc.get("_id")
        upd = {}
        cevrildi = doc.get("cevrildi", False)

        if cevrildi:
            continue

        try:
            if doc.get("description"):
                upd["description"] = translate_text_safe(doc["description"], CACHE)

            seasons = doc.get("seasons")
            if seasons:
                for s in seasons:
                    for ep in s.get("episodes", []):
                        if ep.get("cevrildi", False):
                            continue
                        if ep.get("title"):
                            ep["title"] = translate_text_safe(ep["title"], CACHE)
                        if ep.get("overview"):
                            ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        ep["cevrildi"] = True
                upd["seasons"] = seasons

            upd["cevrildi"] = True
            results.append((_id, upd))
        except Exception as e:
            # Hata varsa loga ekle
            errors.append(f"ID: {_id} | Hata: {str(e)}")

    return results, errors
# ---------------- /cevir (güncellenmiş) ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    global stop_event
    if stop_event.is_set():
        await message.reply_text("⛔ Zaten devam eden bir işlem var.")
        return
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
    )

    start_time = time.time()  # Başlangıç zamanı

    collections = [
        {"col": movie_col, "name": "Filmler", "total": movie_col.count_documents({}), "done": 0, "errors_list": []},
        {"col": series_col, "name": "Diziler", "total_episodes": 0, "done_episodes": 0, "errors_list": []},
    ]

    # Diziler için toplam bölüm sayısını hesapla
    series_col_data = collections[1]
    total_eps = 0
    for doc in series_col.find({}, {"seasons.episodes": 1}):
        for season in doc.get("seasons", []):
            total_eps += len(season.get("episodes", []))
    series_col_data["total_episodes"] = total_eps

    batch_size = 50
    workers = 4
    pool = ThreadPoolExecutor(max_workers=workers)
    loop = asyncio.get_event_loop()
    last_update = time.time()
    update_interval = 10  # saniye

    try:
        for c in collections:
            col = c["col"]
            ids = [d["_id"] for d in col.find({}, {"_id": 1})]
            idx = 0

            while idx < len(ids):
                if stop_event.is_set():
                    break

                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))
                worker_data = {"docs": batch_docs, "stop_flag_set": stop_event.is_set()}

                results, errors = await loop.run_in_executor(pool, translate_batch_worker, worker_data)
                c["errors_list"].extend(errors)

                # Veritabanına yaz
                for _id, upd in results:
                    try:
                        if upd:
                            col.update_one({"_id": _id}, {"$set": upd})
                    except:
                        c["errors_list"].append(f"ID: {_id} | DB Güncelleme Hatası")

                idx += len(batch_ids)

                # İlerleme sayısını doğru şekilde güncelle
                if c["name"] == "Diziler":
                    done_eps = 0
                    for doc in col.aggregate([
                        {"$unwind": "$seasons"},
                        {"$unwind": "$seasons.episodes"},
                        {"$match": {"seasons.episodes.cevrildi": True}},
                        {"$count": "done"}
                    ]):
                        done_eps = doc["done"]
                    c["done_episodes"] = done_eps
                else:
                    c["done"] = col.count_documents({"cevrildi": True})

                # İlerleme mesajını güncelle
                if time.time() - last_update >= update_interval or idx >= len(ids):
                    last_update = time.time()
                    progress_lines = []
                    for coll in collections:
                        done_count = coll.get("done_episodes", coll.get("done", 0))
                        total_count = coll.get("total_episodes", coll.get("total", 0))
                        progress_lines.append(
                            f"**{coll['name']}**: {done_count}/{total_count}\n"
                            f"{progress_bar(done_count, total_count)}\n"
                            f"Hatalar: {len(coll['errors_list'])}\n"
                        )
                    progress_text = "🇹🇷 Türkçe çeviri hazırlanıyor...\n\n" + "\n".join(progress_lines)
                    try:
                        await start_msg.edit_text(
                            progress_text,
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
                        )
                    except:
                        pass
    finally:
        pool.shutdown(wait=False)

# ---------------- Sonuç ekranı ----------------
end_time = time.time()
total_duration = end_time - start_time  # Toplam süre

# Toplam sayılar
total_movies = collections[0].get("done", 0)
total_episodes = collections[1].get("done_episodes", 0)
total_done = total_movies + total_episodes
total_to_translate = collections[0]["total"] + collections[1]["total_episodes"]
total_remaining = total_to_translate - total_done
total_errors = sum(len(c["errors_list"]) for c in collections)

# Genel özet metni
final_text = (
    "📊 **Genel Özet**\n\n"
    f"┠ Film    : {total_movies}\n"
    f"┠ Bölüm   : {total_episodes}\n"
    f"┠ Başarılı: {total_done}\n"
    f"┠ Kalan   : {total_remaining}\n"
    f"┠ Hatalı  : {total_errors}\n"
    f"┠ Süre    : {format_time_custom(total_duration)}\n"
)

# Hataları ekle
hata_icerigi = []
for c in collections:
    if c["errors_list"]:
        hata_icerigi.append(f"*** {c['name']} Hataları ***")
        hata_icerigi.extend(c["errors_list"])
        hata_icerigi.append("")  # Boş satır

if hata_icerigi:
    final_text += "\n".join(hata_icerigi)

# Mesajı güncelle
await start_msg.edit_text(final_text, parse_mode=enums.ParseMode.MARKDOWN)

    # Hataları dosya olarak gönder
    hata_icerigi = []
    for c in collections:
        if c["errors_list"]:
            hata_icerigi.append(f"*** {c['name']} Hataları ***")
            hata_icerigi.extend(c["errors_list"])
            hata_icerigi.append("\n")

    if hata_icerigi:
        log_path = "cevirhatalari.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(hata_icerigi))

        try:
            await client.send_document(chat_id=OWNER_ID, document=log_path, caption="⛔ Çeviri sırasında hatalar oluştu / kalan içerikler")
        except:
            pass
# ---------------- /cevirekle ----------------
@Client.on_message(filters.command("cevirekle") & filters.private & filters.user(OWNER_ID))
async def cevirekle(client: Client, message: Message):
    status = await message.reply_text("🔄 'cevrildi' alanları ekleniyor...")
    total_updated = 0

    for col in (movie_col, series_col):
        # Üst seviye belgeler
        docs_cursor = col.find({"cevrildi": {"$ne": True}}, {"_id": 1})
        bulk_ops = [UpdateOne({"_id": doc["_id"]}, {"$set": {"cevrildi": True}}) for doc in docs_cursor]

        # Dizi bölümleri için
        if col == series_col:
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

    for col in (movie_col, series_col):
        # Üst seviye belgeler
        docs_cursor = col.find({"cevrildi": True}, {"_id": 1})
        bulk_ops = [UpdateOne({"_id": doc["_id"]}, {"$unset": {"cevrildi": ""}}) for doc in docs_cursor]

        # Dizi bölümleri için
        if col == series_col:
            docs_cursor = col.find({"seasons.episodes.cevrildi": True}, {"_id": 1})
            for doc in docs_cursor:
                bulk_ops.append(
                    UpdateOne(
                        {"_id": doc["_id"]},
                        {"$unset": {"seasons.$[].episodes.$[].cevrildi": ""}}
                    )
                )

        if bulk_ops:
            res = col.bulk_write(bulk_ops)
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
