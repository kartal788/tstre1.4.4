# bot_async.py
import asyncio
import time
from pyrogram import Client, filters, enums, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import multiprocessing
import psutil
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import os

# ---------------- GLOBAL ----------------
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
DOWNLOAD_DIR = "/"
BOT_START_TIME = time.time()
cevir_stop_event = asyncio.Event()
tur_stop_event = asyncio.Event()

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
mongo_client = MongoClient(MONGO_URL)
db_name = mongo_client.list_database_names()[0]
db = mongo_client[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ---------------- HELPERS ----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    workers = max(1, min(cpu_count, 4))
    batch = 50 if ram_percent < 50 else 25 if ram_percent < 75 else 10
    return workers, batch

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

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {min(percent, 100.0):.2f}%"

def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"
    total_seconds = int(total_seconds)
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

# ---------------- WORKER ----------------
def translate_batch_worker(batch_data):
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
        if seasons:
            modified = False
            for season in seasons:
                for ep in season.get("episodes", []):
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

# ---------------- CALLBACK ----------------
async def handle_stop(callback_query: CallbackQuery, stop_event: asyncio.Event):
    stop_event.set()
    try:
        await callback_query.message.edit_text("⛔ İşlem iptal edildi!")
    except:
        pass
    try:
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# ---------------- Pyrogram Client ----------------
app = Client("my_bot")

# ---------------- /cevir KOMUTU ----------------
@app.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def turkce_icerik(client: Client, message: Message):
    global cevir_stop_event
    if cevir_stop_event.is_set():
        await message.reply_text("⛔ Devam eden bir işlem var.")
        return
    cevir_stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop_cevir")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": movie_col.count_documents({}), "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "total": series_col.count_documents({}), "done": 0, "errors": 0}
    ]

    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)
    start_time = time.time()
    last_update = 0

    try:
        for c in collections:
            col = c["col"]
            total = c["total"]
            done = c["done"]
            if total == 0:
                continue

            ids = [d["_id"] for d in col.find({}, {"_id": 1})]
            idx = 0
            while idx < len(ids):
                if cevir_stop_event.is_set():
                    break
                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))
                worker_data = {"docs": batch_docs, "stop_flag_set": cevir_stop_event.is_set()}
                loop = asyncio.get_event_loop()
                future = loop.run_in_executor(pool, translate_batch_worker, worker_data)
                results = await future
                for _id, upd in results:
                    if cevir_stop_event.is_set():
                        break
                    if upd:
                        col.update_one({"_id": _id}, {"$set": upd})
                        done += 1
                c["done"] = done
                idx += len(batch_ids)

                if time.time() - last_update > 10:
                    text = f"📌 {c['name']} ilerleme: {done}/{total}\n{progress_bar(done, total)}"
                    try:
                        await start_msg.edit_text(text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop_cevir")]]))
                    except:
                        pass
                    last_update = time.time()
    finally:
        pool.shutdown(wait=False)

    final_text = "🎉 Çeviri tamamlandı!\n"
    for c in collections:
        final_text += f"{c['name']}: {c['done']}/{c['total']}\n"
    try:
        await start_msg.edit_text(final_text)
    except:
        pass

# ---------------- /tur KOMUTU ----------------
@app.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_ve_platform_duzelt(client: Client, message: Message):
    global tur_stop_event
    tur_stop_event.clear()
    start_msg = await message.reply_text(
        "🔄 Tür ve platform güncellemesi başlatıldı…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop_tur")]])
    )

    genre_map = {"Action": "Aksiyon", "Sci-Fi": "Bilim Kurgu", "Comedy": "Komedi"}
    platform_genre_map = {"MAX": "Max", "NF": "Netflix", "DSNP": "Disney"}
    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]

    total_fixed = 0
    last_update = 0
    for col, name in collections:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1, "telegram": 1, "seasons": 1})
        bulk_ops = []
        for doc in docs_cursor:
            if tur_stop_event.is_set():
                break
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                updated = True
            genres = new_genres

            for t in doc.get("telegram", []):
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
                    await start_msg.edit_text(f"{name}: Güncellenen kayıtlar: {total_fixed}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop_tur")]]))
                except:
                    pass
                last_update = time.time()

        if bulk_ops:
            col.bulk_write(bulk_ops)

    try:
        await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")
    except:
        pass

# ---------------- /istatistik KOMUTU ----------------
@app.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def send_statistics(client: Client, message: Message):
    try:
        genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})
        for doc in movie_col.aggregate([
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres", "count": {"$sum": 1}}}
        ]):
            genre_stats[doc["_id"]]["film"] = doc["count"]

        for doc in series_col.aggregate([
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres", "count": {"$sum": 1}}}
        ]):
            genre_stats[doc["_id"]]["dizi"] = doc["count"]

        cpu = round(psutil.cpu_percent(interval=1), 1)
        ram = round(psutil.virtual_memory().percent, 1)
        disk = psutil.disk_usage(DOWNLOAD_DIR)
        free_disk = round(disk.free / (1024 ** 3), 2)
        free_percent = round((disk.free / disk.total) * 100, 1)
        uptime_sec = int(time.time() - BOT_START_TIME)
        h, rem = divmod(uptime_sec, 3600)
        m, s = divmod(rem, 60)
        uptime = f"{h}s {m}d {s}s"

        genre_lines = []
        for genre, counts in sorted(genre_stats.items(), key=lambda x: x[0]):
            genre_lines.append(f"{genre:<12} | Film: {counts['film']:<3} | Dizi: {counts['dizi']:<3}")

        genre_text = "\n".join(genre_lines)

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {movie_col.count_documents({})}\n"
            f"┠ Diziler: {series_col.count_documents({})}\n\n"
            f"<b>Tür Bazlı:</b>\n"
            f"<pre>{genre_text}</pre>\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)

# ---------------- CALLBACK HANDLER ----------------
@app.on_callback_query()
async def on_callback(client: Client, query: CallbackQuery):
    if query.data == "stop_cevir":
        await handle_stop(query, cevir_stop_event)
    elif query.data == "stop_tur":
        await handle_stop(query, tur_stop_event)

# ---------------- BOT START (ASGI / Docker uyumlu) ----------------
async def start_bot():
    await app.start()
    print("Bot başlatıldı...")
    await idle()
    await app.stop()
    print("Bot durduruldu.")

if __name__ == "__main__":
    # Lokal terminal için asyncio.run
    asyncio.run(start_bot())
