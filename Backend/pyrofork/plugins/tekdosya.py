import os
import time
import math
import asyncio
import json
import multiprocessing
import psutil
from concurrent.futures import ProcessPoolExecutor

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator

from Backend.helper.custom_filter import CustomFilters  # Owner filtresi

# ----------------- CONFIG & DATABASE -----------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    import importlib.util
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

translator = GoogleTranslator(source='en', target='tr')

# ----------------- GLOBAL -----------------
stop_event = asyncio.Event()
last_command_time = {}
flood_wait = 5
pending_deletes = {}  # /vsil onay için
confirmation_wait = 120  # saniye
awaiting_confirmation = {}  # /sil onay için

# ----------------- HELPER -----------------
def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = translator.translate(text)
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
    return f"[{bar}] {percent:.2f}%"

def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=0.5)

    if cpu_percent < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent < 60:
        workers = max(1, cpu_count)
    else:
        workers = 1

    if ram_percent < 40:
        batch = 80
    elif ram_percent < 60:
        batch = 40
    elif ram_percent < 75:
        batch = 20
    else:
        batch = 10

    return workers, batch

# ----------------- /vindir -----------------
@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now
    try:
        combined_data = {
            "movie": list(movie_col.find({}, {"_id": 0})),
            "tv": list(series_col.find({}, {"_id": 0}))
        }
        file_path = "/tmp/dizi_ve_film_veritabanı.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)
        await client.send_document(message.chat.id, file_path, caption="📁 Film ve Dizi Koleksiyonları")
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)

# ----------------- /vsil -----------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message: Message):
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
    deleted_files = []

    try:
        # movie
        if arg.isdigit():
            tmdb_id = int(arg)
            movie_docs = list(movie_col.find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]
            tv_docs = list(series_col.find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in ep.get("telegram", [])]

        elif arg.lower().startswith("tt"):
            imdb_id = arg
            movie_docs = list(movie_col.find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]
            tv_docs = list(series_col.find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in ep.get("telegram", [])]

        else:
            target = arg
            movie_docs = movie_col.find({"$or":[{"telegram.id": target},{"telegram.name": target}]})
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                deleted_files += [t.get("name") for t in match]
            tv_docs = series_col.find({})
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        telegram_list = ep.get("telegram", [])
                        match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                        deleted_files += [t.get("name") for t in match]

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        pending_deletes[user_id] = {"files": deleted_files, "arg": arg, "time": now}

        if len(deleted_files) > 10:
            file_path = f"/tmp/silinen_dosyalar_{int(time.time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(message.chat.id, file_path,
                caption=f"⚠️ {len(deleted_files)} dosya silinecek.\nSilmek için 'evet', iptal için 'hayır' yazın. ⏳ {confirmation_wait} sn.")
        else:
            await message.reply_text(
                f"⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n"
                f"{'\\n'.join(deleted_files)}\n\n"
                f"Silmek için **evet** yazın.\nİptal için **hayır** yazın.\n⏳ {confirmation_wait} saniye içinde cevap vermezsen işlem iptal edilir.",
                quote=True
            )
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)

@Client.on_message(filters.private & CustomFilters.owner)
async def confirm_delete(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in pending_deletes:
        return
    data = pending_deletes[user_id]
    now = time.time()
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
    # Silme işlemi movie ve tv
    if arg.isdigit():
        movie_col.delete_many({"tmdb_id": int(arg)})
        series_col.delete_many({"tmdb_id": int(arg)})
    elif arg.lower().startswith("tt"):
        movie_col.delete_many({"imdb_id": arg})
        series_col.delete_many({"imdb_id": arg})
    else:
        target = arg
        for doc in movie_col.find({"$or":[{"telegram.id": target},{"telegram.name": target}]}):
            telegram_list = [t for t in doc.get("telegram", []) if t.get("id") != target and t.get("name") != target]
            if telegram_list:
                movie_col.update_one({"_id": doc["_id"]}, {"$set":{"telegram": telegram_list}})
            else:
                movie_col.delete_one({"_id": doc["_id"]})
        for doc in series_col.find({}):
            modified = False
            for season in doc.get("seasons", []):
                season["episodes"] = [ep for ep in season.get("episodes", []) if all(t.get("id") != target and t.get("name") != target for t in ep.get("telegram", []))]
            series_col.replace_one({"_id": doc["_id"]}, doc)
    del pending_deletes[user_id]
    await message.reply_text("✅ Dosyalar başarıyla silindi.")

# ----------------- /sil -----------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client, message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\nOnaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n⏱ 60 saniye içinde cevap vermezsen işlem otomatik iptal edilir."
    )
    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id].cancel()
    async def timeout():
        await asyncio.sleep(60)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi otomatik olarak iptal edildi.")
    awaiting_confirmation[user_id] = asyncio.create_task(timeout())

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def handle_confirmation(client, message):
    user_id = message.from_user.id
    if user_id not in awaiting_confirmation:
        return
    awaiting_confirmation[user_id].cancel()
    awaiting_confirmation.pop(user_id, None)
    text = message.text.strip().lower()
    if text == "evet":
        movie_count = movie_col.count_documents({})
        series_count = series_col.count_documents({})
        movie_col.delete_many({})
        series_col.delete_many({})
        await message.reply_text(f"✅ Silme tamamlandı.\nFilmler silindi: {movie_count}\nDiziler silindi: {series_count}")
    elif text == "hayır":
        await message.reply_text("❌ Silme iptal edildi.")

# ----------------- /tur -----------------
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client, message):
    stop_event.clear()
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…",
                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]))
    genre_map = {
        "Action":"Aksiyon","Comedy":"Komedi","Drama":"Dram","Thriller":"Gerilim"
        # diğerlerini buraya ekleyebilirsin
    }
    platform_map = {"Netflix":"Netflix","Hbomax":"Max","Disney":"Disney"}
    total_fixed = 0
    for col, name in [(movie_col,"Filmler"), (series_col,"Diziler")]:
        for doc in col.find({}):
            if stop_event.is_set(): break
            genres = doc.get("genres", [])
            updated = False
            new_genres = [genre_map.get(g,g) for g in genres]
            if new_genres != genres:
                updated = True
                genres = new_genres
            for t in doc.get("telegram",[]):
                for key,val in platform_map.items():
                    if key.lower() in t.get("name","").lower() and val not in genres:
                        genres.append(val)
                        updated = True
            if updated:
                col.update_one({"_id":doc["_id"]},{"$set":{"genres":genres}})
                total_fixed +=1
    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ----------------- /istatistik -----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def istatistik(client, message):
    movie_count = movie_col.count_documents({})
    series_count = series_col.count_documents({})
    await message.reply_text(f"📊 İstatistik\nFilmler: {movie_count}\nDiziler: {series_count}")

# ----------------- /cevir -----------------
def translate_batch_worker(batch, stop_flag):
    CACHE = {}
    results = []
    for doc in batch:
        if stop_flag.is_set(): break
        _id = doc["_id"]
        upd = {}
        desc = doc.get("description")
        if desc: upd["description"] = translate_text_safe(desc, CACHE)
        seasons = doc.get("seasons",[])
        if seasons:
            modified=False
            for season in seasons:
                for ep in season.get("episodes",[]):
                    if stop_flag.is_set(): break
                    if "title" in ep: ep["title"]=translate_text_safe(ep["title"],CACHE)
                    if "overview" in ep: ep["overview"]=translate_text_safe(ep["overview"],CACHE)
                    modified=True
            if modified: upd["seasons"]=seasons
        results.append((_id,upd))
    return results

async def process_collection_parallel(collection,name,message):
    loop = asyncio.get_event_loop()
    ids = [d["_id"] for d in collection.find({},{"_id":1})]
    idx=0
    done=0
    total=len(ids)
    last_update=0
    workers,batch_size=dynamic_config()
    pool=ProcessPoolExecutor(max_workers=workers)
    while idx<len(ids):
        if stop_event.is_set(): break
        batch_ids=ids[idx:idx+batch_size]
        batch_docs=list(collection.find({"_id":{"$in":batch_ids}}))
        future=loop.run_in_executor(pool,translate_batch_worker,batch_docs,stop_event)
        results=await future
        for _id,upd in results:
            if stop_event.is_set(): break
            if upd: collection.update_one({"_id":_id},{"$set":upd})
            done+=1
        idx+=len(batch_ids)
        if time.time()-last_update>10 or idx>=len(ids):
            try:
                await message.edit_text(f"{name}: {done}/{total}\n{progress_bar(done,total)}")
            except: pass
            last_update=time.time()
    pool.shutdown(wait=False)
    return total,done

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    stop_event.clear()
    start_msg=await message.reply_text("🇹🇷 Türkçe çeviri hazırlanıyor…")
    movie_total,movie_done=await process_collection_parallel(movie_col,"Filmler",start_msg)
    series_total,series_done=await process_collection_parallel(series_col,"Diziler",start_msg)
    await start_msg.edit_text(f"✅ Çeviri tamamlandı.\nFilmler: {movie_done}/{movie_total}\nDiziler: {series_done}/{series_total}")

# ----------------- Callback: stop -----------------
@Client.on_callback_query()
async def handle_stop_cb(client: Client, query: CallbackQuery):
    if query.data=="stop":
        stop_event.set()
        try: await query.message.edit_text("⛔ İşlem iptal edildi!")
        except: pass
        try: await query.answer("Durdurma talimatı alındı.")
        except: pass
