from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import os
import importlib.util
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import psutil

# ------------ AYARLAR ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"
PROGRESS_UPDATE_INTERVAL = 30  # saniye

# ------------ OTOMATİK DONANIM ÖLÇÜMÜ ------------
MIN_WORKERS = 5
MAX_WORKERS_LIMIT = 40
MIN_BATCH = 50
MAX_BATCH = 200

DEFAULT_MAX_WORKERS = 20
DEFAULT_BATCH_SIZE = 120

def initial_hardware_settings():
    ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    cpu_count = psutil.cpu_count()
    if ram_gb < 2:
        return 10, 60
    elif ram_gb < 8:
        return 20, 120
    else:
        return 35, 180

MAX_WORKERS, BATCH_SIZE = initial_hardware_settings()
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
print(f"Başlangıç: MAX_WORKERS={MAX_WORKERS}, BATCH_SIZE={BATCH_SIZE}")

# Dinamik ayarlama
def adjust_settings_dynamically():
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)
    
    global MAX_WORKERS, BATCH_SIZE, executor

    # RAM kullanımı veya CPU yüksek → azalt
    if mem.percent > 80 or cpu > 85:
        MAX_WORKERS = max(MIN_WORKERS, MAX_WORKERS - 5)
        BATCH_SIZE = max(MIN_BATCH, BATCH_SIZE - 20)
    # RAM ve CPU düşük → artır
    elif mem.percent < 50 and cpu < 50:
        MAX_WORKERS = min(MAX_WORKERS_LIMIT, MAX_WORKERS + 5)
        BATCH_SIZE = min(MAX_BATCH, BATCH_SIZE + 20)
    
    executor._max_workers = MAX_WORKERS
    print(f"Dinamik Güncelleme: MAX_WORKERS={MAX_WORKERS}, BATCH_SIZE={BATCH_SIZE}, RAM={mem.percent}%, CPU={cpu}%")

# ------------ DATABASE'i config.py'den alma ------------
def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

# ------------ DATABASE Bağlantısı ------------
db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client = MongoClient(MONGO_URL)
db_name = client.list_database_names()[0]
db = client[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ------------ Çeviri motoru ------------
translator = GoogleTranslator(source='en', target='tr')

# ------------ Yardımcı Fonksiyonlar ------------
def format_time(seconds):
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

def looks_like_turkish(text: str) -> bool:
    if not text: return True
    turkish_chars = set("ıİşşĞğÜüÖöÇç")
    return any((c in turkish_chars) for c in text)

def _do_translate(text: str) -> str:
    try: return translator.translate(str(text))
    except: return str(text)

async def translate_text_async(text: str) -> str:
    if text is None: return ""
    t = str(text).strip()
    if t == "" or looks_like_turkish(t): return t
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, _do_translate, t)

async def translate_many_async(texts):
    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(executor, _do_translate, str(t)) if (t and not looks_like_turkish(str(t)))
        else asyncio.sleep(0, result=(t or ""))
        for t in texts
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return results

# ------------ Koleksiyon İşleyici ------------
async def process_collection_interactive(collection, name, message):
    cursor = collection.find({})
    data = list(cursor)
    total = len(data)
    if total == 0:
        try:
            await message.edit_text(f"{name}: 0/0\n{progress_bar(0,0)}\nKalan: 0, Hatalar: 0\n⏳ Kalan tahmini süre (ETA): 00:00:00")
        except: pass
        return 0,0,0,0.0

    done, errors = 0, 0
    start_time = time.time()
    last_update = 0
    updates = []

    while done < total:
        batch = data[done:done+BATCH_SIZE]

        desc_texts, desc_map = [], []
        ep_title_texts, ep_overview_texts = [], []
        t_iter_idx, o_iter_idx = 0, 0

        for i, row in enumerate(batch):
            d = row.get("description")
            if d is not None:
                desc_texts.append(d)
                desc_map.append(i)
            seasons = row.get("seasons")
            if seasons and isinstance(seasons, list):
                for s_idx, season in enumerate(seasons):
                    episodes = season.get("episodes")
                    if episodes and isinstance(episodes, list):
                        for e_idx, ep in enumerate(episodes):
                            t = ep.get("title")
                            if t is not None: ep_title_texts.append(t)
                            o = ep.get("overview")
                            if o is not None: ep_overview_texts.append(o)

        desc_results = await translate_many_async(desc_texts) if desc_texts else []
        title_results = await translate_many_async(ep_title_texts) if ep_title_texts else []
        overview_results = await translate_many_async(ep_overview_texts) if ep_overview_texts else []

        for idx, translated in enumerate(desc_results):
            batch_idx = desc_map[idx]
            batch[batch_idx]["description"] = translated

        t_iter, o_iter = iter(title_results), iter(overview_results)
        for row in batch:
            seasons = row.get("seasons")
            if seasons and isinstance(seasons, list):
                for season in seasons:
                    episodes = season.get("episodes")
                    if episodes and isinstance(episodes, list):
                        for ep in episodes:
                            if ep.get("title") is not None:
                                ep["title"] = next(t_iter)
                            if ep.get("overview") is not None:
                                ep["overview"] = next(o_iter)

        for row in batch:
            update_dict = {}
            if "description" in row: update_dict["description"] = row["description"]
            if "seasons" in row: update_dict["seasons"] = row["seasons"]
            if update_dict:
                try: updates.append(UpdateOne({"_id": row["_id"]}, {"$set": update_dict}))
                except: errors += 1

        if updates:
            loop = asyncio.get_running_loop()
            def _bulk(): return collection.bulk_write(updates, ordered=False)
            try: await loop.run_in_executor(executor, _bulk)
            except: errors += len(updates)
            updates = []

        done += len(batch)

        # Dinamik ayar güncellemesi
        adjust_settings_dynamically()

        # ETA hesaplama
        current_time = time.time()
        elapsed = current_time - start_time
        rate = done / elapsed if elapsed > 0 else 0
        remaining_items = total - done
        eta = remaining_items / rate if rate > 0 else 0

        if current_time - last_update > PROGRESS_UPDATE_INTERVAL or done == total:
            bar = progress_bar(done, total)
            text = f"{name}: {done}/{total}\n{bar}\nKalan: {remaining_items}, Hatalar: {errors}\n⏳ Kalan tahmini süre (ETA): {format_time(eta)}"
            try: await message.edit_text(text)
            except: pass
            last_update = current_time

    total_elapsed = time.time() - start_time
    return total, done, errors, total_elapsed

# ------------ /cevir Komutu ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    start_msg = await message.reply_text(
        "🇹🇷 Film ve dizi açıklamaları Türkçeye çevriliyor…\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    movie_total, movie_done, movie_errors, movie_time = await process_collection_interactive(movie_col, "Filmler", start_msg)
    series_total, series_done, series_errors, series_time = await process_collection_interactive(series_col, "Diziler", start_msg)

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = movie_time + series_time

    summary = (
        "🎉 *Film & Dizi Türkçeleştirme Sonuçları*\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\n⏱ Toplam süre : {format_time(total_time)}\n"
    )

    await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
