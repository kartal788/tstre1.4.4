from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import os
import importlib.util
import time

# ------------ DATABASE Bağlantısı ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

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

# ------------ Güvenli Çeviri Fonksiyonu ------------
def translate_text_safe(text):
    if not text or str(text).strip() == "":
        return ""
    try:
        return translator.translate(str(text))
    except Exception:
        return str(text)

# ------------ Progres bar ------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ------------ Koleksiyon İşleyici ------------
async def process_collection_interactive(collection, name, message):
    data = list(collection.find({}))
    total = len(data)
    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    while done < total:
        batch = data[done:done+20]  # 20 içerik batch
        for row in batch:
            update_dict = {}
            try:
                # Description
                desc = row.get("description")
                if desc:
                    update_dict["description"] = translate_text_safe(desc)

                # Seasons & episodes
                seasons = row.get("seasons")
                if seasons and isinstance(seasons, list):
                    for season in seasons:
                        episodes = season.get("episodes")
                        if episodes and isinstance(episodes, list):
                            for ep in episodes:
                                if "title" in ep and ep["title"]:
                                    ep["title"] = translate_text_safe(ep["title"])
                                if "overview" in ep and ep["overview"]:
                                    ep["overview"] = translate_text_safe(ep["overview"])
                    update_dict["seasons"] = seasons

                # Genres
                genres = row.get("genres")
                if genres and isinstance(genres, list):
                    update_dict["genres"] = [translate_text_safe(g) for g in genres]

                # Update MongoDB
                if update_dict:
                    collection.update_one({"_id": row["_id"]}, {"$set": update_dict})

            except Exception as e:
                errors += 1
                print(f"Hata: {e}")

            done += 1
            current_time = time.time()

            if current_time - last_update > 30 or done == total:
                bar = progress_bar(done, total)
                text = f"{name}: {done}/{total}\n{bar}\nKalan: {total - done}, Hatalar: {errors}"
                try:
                    await message.edit_text(text)
                except Exception:
                    pass
                last_update = current_time

    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

# ------------ /cevir Komutu ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    start_msg = await message.reply_text(
        "🇹🇷 Film ve dizi açıklamaları Türkçeye çevriliyor…\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    # Filmler
    movie_total, movie_done, movie_errors, movie_time = await process_collection_interactive(
        movie_col, "Filmler", start_msg
    )

    # Diziler
    series_total, series_done, series_errors, series_time = await process_collection_interactive(
        series_col, "Diziler", start_msg
    )

    # -------- Özet --------
    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)

    # Saat/dakika/saniye formatı
    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours)}h{int(minutes)}m{int(seconds)}s"

    summary = (
        "🎉 *Film & Dizi Türkçeleştirme Sonuçları*\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {eta_str}\n"
    )

    await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
