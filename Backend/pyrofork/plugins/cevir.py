from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import os
import importlib.util
import time
import math

# ------------ DATABASE'i config.py'den alma ------------
CONFIG_PATH = "/home/debian/tstre1.4.4/config.py"

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
if not db_urls:
    raise Exception("DATABASE bulunamadı!")

MONGO_URL = db_urls[0]
client = MongoClient(MONGO_URL)
db_name = client.list_database_names()[0]
db = client[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ------------ Deep Translator ------------
translator = GoogleTranslator(source='en', target='tr')

# ------------ Güvenli Çeviri Fonksiyonu ------------
def translate_text_safe(text):
    if not text or str(text).strip() == "":
        return ""
    try:
        return translator.translate(str(text))
    except Exception:
        return str(text)

# ------------ Progres bar ve ETA ------------
def progress_bar_eta(current, total, elapsed, bar_length=20):
    if total == 0:
        return "[--------------------] %0 ETA: 0s"
    percent = int((current / total) * 100)
    filled = int(bar_length * current // total)
    bar = "█" * filled + "-" * (bar_length - filled)
    if current == 0:
        eta = 0
    else:
        rate = elapsed / current
        eta = int(rate * (total - current))
    return f"[{bar}] %{percent} ETA: {eta}s"

# ------------ Koleksiyon İşleyici (Tek Mesaj Güncelleme) ------------
async def process_collection_interactive(collection, name, message, start_msg_id):
    data = list(collection.find({}))
    total = len(data)
    done = 0
    errors = 0

    start_time = time.time()

    while done < total:
        batch = data[done:done+20]  # 20 içeriklik batch
        for row in batch:
            update_dict = {}
            try:
                desc = row.get("description")
                if desc:
                    update_dict["description"] = translate_text_safe(desc)

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

                if update_dict:
                    collection.update_one({"_id": row["_id"]}, {"$set": update_dict})

            except Exception as e:
                errors += 1
                print(f"Hata: {e}")

            done += 1
            elapsed = time.time() - start_time

        # Mesaj güncelle
        bar_eta = progress_bar_eta(done, total, elapsed)
        text = f"{name}: {done}/{total} içerik işlendi {bar_eta}\nKalan: {total - done}, Hatalar: {errors}"
        await message.edit_text(text)

    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

# ------------ /cevir Komutu (Interaktif) ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    # Başlatma mesajı
    start_msg = await message.reply_text(
        "🇹🇷 Film ve dizi açıklamaları Türkçeye çevriliyor…\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    # Filmler
    movie_total, movie_done, movie_errors, movie_time = await process_collection_interactive(
        movie_col, "Filmler", start_msg, start_msg.id
    )

    # Diziler
    series_total, series_done, series_errors, series_time = await process_collection_interactive(
        series_col, "Diziler", start_msg, start_msg.id
    )

    # -------- Super Özet Tablosu --------
    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)

    summary = (
        "🎉 *Film & Dizi Türkçeleştirme Sonuçları*\n\n"
        "──────────────────────────────\n"
        f"📌 Filmler\nToplam içerik : {movie_total}\nİşlenen      : {movie_done}\nBaşarılı     : {movie_done - movie_errors}\nHatalı       : {movie_errors}\nSüre         : {movie_time} sn\n"
        "──────────────────────────────\n"
        f"📌 Diziler\nToplam içerik : {series_total}\nİşlenen      : {series_done}\nBaşarılı     : {series_done - series_errors}\nHatalı       : {series_errors}\nSüre         : {series_time} sn\n"
        "──────────────────────────────\n"
        f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {total_time} sn\n"
        "──────────────────────────────\n"
        "✅ Tüm içerikler başarıyla Türkçeye çevrildi!"
    )

    await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
