from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from dotenv import load_dotenv
import os, re
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"

if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

# ------------------------------------------------------------------
#  UNIVERSAL ID PARSE + STREMIO → TMDB → IMDb fallback
# ------------------------------------------------------------------

def extract_id(raw):
    raw = raw.strip()

    # STREMIO → TMDB
    stremio = re.search(r"/detail/(movie|series)/(\d+)-", raw)
    if stremio:
        tmdb = stremio.group(2)
        return ("tmdb", tmdb, f"tt{tmdb}")

    # TMDB ID
    if raw.isdigit():
        return ("tmdb", raw, f"tt{raw}")

    # IMDb ID
    if raw.lower().startswith("tt"):
        return ("imdb", raw, None)

    # Telegram ID (URL)
    tg = re.search(r"/dl/([A-Za-z0-9]+)", raw)
    if tg:
        return ("telegram", tg.group(1), None)

    # Long alphanumeric → Telegram ID
    if len(raw) > 30 and raw.isalnum():
        return ("telegram", raw, None)

    # filename
    return ("filename", raw, None)


# ------------------------------------------------------------------
#  SİLME MOTORU
#  category = "movie", "tv", "all"
# ------------------------------------------------------------------

def process_delete(db, id_type, val, imdb_fallback=None, test=False, category="all"):
    deleted = []

    def allow(cat):
        return category == "all" or category == cat

    # ----------------------- TMDB -----------------------
    if id_type == "tmdb":
        tmdb_id = int(val)

        movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id})) if allow("movie") else []
        tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id})) if allow("tv") else []

        # TMDB yoksa IMDb fallback
        if not movie_docs and not tv_docs and imdb_fallback:
            return process_delete(db, "imdb", imdb_fallback, None, test, category)

        # MOVIE
        for doc in movie_docs:
            for t in doc.get("telegram", []):
                deleted.append(t.get("name"))
            if not test:
                db["movie"].delete_one({"_id": doc["_id"]})

        # TV
        for doc in tv_docs:
            for s in doc.get("seasons", []):
                for e in s.get("episodes", []):
                    for t in e.get("telegram", []):
                        deleted.append(t.get("name"))
            if not test:
                db["tv"].delete_one({"_id": doc["_id"]})

        return deleted

    # ----------------------- IMDb -----------------------
    if id_type == "imdb":
        imdb_id = val

        movie_docs = list(db["movie"].find({"imdb_id": imdb_id})) if allow("movie") else []
        tv_docs = list(db["tv"].find({"imdb_id": imdb_id})) if allow("tv") else []

        for doc in movie_docs:
            for t in doc.get("telegram", []):
                deleted.append(t.get("name"))
            if not test:
                db["movie"].delete_one({"_id": doc["_id"]})

        for doc in tv_docs:
            for s in doc.get("seasons", []):
                for e in s.get("episodes", []):
                    for t in e.get("telegram", []):
                        deleted.append(t.get("name"))
            if not test:
                db["tv"].delete_one({"_id": doc["_id"]})

        return deleted

    # ---------------- TELEGRAM / FILENAME --------------
    target = val

    # MOVIE
    if allow("movie"):
        for doc in list(db["movie"].find({})):
            old = doc.get("telegram", [])
            new = [t for t in old if t.get("id") != target and t.get("name") != target]
            removed = [t.get("name") for t in old if t not in new]
            deleted.extend(removed)

            if removed and not test:
                if not new:
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

    # TV
    if allow("tv"):
        for doc in list(db["tv"].find({})):
            changed = False
            remove_seasons = []

            for season in doc.get("seasons", []):
                remove_eps = []
                for ep in season.get("episodes", []):
                    old = ep.get("telegram", [])
                    new = [t for t in old if t.get("id") != target and t.get("name") != target]

                    removed = [t.get("name") for t in old if t not in new]
                    deleted.extend(removed)

                    if removed:
                        changed = True

                    if new:
                        ep["telegram"] = new
                    else:
                        remove_eps.append(ep)

                for e in remove_eps:
                    season["episodes"].remove(e)

                if not season["episodes"]:
                    remove_seasons.append(season)

            for s in remove_seasons:
                doc["seasons"].remove(s)

            if changed and not test:
                if not doc["seasons"]:
                    db["tv"].delete_one({"_id": doc["_id"]})
                else:
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

    return deleted


# ------------------------------------------------------------------
#  UZUN LİSTE → TXT OLARAK GÖNDER
# ------------------------------------------------------------------

async def send_output(message, data, prefix):
    if not data:
        return await message.reply_text("⚠️ Hiçbir dosya bulunamadı.")

    if len(data) > 10:
        path = f"/tmp/{prefix}_{int(time())}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(data))
        return await message.reply_document(path, caption=f"{len(data)} dosya listelendi.")

    return await message.reply_text("\n".join(data))


# ------------------------------------------------------------------
#  /vbilgi – SİLMEZ, SADECE DOSYALARI GÖSTERİR (Film + Dizi)
# ------------------------------------------------------------------

@Client.on_message(filters.command("vbilgi") & filters.private & CustomFilters.owner)
async def vbilgi(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vbilgi <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=True, category="all")
    await send_output(message, data, "vbilgi")


# ------------------------------------------------------------------
#  /vsil – TÜM kategoriler
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def vsil(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsil <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=False, category="all")
    await send_output(message, data, "vsil")


# ------------------------------------------------------------------
#  /vtest – TÜM kategoriler (silmez)
# ------------------------------------------------------------------

@Client.on_message(filters.command("vtest") & filters.private & CustomFilters.owner)
async def vtest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vtest <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=True, category="all")
    await send_output(message, data, "vtest")


# ------------------------------------------------------------------
#  /vsild – SADECE DİZİ
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsild") & filters.private & CustomFilters.owner)
async def vsild(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsild <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=False, category="tv")
    await send_output(message, data, "vsild")


# ------------------------------------------------------------------
#  /vsildtest – SADECE DİZİ (silmez)
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsildtest") & filters.private & CustomFilters.owner)
async def vsildtest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsildtest <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=True, category="tv")
    await send_output(message, data, "vsildtest")


# ------------------------------------------------------------------
#  /vsilf – SADECE FİLM
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsilf") & filters.private & CustomFilters.owner)
async def vsilf(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsilf <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=False, category="movie")
    await send_output(message, data, "vsilf")


# ------------------------------------------------------------------
#  /vsilftest – SADECE FİLM (silmez)
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsilftest") & filters.private & CustomFilters.owner)
async def vsilftest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsilftest <id/link>")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    idtype, value, fallback = extract_id(message.command[1])

    data = process_delete(db, idtype, value, fallback, test=True, category="movie")
    await send_output(message, data, "vsilftest")
