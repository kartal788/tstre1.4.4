from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os, re
from dotenv import load_dotenv
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"

if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

flood_wait = 5
last_command_time = {}

# -----------------------------
#  UNİVERSAL ID EXTRACTOR
# -----------------------------

def extract_id(raw):
    raw = raw.strip()

    # 1) Telegram ID extraction from direct URL
    # Example: https://domain.com/dl/<id>/video.mkv
    tg_match = re.search(r"/dl/([A-Za-z0-9]+)", raw)
    if tg_match:
        return tg_match.group(1), "telegram"

    # 2) Telegram ID as text
    if len(raw) > 30 and raw.isalnum():
        return raw, "telegram"

    # 3) IMDb ID
    if raw.lower().startswith("tt"):
        return raw, "imdb"

    # 4) TMDB link (stremio)
    # Example: https://web.stremio.com/#/detail/movie/10521-1
    stremio = re.search(r"/detail/(movie|series)/(\d+)-", raw)
    if stremio:
        return stremio.group(2), "tmdb"

    # 5) TMDB ID numeric
    if raw.isdigit():
        return raw, "tmdb"

    # 6) Fallback → treat as filename/id
    return raw, "filename"


# -----------------------------
#  /vsil KOMUTU
# -----------------------------

@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Kullanım:\n"
            "/vsil <telegram_id | tmdb_id | imdb_id | dosya adı | stremio link>", 
            quote=True
        )
        return

    raw_arg = message.command[1]
    arg, arg_type = extract_id(raw_arg)

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        mongo = MongoClient(db_urls[1])
        db = mongo[mongo.list_database_names()[0]]

        deleted_files = []

        # ---------------------------------------
        # TMDB → Komple film/dizi silme
        # ---------------------------------------
        if arg_type == "tmdb":
            tmdb_id = int(arg)

            # Movie
            movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    deleted_files.append(t.get("name"))
                db["movie"].delete_one({"_id": doc["_id"]})

            # TV
            tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            deleted_files.append(t.get("name"))
                db["tv"].delete_one({"_id": doc["_id"]})

        # ---------------------------------------
        # IMDb → Komple film/dizi silme
        # ---------------------------------------
        elif arg_type == "imdb":
            imdb_id = arg

            movie_docs = list(db["movie"].find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    deleted_files.append(t.get("name"))
                db["movie"].delete_one({"_id": doc["_id"]})

            tv_docs = list(db["tv"].find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            deleted_files.append(t.get("name"))
                db["tv"].delete_one({"_id": doc["_id"]})

        # ---------------------------------------
        # Telegram ID veya dosya adı → bölüm/film silme
        # ---------------------------------------
        else:
            target = arg

            # MOVIE
            movie_docs = list(db["movie"].find({}))
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                new_list = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]

                removed = [t.get("name") for t in telegram_list if t not in new_list]
                deleted_files += removed

                if not new_list:  # Filmde başka dosya kalmadı → filmi sil
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new_list
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

            # TV
            tv_docs = list(db["tv"].find({}))
            for doc in tv_docs:
                modified = False
                seasons_to_remove = []

                for season in doc.get("seasons", []):
                    episodes_to_remove = []

                    for ep in season.get("episodes", []):
                        tlist = ep.get("telegram", [])
                        new_tlist = [t for t in tlist if t.get("id") != target and t.get("name") != target]

                        removed = [t.get("name") for t in tlist if t not in new_tlist]
                        deleted_files += removed

                        if new_tlist:
                            ep["telegram"] = new_tlist
                        else:
                            episodes_to_remove.append(ep)

                    for ep in episodes_to_remove:
                        season["episodes"].remove(ep)

                    if not season["episodes"]:
                        seasons_to_remove.append(season)

                for s in seasons_to_remove:
                    doc["seasons"].remove(s)

                # Dizi tamamen boş kaldıysa → tamamını sil
                if not doc.get("seasons"):
                    db["tv"].delete_one({"_id": doc["_id"]})
                else:
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

        # ---------------------------------------
        # SONUC MESAJI
        # ---------------------------------------

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir dosya bulunamadı.", quote=True)
            return

        if len(deleted_files) > 10:
            file_path = f"/tmp/silinen_{int(time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(message.chat.id, file_path, caption=f"🗑 {len(deleted_files)} dosya silindi.")
        else:
            text = "\n".join(deleted_files)
            await message.reply_text(f"🗑 Silinen {len(deleted_files)} dosya:\n\n{text}")

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("vsil hata:", e)
