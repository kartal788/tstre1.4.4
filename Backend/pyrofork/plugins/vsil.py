from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"
flood_wait = 5
last_command_time = {}

if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

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
            "⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n"
            "/vsil <telegram_id veya dosya_adı>\n"
            "/vsil <tmdb_id>\n"
            "/vsil tt<imdb_id>", quote=True)
        return

    arg = message.command[1]
    deleted_files = []

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        client_db = MongoClient(db_urls[1])
        db_name_list = client_db.list_database_names()
        db = client_db[db_name_list[0]]

        # -------- Silinecek dosyaları listele --------
        if arg.isdigit():
            tmdb_id = int(arg)
            movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]

            tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for episode in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in episode.get("telegram", [])]

        elif arg.lower().startswith("tt"):
            imdb_id = arg
            movie_docs = list(db["movie"].find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]

            tv_docs = list(db["tv"].find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for episode in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in episode.get("telegram", [])]

        else:
            target = arg
            movie_docs = db["movie"].find({"$or":[{"telegram.id": target},{"telegram.name": target}]})
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                deleted_files += [t.get("name") for t in match]

            tv_docs = db["tv"].find({})
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for episode in season.get("episodes", []):
                        telegram_list = episode.get("telegram", [])
                        match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                        deleted_files += [t.get("name") for t in match]

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        # -------- Dosyaları direkt sil --------
        if arg.isdigit():
            tmdb_id = int(arg)
            db["movie"].delete_many({"tmdb_id": tmdb_id})
            db["tv"].delete_many({"tmdb_id": tmdb_id})

        elif arg.lower().startswith("tt"):
            imdb_id = arg
            db["movie"].delete_many({"imdb_id": imdb_id})
            db["tv"].delete_many({"imdb_id": imdb_id})

        else:
            target = arg
            movie_docs = db["movie"].find({"$or":[{"telegram.id": target},{"telegram.name": target}]})
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                new_telegram = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
                if not new_telegram:
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new_telegram
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

            tv_docs = db["tv"].find({})
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
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

        await message.reply_text(f"✅ {len(deleted_files)} dosya başarıyla silindi.")

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)
