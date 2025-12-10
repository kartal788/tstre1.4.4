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
            "⚠️ Lütfen silinecek dosya adını, TMDB/IMDb ID veya Telegram ID girin:\n"
            "/vsil 136548920 \n"
            "/vsil tt1025646\n"
            "/vsil 2NcDg93E2vkVYrXnpllWOnDH576JS4LjRdYUFjg1LnSETEjsAvYCXVF", quote=True)
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

        # -------- TMDB ID ile silme --------
        if arg.isdigit():
            tmdb_id = int(arg)
            # Movie
            movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]
            db["movie"].delete_many({"tmdb_id": tmdb_id})

            # TV
            tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for episode in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in episode.get("telegram", [])]
            db["tv"].delete_many({"tmdb_id": tmdb_id})

        # -------- IMDb ID ile silme --------
        elif arg.lower().startswith("tt"):
            imdb_id = arg
            # Movie
            movie_docs = list(db["movie"].find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]
            db["movie"].delete_many({"imdb_id": imdb_id})

            # TV
            tv_docs = list(db["tv"].find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for episode in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in episode.get("telegram", [])]
            db["tv"].delete_many({"imdb_id": imdb_id})

        # -------- Telegram ID ile silme --------
        else:
            target_id = arg
            # TV Koleksiyonu
            tv_docs = list(db["tv"].find({}))
            for doc in tv_docs:
                modified = False
                seasons_to_remove = []
                for season in doc.get("seasons", []):
                    episodes_to_remove = []
                    for episode in season.get("episodes", []):
                        telegram_list = episode.get("telegram", [])
                        match = [t for t in telegram_list if t.get("id") == target_id]
                        if match:
                            deleted_files += [t.get("name") for t in match]
                            new_telegram = [t for t in telegram_list if t.get("id") != target_id]
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
                # Eğer dizide artık sezon yoksa, tüm diziyi sil
                if not doc.get("seasons"):
                    db["tv"].delete_one({"_id": doc["_id"]})
                elif modified:
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

            # Movie Koleksiyonu
            movie_docs = list(db["movie"].find({}))
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                match = [t for t in telegram_list if t.get("id") == target_id]
                if match:
                    deleted_files += [t.get("name") for t in match]
                    new_telegram = [t for t in telegram_list if t.get("id") != target_id]
                    if not new_telegram:
                        db["movie"].delete_one({"_id": doc["_id"]})
                    else:
                        doc["telegram"] = new_telegram
                        db["movie"].replace_one({"_id": doc["_id"]}, doc)

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        # -------- Silinen dosyaları gönder --------
        if len(deleted_files) > 10:
            file_path = f"/tmp/silinen_dosyalar_{int(time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(chat_id=message.chat.id, document=file_path,
                                       caption=f"✅ {len(deleted_files)} dosya başarıyla silindi.")
        else:
            await message.reply_text("✅ Silinen dosyalar:\n" + "\n".join(deleted_files))

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)
