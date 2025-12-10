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

    # Flood kontrolü
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Lütfen silinecek dosya adını, ID veya telegram ID girin:\n"
            "/vsil <isim>\n"
            "/vsil tmdb <id>\n"
            "/vsil tt<imdb_id>\n"
            "/vsil <telegram_id>", quote=True)
        return

    telegram_id_arg = message.command[1]
    deleted_files = []

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        client_db = MongoClient(db_urls[1])
        db_name_list = client_db.list_database_names()
        db = client_db[db_name_list[0]]

        # -------- movie koleksiyonu --------
        movie_col = db["movie"]
        movie_docs = movie_col.find({"telegram.id": telegram_id_arg})
        for doc in movie_docs:
            telegram_list = doc.get("telegram", [])
            # Silinecek dosya var mı kontrol
            new_telegram = [t for t in telegram_list if t.get("id") != telegram_id_arg]
            deleted_files += [t.get("name") for t in telegram_list if t.get("id") == telegram_id_arg]

            if not new_telegram:
                # Artık telegram dosyası yoksa tüm filmi sil
                movie_col.delete_one({"_id": doc["_id"]})
            else:
                doc["telegram"] = new_telegram
                movie_col.replace_one({"_id": doc["_id"]}, doc)

        # -------- tv koleksiyonu --------
        tv_col = db["tv"]
        tv_docs = tv_col.find({})
        for doc in tv_docs:
            modified = False
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    telegram_list = episode.get("telegram", [])
                    new_telegram = [t for t in telegram_list if t.get("id") != telegram_id_arg]
                    if len(new_telegram) != len(telegram_list):
                        deleted_files += [t.get("name") for t in telegram_list if t.get("id") == telegram_id_arg]
                        episode["telegram"] = new_telegram
                        modified = True
            if modified:
                tv_col.replace_one({"_id": doc["_id"]}, doc)

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        # Silinen dosyaları gönder
        if len(deleted_files) > 50 or sum(len(f) for f in deleted_files) > 4000:
            file_path = f"/tmp/deleted_files_{int(time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(chat_id=message.chat.id, document=file_path, caption="✅ Silinen dosyalar")
        else:
            deleted_list_text = "\n".join(deleted_files)
            await message.reply_text(f"✅ Silinen dosyalar:\n{deleted_list_text}", quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)
