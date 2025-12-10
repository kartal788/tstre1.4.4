import asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient

# ---------------- DATABASE ----------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    import os, importlib.util
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

# ---------------- /silinecek ----------------
@Client.on_message(filters.command("silinecek") & filters.private & CustomFilters.owner)
async def delete_handler(client: Client, message: Message):
    args = message.text.split()
    if len(args) < 3:
        await message.reply_text("Kullanım: /silinecek tmdb <id> veya /silinecek imdb <id> [dosya]")
        return

    field, value = args[1], args[2]
    filename = args[3] if len(args) > 3 else None

    await message.reply_text(f"{field}={value} için silmeyi onaylayın. 60s içinde yazın: evet")

    try:
        def check(m):
            return m.from_user.id == message.from_user.id and m.text.lower() == "evet"

        confirm_msg = await asyncio.wait_for(
            client.listen(message.chat.id, filters=filters.text & filters.user(message.from_user.id)),
            timeout=60
        )
        if confirm_msg.text.lower() != "evet":
            await message.reply_text("Onay geçersiz. İşlem iptal edildi.")
            return

    except asyncio.TimeoutError:
        await message.reply_text("⏱ Süre doldu. İşlem iptal edildi.")
        return

    # ---------------- Silme Mantığı ----------------
    deleted_count = 0
    if field.lower() == "tmdb":
        if filename:  # Dizi bölümü silme
            res = series_col.update_many(
                { "tmdb_id": int(value), "seasons.episodes.filename": filename },
                { "$pull": { "seasons.$[].episodes": { "filename": filename } } }
            )
            deleted_count = res.modified_count
        else:  # Dizi tüm bölümler veya film
            if series_col.find_one({"tmdb_id": int(value)}):
                res = series_col.delete_many({"tmdb_id": int(value)})
                deleted_count = res.deleted_count
            elif movie_col.find_one({"tmdb_id": int(value)}):
                res = movie_col.delete_many({"tmdb_id": int(value)})
                deleted_count = res.deleted_count
    elif field.lower() == "imdb":
        if filename:  # Dizi bölümü silme
            res = series_col.update_many(
                { "imdb_id": value, "seasons.episodes.filename": filename },
                { "$pull": { "seasons.$[].episodes": { "filename": filename } } }
            )
            deleted_count = res.modified_count
        else:  # Dizi tüm bölümler veya film
            if series_col.find_one({"imdb_id": value}):
                res = series_col.delete_many({"imdb_id": value})
                deleted_count = res.deleted_count
            elif movie_col.find_one({"imdb_id": value}):
                res = movie_col.delete_many({"imdb_id": value})
                deleted_count = res.deleted_count

    await message.reply_text(f"✅ Silme işlemi tamamlandı. Toplam silinen kayıt/bölüm: {deleted_count}")
