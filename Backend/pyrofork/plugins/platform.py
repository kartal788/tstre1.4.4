import asyncio
import time
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util

# -----------------------
stop_event = asyncio.Event()

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
# -----------------------

@Client.on_message(filters.command("platform") & filters.private & CustomFilters.owner)
async def platform_duzelt(client: Client, message):
    stop_event.clear()

    start_msg = await message.reply_text(
        "🔧 Platform türleri güncelleniyor…\nİlerleme tek mesajda gösterilecektir.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
    )

    platform_genre_map = {
        "MAX": "Max",
        "Hbomax": "Max",
        "NF": "Netflix",
        "DSNP": "Disney",
        "Tod": "Tod",
        "Blutv": "Max",
        "Tv+": "Tv+",
        "Exxen": "Exxen",
        "Gain": "Gain",
        "HBO": "Max",
        "Tabii": "Tabii",
        "AMZN": "Amazon",
    }

    collections = [
        (movie_col, "Filmler"),
        (series_col, "Diziler")
    ]

    total_fixed = 0
    last_update = 0

    for col, name in collections:
        ids_cursor = col.find({}, {"_id": 1, "telegram": 1, "genres": 1, "seasons": 1})
        ids = [d["_id"] for d in ids_cursor]
        idx = 0

        while idx < len(ids):
            if stop_event.is_set():
                break

            doc_id = ids[idx]
            doc = col.find_one({"_id": doc_id})
            genres = doc.get("genres", [])
            updated = False

            # Movie / dizi telegram listesi
            for t in doc.get("telegram", []):
                name_field = t.get("name", "").lower()
                for key, genre_name in platform_genre_map.items():
                    if key.lower() in name_field and genre_name not in genres:
                        genres.append(genre_name)
                        updated = True

            # TV ise seasons -> episodes -> telegram
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name", "").lower()
                        for key, genre_name in platform_genre_map.items():
                            if key.lower() in name_field and genre_name not in genres:
                                genres.append(genre_name)
                                updated = True

            if updated:
                col.update_one({"_id": doc_id}, {"$set": {"genres": genres}})
                total_fixed += 1

            idx += 1

            if time.time() - last_update > 5:
                try:
                    await start_msg.edit_text(
                        f"{name}: Güncellenen kayıtlar: {total_fixed}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]),
                    )
                except:
                    pass
                last_update = time.time()

    try:
        await start_msg.edit_text(
            f"✅ Platform tür güncellemesi tamamlandı.\n\n"
            f"Toplam değiştirilen kayıt: {total_fixed}",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    except:
        pass
