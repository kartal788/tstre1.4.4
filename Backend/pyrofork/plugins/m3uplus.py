from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util
import tempfile

# ------------ CONFIG/ENV'DEN DEĞİŞKENLERİ ALMA ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return config

config = read_config()

# DATABASE URL (virgülle ayrılmış, ikinci URL kullanılacak)
db_raw = getattr(config, "DATABASE", "") or os.getenv("DATABASE", "")
db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")
MONGO_URL = db_urls[1]

# BASE_URL (M3U linkleri için)
BASE_URL = getattr(config, "BASE_URL", "") or os.getenv("BASE_URL", "")
if not BASE_URL:
    raise Exception("BASE_URL config veya env'de bulunamadı!")

# ------------ MONGO BAĞLANTISI ------------
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

# ------------ /m3uplus KOMUTU ------------
@Client.on_message(filters.command("m3uplus") & filters.private & CustomFilters.owner)
async def send_m3u(client, message: Message):
    start_msg = await message.reply_text("📝 M3U dosyası hazırlanıyor, lütfen bekleyin...")

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".m3u")
    tmp_file_path = tmp_file.name
    tmp_file.close()

    try:
        with open(tmp_file_path, "w", encoding="utf-8") as m3u:
            m3u.write("#EXTM3U\n")

            # --- Filmler (Movies grubu) ---
            for movie in db["movie"].find({}):
                title = movie.get("title", "Unknown Movie")
                logo = movie.get("poster", "")
                group = "Movies"
                telegram_files = movie.get("telegram", [])
                for tg in telegram_files:
                    quality = tg.get("quality", "Unknown")
                    file_id = tg.get("id")
                    if not file_id:
                        continue
                    url = f"{BASE_URL}/{file_id}/video.mkv"
                    name = f"{title} [{quality}]"
                    m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{group}",{name}\n')
                    m3u.write(f"{url}\n")

            # --- Diziler (TV Shows grubu) ---
            for tv in db["tv"].find({}):
                title = tv.get("title", "Unknown TV")
                logo = tv.get("poster", "")
                group = "TV Shows"
                telegram_files = tv.get("telegram", [])
                for tg in telegram_files:
                    quality = tg.get("quality", "Unknown")
                    file_id = tg.get("id")
                    if not file_id:
                        continue
                    url = f"{BASE_URL}/{file_id}/video.mkv"
                    name = f"{title} [{quality}]"
                    m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{group}",{name}\n')
                    m3u.write(f"{url}\n")

        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_file_path,
            caption="📂 M3U dosyanız hazır!"
        )
        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ M3U dosyası oluşturulamadı.\nHata: {e}")

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
