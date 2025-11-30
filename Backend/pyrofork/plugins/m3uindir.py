from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util

# ------------ CONFIG/ENV'DEN ALMA ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return config

config = read_config()
db_raw = getattr(config, "DATABASE", "") or os.getenv("DATABASE", "")
db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
BASE_URL = getattr(config, "BASE_URL", "") or os.getenv("BASE_URL", "")
if not BASE_URL:
    raise Exception("BASE_URL config veya env'de bulunamadı!")

# ------------ MONGO BAĞLANTISI ------------
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

# ------------ Platform Bazlı Kategoriler ------------
PLATFORM_CATEGORIES = {
    "dsnp": "Disney Filmleri/Dizileri",
    "disney": "Disney Filmleri/Dizileri",
    "nf": "Netflix Filmleri/Dizileri",
    "netflix": "Netflix Filmleri/Dizileri",
    "exxen": "Exxen Filmleri/Dizileri",
    "tabii": "Tabii Filmleri/Dizileri",
    "blutv": "HBO Filmleri/Dizileri",
    "hbo": "HBO Filmleri/Dizileri",
    "hbomax": "HBO Filmleri/Dizileri",
    "appletv": "Apple Filmleri/Dizileri",
    "apple": "Apple Filmleri/Dizileri"
}

# ------------ /m3uindir KOMUTU ------------
@Client.on_message(filters.command("m3uindir") & filters.private & CustomFilters.owner)
async def send_m3u_file(client, message: Message):
    start_msg = await message.reply_text("📝 filmlervediziler.m3u dosyası hazırlanıyor, lütfen bekleyin...")

    file_path = "filmlervediziler.m3u"

    try:
        with open(file_path, "w", encoding="utf-8") as m3u:
            m3u.write("#EXTM3U\n")

            # --- Filmler ---
            for movie in db["movie"].find({}):
                title = movie.get("title", "Unknown Movie")
                logo = movie.get("poster", "")
                genres = movie.get("genres", [])
                telegram_files = movie.get("telegram", [])

                for tg in telegram_files:
                    quality = tg.get("quality", "Unknown")
                    file_id = tg.get("id")
                    if not file_id:
                        continue
                    url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                    name = f"{title} [{quality}]"

                    # Önce platform bazlı kategori
                    platform_group = None
                    for key, group_name in PLATFORM_CATEGORIES.items():
                        if key.lower() in title.lower():
                            platform_group = group_name
                            break
                    if platform_group:
                        m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{platform_group}",{name}\n')
                        m3u.write(f"{url}\n")

                    # Ardından tür bazlı kategori
                    if genres:
                        for genre in genres:
                            m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre} Filmleri",{name}\n')
                            m3u.write(f"{url}\n")
                    else:
                        m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="Filmler",{name}\n')
                        m3u.write(f"{url}\n")

            # --- Diziler ---
            for tv in db["tv"].find({}):
                title = tv.get("title", "Unknown TV")
                seasons = tv.get("seasons", [])
                logo_tv = tv.get("poster", "")

                for season in seasons:
                    season_number = season.get("season_number", 1)
                    episodes = season.get("episodes", [])

                    for ep in episodes:
                        ep_number = ep.get("episode_number", 1)
                        logo = ep.get("episode_backdrop") or logo_tv
                        telegram_files = ep.get("telegram", [])

                        for tg in telegram_files:
                            quality = tg.get("quality", "Unknown")
                            file_id = tg.get("id")
                            if not file_id:
                                continue
                            url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                            name = f"{title} S{season_number:02d}E{ep_number:02d} [{quality}]"

                            # Önce platform bazlı kategori
                            platform_group = None
                            for key, group_name in PLATFORM_CATEGORIES.items():
                                if key.lower() in title.lower():
                                    platform_group = group_name
                                    break
                            if platform_group:
                                m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{platform_group}",{name}\n')
                                m3u.write(f"{url}\n")

                            # Ardından tür bazlı kategori
                            m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="Diziler",{name}\n')
                            m3u.write(f"{url}\n")

        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📂 filmlervediziler.m3u dosyanız hazır!"
        )
        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ Dosya oluşturulamadı.\nHata: {e}")
