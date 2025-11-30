from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util
import re

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

# ------------ /m3uindir KOMUTU ------------
@Client.on_message(filters.command("m3uindir") & filters.private & CustomFilters.owner)
async def send_m3u_file(client, message: Message):
    start_msg = await message.reply_text("📝 filmlervediziler.m3u dosyası hazırlanıyor, lütfen bekleyin...")

    file_path = "filmlervediziler.m3u"

    try:
        with open(file_path, "w", encoding="utf-8") as m3u:
            m3u.write("#EXTM3U\n")

            # --------------------------------------------------------------------------------
            #                                       FİLMLER
            # --------------------------------------------------------------------------------
            for movie in db["movie"].find({}):
                title = movie.get("title", "Unknown Movie")
                logo = movie.get("poster", "")
                telegram_files = movie.get("telegram", [])

                for tg in telegram_files:
                    quality = tg.get("quality", "Unknown")
                    file_id = tg.get("id")
                    file_name = tg.get("name", "")
                    if not file_id:
                        continue

                    url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                    name = f"{title} [{quality}]"

                    # --- Yıl tespit ---
                    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", file_name)
                    if year_match:
                        year = int(year_match.group(1))
                        if year < 1950:
                            group = "1940’lar ve Öncesi Filmleri"
                        elif 1950 <= year <= 1959:
                            group = "1950’ler Filmleri"
                        elif 1960 <= year <= 1969:
                            group = "1960’lar Filmleri"
                        elif 1970 <= year <= 1979:
                            group = "1970’ler Filmleri"
                        elif 1980 <= year <= 1989:
                            group = "1980’ler Filmleri"
                        elif 1990 <= year <= 1999:
                            group = "1990’lar Filmleri"
                        elif 2000 <= year <= 2009:
                            group = "2000’ler Filmleri"
                        elif 2010 <= year <= 2019:
                            group = "2010’lar Filmleri"
                        elif 2020 <= year <= 2029:
                            group = "2020’ler Filmleri"
                        else:
                            group = "Filmler"
                    else:
                        group = "Filmler"

                    # M3U yaz
                    m3u.write(
                        f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" '
                        f'group-title="{group}",{name}\n'
                    )
                    m3u.write(f"{url}\n")

            # --------------------------------------------------------------------------------
            #                                       DİZİLER
            # --------------------------------------------------------------------------------
            for tv in db["tv"].find({}):
                title = tv.get("title", "Unknown TV")
                logo_tv = tv.get("poster", "")
                seasons = tv.get("seasons", [])

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
                            file_name = tg.get("name", "").lower()

                            if not file_id:
                                continue

                            url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                            name = f"{title} S{season_number:02d}E{ep_number:02d} [{quality}]"

                            # --- Dizi platform kategorisi ---
                            if "dsnp" in file_name:
                                group = "Disney Dizileri"
                            elif "nf" in file_name:
                                group = "Netflix Dizileri"
                            elif "exxen" in file_name:
                                group = "Exxen Dizileri"
                            elif "tabii" in file_name:
                                group = "Tabii Dizileri"
                            elif "hbo" in file_name or "hbomax" in file_name or "blutv" in file_name:
                                group = "Hbo Dizileri"
                            else:
                                group = "Diziler"

                            m3u.write(
                                f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" '
                                f'group-title="{group}",{name}\n'
                            )
                            m3u.write(f"{url}\n")


        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📂 filmlervediziler.m3u dosyanız hazır!"
        )
        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ Dosya oluşturulamadı.\nHata: {e}")
