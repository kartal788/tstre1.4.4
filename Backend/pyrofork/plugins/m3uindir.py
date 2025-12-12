from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import re

# ------------ SADECE ENV'DEN AL ------------

db_raw = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]

BASE_URL = os.getenv("BASE_URL", "")
if not BASE_URL:
    raise Exception("BASE_URL ortam değişkeninde bulunamadı!")

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

            # -----------------------------
            # FILMLER
            # -----------------------------
            for movie in db["movie"].find({}):
                logo = movie.get("poster", "")
                telegram_files = movie.get("telegram", [])
                genres = movie.get("genres", [])

                for tg in telegram_files:
                    file_id = tg.get("id")
                    name = tg.get("name")
                    if not file_id or not name:
                        continue

                    url = f"{BASE_URL}/dl/{file_id}/video.mkv"

                    # --- Yıl kategorisi ---
                    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", name)
                    if year_match:
                        year = int(year_match.group(1))
                        if year < 1950:
                            year_group = "1940’lar ve Öncesi Filmleri"
                        elif 1950 <= year <= 1959:
                            year_group = "1950’ler Filmleri"
                        elif 1960 <= year <= 1969:
                            year_group = "1960’lar Filmleri"
                        elif 1970 <= year <= 1979:
                            year_group = "1970’ler Filmleri"
                        elif 1980 <= year <= 1989:
                            year_group = "1980’ler Filmleri"
                        elif 1990 <= year <= 1999:
                            year_group = "1990’lar Filmleri"
                        elif 2000 <= year <= 2009:
                            year_group = "2000’ler Filmleri"
                        elif 2010 <= year <= 2019:
                            year_group = "2010’lar Filmleri"
                        elif 2020 <= year <= 2029:
                            year_group = "2020’ler Filmleri"
                        else:
                            year_group = "Filmler"
                    else:
                        year_group = "Filmler"

                    m3u.write(
                        f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{year_group}",{name}\n'
                    )
                    m3u.write(f"{url}\n")

                    # --- Tür kategorileri ---
                    for genre in genres:
                        genre_group = f"{genre} Filmleri"
                        m3u.write(
                            f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{genre_group}",{name}\n'
                        )
                        m3u.write(f"{url}\n")

            # -----------------------------
            # DİZİLER
            # -----------------------------
            for tv in db["tv"].find({}):
                logo_tv = tv.get("poster", "")
                seasons = tv.get("seasons", [])

                for season in seasons:
                    episodes = season.get("episodes", [])

                    for ep in episodes:
                        logo = ep.get("episode_backdrop") or logo_tv
                        telegram_files = ep.get("telegram", [])

                        for tg in telegram_files:
                            file_id = tg.get("id")
                            name = tg.get("name")
                            if not file_id or not name:
                                continue

                            url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                            name_low = name.lower()

                            # --- Platform kategorileri ---
                            if "dsnp" in name_low:
                                group = "Disney Dizileri"
                            elif "nf" in name_low:
                                group = "Netflix Dizileri"
                            elif "exxen" in name_low:
                                group = "Exxen Dizileri"
                            elif "tabii" in name_low:
                                group = "Tabii Dizileri"
                            elif "hbo" in name_low or "hbomax" in name_low or "blutv" in name_low:
                                group = "Hbo Dizileri"
                            elif "amzn" in name_low:
                                group = "Amazon Dizileri"
                            elif "gain" in name_low:
                                group = "Gain Dizileri"
                            elif "tod" in name_low:
                                group = "Tod Dizileri"
                            else:
                                group = "Diziler"

                            m3u.write(
                                f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{group}",{name}\n'
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
