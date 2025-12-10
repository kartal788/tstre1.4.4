import asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from pymongo import MongoClient
import os
import importlib.util

from Backend.helper.custom_filter import CustomFilters  # Owner filtresi

CONFIG_PATH = "/home/debian/dfbot/config.env"

# ---------------- DATABASE ----------------
def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
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

# ---------------- /silinecek Komutu ----------------
@Client.on_message(filters.command("silinecek") & filters.private & CustomFilters.owner)
async def silinecek_cmd(client: Client, message: Message):
    args = message.text.split()
    if len(args) < 3:
        await message.reply_text("⚠️ Kullanım: /silinecek tmdb <id> | imdb <id> | dosya <dosya_adı>")
        return

    sil_type = args[1].lower()
    sil_value = args[2]

    if sil_type not in ["tmdb", "imdb", "dosya"]:
        await message.reply_text("⚠️ Geçersiz tür. tmdb, imdb veya dosya kullanın.")
        return

    # ---------------- Hangi kayıt silinecek, kullanıcıya sor ----------------
    if sil_type == "tmdb":
        text = f"❗ TMDB ID `{sil_value}` olan {'film' if movie_col.count_documents({'tmdb_id': int(sil_value)}) else 'dizi'} kaydı silinsin mi? 60 sn içinde 'Evet' veya 'Hayır' yazın."
    elif sil_type == "imdb":
        text = f"❗ IMDB ID `{sil_value}` olan {'film' if movie_col.count_documents({'imdb_id': sil_value}) else 'dizi'} kaydı silinsin mi? 60 sn içinde 'Evet' veya 'Hayır' yazın."
    else:
        text = f"❗ Dosya adı `{sil_value}` olan bölüm silinsin mi? 60 sn içinde 'Evet' veya 'Hayır' yazın."

    prompt_msg = await message.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN)

    # ---------------- Kullanıcı yanıtını bekle ----------------
    try:
        reply: Message = await client.listen(message.chat.id, timeout=60)
    except asyncio.TimeoutError:
        await prompt_msg.edit_text("⏰ 60 saniye geçti, işlem iptal edildi!")
        return

    cevap = reply.text.lower()
    if cevap != "evet":
        await prompt_msg.edit_text("❌ İşlem iptal edildi!")
        return

    # ---------------- Silme işlemi ----------------
    deleted_count = 0

    try:
        if sil_type == "tmdb":
            # Film kontrolü
            deleted_count = movie_col.delete_many({"tmdb_id": int(sil_value)}).deleted_count
            # Dizi kontrolü (tüm bölümler)
            deleted_count += series_col.update_many({"tmdb_id": int(sil_value)}, {"$set": {"seasons": []}}).modified_count

        elif sil_type == "imdb":
            deleted_count = movie_col.delete_many({"imdb_id": sil_value}).deleted_count
            deleted_count += series_col.update_many({"imdb_id": sil_value}, {"$set": {"seasons": []}}).modified_count

        else:  # dosya
            # Sadece o bölümü sil
            series_docs = series_col.find({"seasons.episodes.file_name": sil_value})
            for doc in series_docs:
                seasons = doc.get("seasons", [])
                modified = False
                for season in seasons:
                    eps = season.get("episodes", [])
                    season["episodes"] = [ep for ep in eps if ep.get("file_name") != sil_value]
                    if len(season["episodes"]) != len(eps):
                        modified = True
                if modified:
                    series_col.update_one({"_id": doc["_id"]}, {"$set": {"seasons": seasons}})
                    deleted_count += 1

        await prompt_msg.edit_text(f"✅ Silme işlemi tamamlandı. Toplam silinen kayıt/bölüm: {deleted_count}")
    except Exception as e:
        await prompt_msg.edit_text(f"⚠️ Silme sırasında hata: {e}")
