import asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from pymongo import MongoClient
import os
import importlib.util
from Backend.helper.custom_filter import CustomFilters  # owner filtresi için

CONFIG_PATH = "/home/debian/dfbot/config.env"

# ---------------- Database ----------------
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
async def delete_content(client: Client, message: Message):
    # Komutu ayıkla: /silinecek tmdb 69315 veya /silinecek imdb tt12345
    args = message.text.split()
    if len(args) < 3:
        await message.reply_text("⚠️ Kullanım: /silinecek tmdb <id> veya /silinecek imdb <id> [dosya_adı]")
        return

    field, value = args[1], args[2]
    filename = args[3] if len(args) >= 4 else None

    await message.reply_text(f"⚠️ {field}={value} için silme işlemini onaylayın. Yazın: evet\n60 saniye içinde yanıt gelmezse iptal edilecektir.")

    def check(m: Message):
        return m.from_user.id == message.from_user.id and m.text.lower() == "evet"

    try:
        # 60 saniye bekle
        confirm = await client.listen(message.chat.id, filters=filters.text & filters.user(message.from_user.id), timeout=60)
        if confirm.text.lower() != "evet":
            await message.reply_text("❌ Onay geçersiz. İşlem iptal edildi.")
            return
    except asyncio.TimeoutError:
        await message.reply_text("⏱ 60 saniye içinde yanıt gelmedi. İşlem iptal edildi.")
        return

    # Film silme
    if movie_col.count_documents({field: value}) > 0:
        movie_col.delete_one({field: value})
        await message.reply_text(f"✅ Film kaydı silindi: {field}={value}")
        return

    # Dizi silme
    series_query = {field: value}
    series_docs = list(series_col.find(series_query))
    if not series_docs:
        await message.reply_text("⚠️ Dizi bulunamadı.")
        return

    # Eğer dosya adı belirtilmişse sadece o bölüm
    if filename:
        for doc in series_docs:
            seasons = doc.get("seasons", [])
            modified = False
            for season in seasons:
                episodes = season.get("episodes", [])
                new_eps = [ep for ep in episodes if ep.get("file_name") != filename]
                if len(new_eps) != len(episodes):
                    season["episodes"] = new_eps
                    modified = True
            if modified:
                series_col.update_one({"_id": doc["_id"]}, {"$set": {"seasons": seasons}})
        await message.reply_text(f"✅ Belirtilen bölüm silindi: {filename}")
    else:
        # Tüm diziyi sil
        series_col.delete_many(series_query)
        await message.reply_text(f"✅ Dizi kaydı silindi: {field}={value}")
