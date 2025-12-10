from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os, re
from dotenv import load_dotenv
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

flood_wait = 5
last_command_time = {}

# ---------------------------
#  ID / LINK PARSER
# ---------------------------
def parse_arg(raw):
    raw = raw.strip()

    # Telegram link
    tg_match = re.search(r"/dl/([A-Za-z0-9_-]+)", raw)
    if tg_match:
        return tg_match.group(1), "telegram"

    # Telegram ID directly
    if len(raw) > 30 and raw.isalnum():
        return raw, "telegram"

    # IMDb
    if raw.lower().startswith("tt"):
        return raw, "imdb"

    # Stremio movie/series link
    stremio = re.search(r"/detail/(movie|series)/(\d+)-", raw)
    if stremio:
        return stremio.group(2), "tmdb"

    # TMDB numeric
    if raw.isdigit():
        return raw, "tmdb"

    # Fallback → treat as filename/id
    return raw, "filename"


# ---------------------------
#  Helper Functions
# ---------------------------
def find_movies(db, arg, arg_type):
    deleted = []
    if arg_type in ["tmdb", "imdb"]:
        key = "tmdb_id" if arg_type == "tmdb" else "imdb_id"
        docs = list(db["movie"].find({key: int(arg) if arg_type=="tmdb" else arg}))
        for doc in docs:
            deleted += [t.get("name") for t in doc.get("telegram",[])]
        return docs, deleted
    else:  # telegram_id or filename
        docs = list(db["movie"].find({}))
        for doc in docs:
            tlist = doc.get("telegram", [])
            match = [t for t in tlist if t.get("id")==arg or t.get("name")==arg]
            deleted += [t.get("name") for t in match]
        return docs, deleted

def find_tv(db, arg, arg_type):
    deleted = []
    if arg_type in ["tmdb","imdb"]:
        key = "tmdb_id" if arg_type=="tmdb" else "imdb_id"
        docs = list(db["tv"].find({key: int(arg) if arg_type=="tmdb" else arg}))
        for doc in docs:
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    deleted += [t.get("name") for t in ep.get("telegram",[])]
        return docs, deleted
    else:  # telegram_id or filename
        docs = list(db["tv"].find({}))
        for doc in docs:
            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    tlist = ep.get("telegram", [])
                    match = [t for t in tlist if t.get("id")==arg or t.get("name")==arg]
                    deleted += [t.get("name") for t in match]
        return docs, deleted


# ---------------------------
#  CORE VSIL FUNCTION
# ---------------------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def vsil_cmd(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()
    if user_id in last_command_time and now - last_command_time[user_id]<flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command)<2:
        await message.reply_text("⚠️ Kullanım: /vsil <tmdb | imdb | telegram_id | dosya adı | stremio link>", quote=True)
        return

    raw_arg = message.command[1]
    arg, arg_type = parse_arg(raw_arg)

    try:
        client_db = MongoClient(db_urls[1])
        db = client_db[client_db.list_database_names()[0]]

        deleted_files = []

        # MOVIE
        movie_docs, del_movie = find_movies(db, arg, arg_type)
        deleted_files += del_movie

        # TV
        tv_docs, del_tv = find_tv(db, arg, arg_type)
        deleted_files += del_tv

        # -------- DELETE MOVIE --------
        for doc in movie_docs:
            if arg_type in ["tmdb","imdb"]:
                db["movie"].delete_one({"_id": doc["_id"]})
            else:
                tlist = doc.get("telegram", [])
                new_tlist = [t for t in tlist if t.get("id")!=arg and t.get("name")!=arg]
                if not new_tlist:
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new_tlist
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

        # -------- DELETE TV --------
        for doc in tv_docs:
            modified = False
            seasons_to_remove = []
            for season in doc.get("seasons", []):
                eps_to_remove = []
                for ep in season.get("episodes", []):
                    tlist = ep.get("telegram", [])
                    new_tlist = [t for t in tlist if t.get("id")!=arg and t.get("name")!=arg]
                    if new_tlist:
                        ep["telegram"] = new_tlist
                    else:
                        eps_to_remove.append(ep)
                    if len(new_tlist)!=len(tlist):
                        modified = True
                for ep in eps_to_remove:
                    season["episodes"].remove(ep)
                if not season["episodes"]:
                    seasons_to_remove.append(season)
            for s in seasons_to_remove:
                doc["seasons"].remove(s)
            if not doc.get("seasons"):
                db["tv"].delete_one({"_id": doc["_id"]})
            elif modified:
                db["tv"].replace_one({"_id": doc["_id"]}, doc)

        # -------- RESULT MESSAGE --------
        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir dosya bulunamadı.", quote=True)
            return

        if len(deleted_files)>10:
            file_path = f"/tmp/silinen_{int(time())}.txt"
            with open(file_path,"w",encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(message.chat.id,file_path,caption=f"🗑 {len(deleted_files)} dosya silindi.")
        else:
            await message.reply_text(f"🗑 Silinen {len(deleted_files)} dosya:\n\n" + "\n".join(deleted_files))

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("vsil hata:", e)


# ---------------------------
#  HELPER COMMANDS
# ---------------------------

async def get_deleted_info(db, arg, arg_type):
    movie_docs, del_movie = find_movies(db, arg, arg_type)
    tv_docs, del_tv = find_tv(db, arg, arg_type)
    deleted_files = del_movie + del_tv
    return deleted_files, movie_docs, tv_docs


@Client.on_message(filters.command("vsilbilgi") & filters.private & CustomFilters.owner)
async def vsil_info(client: Client, message: Message):
    if len(message.command)<2:
        await message.reply_text("⚠️ Kullanım: /vsilinfo <tmdb | imdb | telegram_id | dosya adı | stremio link>", quote=True)
        return
    raw_arg = message.command[1]
    arg, arg_type = parse_arg(raw_arg)
    try:
        client_db = MongoClient(db_urls[1])
        db = client_db[client_db.list_database_names()[0]]
        deleted_files, movie_docs, tv_docs = await get_deleted_info(db,arg,arg_type)

        text = ""
        if movie_docs:
            for doc in movie_docs:
                text += f"🎬 Movie: {doc.get('title','N/A')} → {len(doc.get('telegram',[]))} dosya\n"
        if tv_docs:
            for doc in tv_docs:
                seasons = doc.get("seasons",[])
                eps = sum([len(s.get("episodes",[])) for s in seasons])
                text += f"📺 TV: {doc.get('title','N/A')} → {len(seasons)} sezon, {eps} bölüm\n"
        if not text:
            text = "⚠️ Hiçbir medya bulunamadı."
        await message.reply_text(text)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")


@Client.on_message(filters.command("vsiltest") & filters.private & CustomFilters.owner)
async def vsil_test(client: Client, message: Message):
    if len(message.command)<2:
        await message.reply_text("⚠️ Kullanım: /vsiltest <tmdb | imdb | telegram_id | dosya adı | stremio link>", quote=True)
        return
    raw_arg = message.command[1]
    arg, arg_type = parse_arg(raw_arg)
    try:
        client_db = MongoClient(db_urls[1])
        db = client_db[client_db.list_database_names()[0]]
        deleted_files, movie_docs, tv_docs = await get_deleted_info(db,arg,arg_type)
        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir dosya bulunamadı.", quote=True)
        else:
            await message.reply_text(f"📝 Simülasyon: {len(deleted_files)} dosya silinecek:\n" + "\n".join(deleted_files))

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")


@Client.on_message(filters.command("vsilsay") & filters.private & CustomFilters.owner)
async def vsil_count(client: Client, message: Message):
    if len(message.command)<2:
        await message.reply_text("⚠️ Kullanım: /vsilcount <tmdb | imdb | telegram_id | dosya adı | stremio link>", quote=True)
        return
    raw_arg = message.command[1]
    arg, arg_type = parse_arg(raw_arg)
    try:
        client_db = MongoClient(db_urls[1])
        db = client_db[client_db.list_database_names()[0]]
        deleted_files, movie_docs, tv_docs = await get_deleted_info(db,arg,arg_type)
        await message.reply_text(f"ℹ️ Toplam {len(deleted_files)} dosya bulundu.")
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")


@Client.on_message(filters.command("vsilliste") & filters.private & CustomFilters.owner)
async def vsil_list(client: Client, message: Message):
    if len(message.command)<2:
        await message.reply_text("⚠️ Kullanım: /vsillist <tmdb | imdb | telegram_id | dosya adı | stremio link>", quote=True)
        return
    raw_arg = message.command[1]
    arg, arg_type = parse_arg(raw_arg)
    try:
        client_db = MongoClient(db_urls[1])
        db = client_db[client_db.list_database_names()[0]]
        deleted_files, _, _ = await get_deleted_info(db,arg,arg_type)
        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir dosya bulunamadı.", quote=True)
        else:
            text = "\n".join(deleted_files)
            await message.reply_text(f"📄 Dosya listesi ({len(deleted_files)} dosya):\n{text}")
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
