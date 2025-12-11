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

# -----------------------------
#  UNIVERSAL ID EXTRACTOR
# -----------------------------
def extract_id(raw):
    raw = raw.strip()

    tg_match = re.search(r"/dl/([A-Za-z0-9]+)", raw)
    if tg_match:
        return tg_match.group(1), "telegram"

    if len(raw) > 30 and raw.isalnum():
        return raw, "telegram"

    if raw.lower().startswith("tt"):
        return raw, "imdb"

    stremio = re.search(r"/detail/(movie|series)/(\d+)-", raw)
    if stremio:
        tmdb_id = stremio.group(2)
        imdb_id = f"tt{tmdb_id}"
        return {"tmdb": tmdb_id, "imdb": imdb_id}, "stremio"

    if raw.isdigit():
        return raw, "tmdb"

    return raw, "filename"


# ==============================
#       /vsil KOMUTU
# ==============================
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command) < 2:
        await message.reply_text("⚠️ Kullanım:\n/vsil id/link/dosya", quote=True)
        return

    raw_arg = message.command[1]
    arg, arg_type = extract_id(raw_arg)

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.", quote=True)
            return

        mongo = MongoClient(db_urls[1])
        db = mongo[mongo.list_database_names()[0]]
        deleted_files = []

        # ----- STREMIO FALLBACK -----
        if arg_type == "stremio":
            tmdb_id = arg["tmdb"]
            imdb_id = arg["imdb"]

            movie_tmdb = db["movie"].count_documents({"tmdb_id": int(tmdb_id)})
            tv_tmdb = db["tv"].count_documents({"tmdb_id": int(tmdb_id)})

            if movie_tmdb or tv_tmdb:
                arg = tmdb_id
                arg_type = "tmdb"
            else:
                arg = imdb_id
                arg_type = "imdb"

        # ----- TMDB -----
        if arg_type == "tmdb":
            tmdb_id = int(arg)

            movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    deleted_files.append(t.get("name"))
                db["movie"].delete_one({"_id": doc["_id"]})

            tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            deleted_files.append(t.get("name"))
                db["tv"].delete_one({"_id": doc["_id"]})

        # ----- IMDb -----
        elif arg_type == "imdb":
            imdb_id = arg

            movie_docs = list(db["movie"].find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    deleted_files.append(t.get("name"))
                db["movie"].delete_one({"_id": doc["_id"]})

            tv_docs = list(db["tv"].find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            deleted_files.append(t.get("name"))
                db["tv"].delete_one({"_id": doc["_id"]})

        # ----- Tekil dosya silme -----
        else:
            target = arg

            movie_docs = list(db["movie"].find({}))
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                new_list = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]

                removed = [t.get("name") for t in telegram_list if t not in new_list]
                deleted_files += removed

                if not new_list:
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new_list
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

            tv_docs = list(db["tv"].find({}))
            for doc in tv_docs:
                seasons_to_remove = []

                for season in doc.get("seasons", []):
                    episodes_to_remove = []

                    for ep in season.get("episodes", []):
                        tlist = ep.get("telegram", [])
                        new_tlist = [t for t in tlist if t.get("id") != target and t.get("name") != target]

                        removed = [t.get("name") for t in tlist if t not in new_tlist]
                        deleted_files += removed

                        if new_tlist:
                            ep["telegram"] = new_tlist
                        else:
                            episodes_to_remove.append(ep)

                    for ep in episodes_to_remove:
                        season["episodes"].remove(ep)

                    if not season["episodes"]:
                        seasons_to_remove.append(season)

                for s in seasons_to_remove:
                    doc["seasons"].remove(s)

                if not doc.get("seasons"):
                    db["tv"].delete_one({"_id": doc["_id"]})
                else:
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

        # ----- SONUÇ -----
        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir dosya bulunamadı.", quote=True)
            return

        if len(deleted_files) > 10:
            file_path = f"/tmp/silinen_{int(time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(message.chat.id, file_path,
                                       caption=f"🗑 {len(deleted_files)} dosya silindi.")
        else:
            await message.reply_text("🗑 Silinen dosyalar:\n\n" + "\n".join(deleted_files), quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)



# ==============================
#        /vbilgi KOMUTU
# ==============================
@Client.on_message(filters.command("vbilgi") & filters.private & CustomFilters.owner)
async def vbilgi(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command) < 2:
        await message.reply_text("📌 Kullanım:\n/vbilgi id/link/dosya", quote=True)
        return

    raw_arg = message.command[1]
    arg, arg_type = extract_id(raw_arg)

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ Veritabanı bulunamadı.", quote=True)
            return

        mongo = MongoClient(db_urls[1])
        db = mongo[mongo.list_database_names()[0]]

        # ----- STREMIO FALLBACK -----
        if arg_type == "stremio":
            tmdb_id = arg["tmdb"]
            imdb_id = arg["imdb"]

            movie_tmdb = db["movie"].count_documents({"tmdb_id": int(tmdb_id)})
            tv_tmdb = db["tv"].count_documents({"tmdb_id": int(tmdb_id)})

            if movie_tmdb or tv_tmdb:
                arg = tmdb_id
                arg_type = "tmdb"
            else:
                arg = imdb_id
                arg_type = "imdb"

        result_text = ""

        # ----- TMDB -----
        if arg_type == "tmdb":
            tmdb_id = int(arg)

            movie = db["movie"].find_one({"tmdb_id": tmdb_id})
            tv = db["tv"].find_one({"tmdb_id": tmdb_id})

            if movie:
                result_text += f"🎬 *Film*\nTMDB: `{tmdb_id}`\nIMDb: `{movie.get('imdb_id')}`\n"
                for t in movie.get("telegram", []):
                    result_text += f"— {t.get('name')}\n"

            if tv:
                result_text += f"\n📺 *Dizi*\nTMDB: `{tmdb_id}`\nIMDb: `{tv.get('imdb_id')}`\n"
                for s in tv.get("seasons", []):
                    result_text += f"Sezon {s['season_number']}:\n"
                    for e in s["episodes"]:
                        result_text += f"  Bölüm {e['episode_number']}:\n"
                        for t in e["telegram"]:
                            result_text += f"    — {t['name']}\n"

        # ----- IMDb -----
        elif arg_type == "imdb":
            imdb_id = arg

            movie = db["movie"].find_one({"imdb_id": imdb_id})
            tv = db["tv"].find_one({"imdb_id": imdb_id})

            if movie:
                result_text += f"🎬 *Film*\nIMDb: `{imdb_id}`\nTMDB: `{movie.get('tmdb_id')}`\n"
                for t in movie.get("telegram", []):
                    result_text += f"— {t.get('name')}\n"

            if tv:
                result_text += f"\n📺 *Dizi*\nIMDb: `{imdb_id}`\nTMDB: `{tv.get('tmdb_id')}`\n"
                for s in tv["seasons"]:
                    result_text += f"Sezon {s['season_number']}:\n"
                    for e in s["episodes"]:
                        result_text += f"  Bölüm {e['episode_number']}:\n"
                        for t in e["telegram"]:
                            result_text += f"    — {t['name']}\n"

        # ----- Tekil dosya -----
        else:
            target = arg

            movie_docs = list(db["movie"].find({}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    if t.get("id") == target or t.get("name") == target:
                        result_text += (
                            f"🎬 Film\nTMDB: `{doc.get('tmdb_id')}` "
                            f"IMDb: `{doc.get('imdb_id')}`\n"
                            f"Dosya: {t.get('name')}\n"
                        )

            tv_docs = list(db["tv"].find({}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            if t.get("id") == target or t.get("name") == target:
                                result_text += (
                                    f"📺 Dizi\nTMDB: `{doc.get('tmdb_id')}` "
                                    f"IMDb: `{doc.get('imdb_id')}`\n"
                                    f"Sezon: {s.get('season_number')} "
                                    f"Bölüm: {e.get('episode_number')}\n"
                                    f"Dosya: {t.get('name')}\n"
                                )

        if not result_text:
            await message.reply_text("⚠️ Bilgi bulunamadı.", quote=True)
        else:
            await message.reply_text(result_text, quote=True)

    except Exception as e:
        await message.reply_text(f"Hata: {e}", quote=True)
        print("vbilgi hata:", e)



# ==============================
#         /vtest  (YENİ)
# ==============================
@Client.on_message(filters.command("vtest") & filters.private & CustomFilters.owner)
async def vtest(client: Client, message: Message):
    """
    /vsil ile aynı işlemleri yapar AMA HİÇBİR ŞEY SİLMEZ.
    Sadece silinecek dosyaların listesini verir.
    """
    user_id = message.from_user.id
    now = time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command) < 2:
        await message.reply_text("📌 Kullanım: /vtest id/link/dosya", quote=True)
        return

    raw_arg = message.command[1]
    arg, arg_type = extract_id(raw_arg)

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ Veritabanı bulunamadı.", quote=True)
            return

        mongo = MongoClient(db_urls[1])
        db = mongo[mongo.list_database_names()[0]]

        to_delete = []

        # ---- STREMIO FALLBACK ----
        if arg_type == "stremio":
            tmdb_id = arg["tmdb"]
            imdb_id = arg["imdb"]

            movie_tmdb = db["movie"].count_documents({"tmdb_id": int(tmdb_id)})
            tv_tmdb = db["tv"].count_documents({"tmdb_id": int(tmdb_id)})

            if movie_tmdb or tv_tmdb:
                arg = tmdb_id
                arg_type = "tmdb"
            else:
                arg = imdb_id
                arg_type = "imdb"

        # ----- TMDB -----
        if arg_type == "tmdb":
            tmdb_id = int(arg)

            movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    to_delete.append(t.get("name"))

            tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            to_delete.append(t.get("name"))

        # ----- IMDb -----
        elif arg_type == "imdb":
            imdb_id = arg

            movie_docs = list(db["movie"].find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    to_delete.append(t.get("name"))

            tv_docs = list(db["tv"].find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            to_delete.append(t.get("name"))

        # ----- Tekil dosya -----
        else:
            target = arg

            movie_docs = list(db["movie"].find({}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    if t.get("id") == target or t.get("name") == target:
                        to_delete.append(t.get("name"))

            tv_docs = list(db["tv"].find({}))
            for doc in tv_docs:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            if t.get("id") == target or t.get("name") == target:
                                to_delete.append(t.get("name"))

        # ---- SONUÇ ----
        if not to_delete:
            await message.reply_text("⚠️ Silinecek bir şey bulunamadı.", quote=True)
            return

        # Gönderirken düz metin olarak gönderiyoruz; parse_mode kaldırıldı
        preview_text = "🧪 Test modu\nBu dosyalar SİLİNECEK (ama /vtest hiçbir şeyi silmez):\n\n" + "\n".join(to_delete)
        await message.reply_text(preview_text, quote=True)

    except Exception as e:
        await message.reply_text(f"Hata: {e}", quote=True)
        print("vtest hata:", e)
