import os
import importlib.util
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
import uvicorn

# ==========================
# CONFIG OKUMA
# ==========================
CONFIG_PATH = "/home/debian/dfbot/config.env"

def load_config():
    config = {}
    if os.path.exists(CONFIG_PATH):
        spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for key in dir(module):
            if key.isupper():
                config[key] = getattr(module, key)

    # ENV fallback
    for key in ["API_ID", "API_HASH", "BOT_TOKEN", "OWNER_ID", "BASE_URL"]:
        config[key] = config.get(key) or os.getenv(key)

    return config


CFG = load_config()
OWNER_ID = int(CFG["OWNER_ID"])
BASE_URL = CFG["BASE_URL"]


# ==========================
# DURUM: Yayın Modu Açık mı?
# ==========================
yayin_modu = False

# ==========================
# PYROGRAM BOT
# ==========================
bot = Client(
    "uplink",
    api_id=int(CFG["API_ID"]),
    api_hash=CFG["API_HASH"],
    bot_token=CFG["BOT_TOKEN"],
    in_memory=True
)

# RAM Storage
ram_storage = {}


# ==========================
# /yayin KOMUTU
# ==========================
@bot.on_message(filters.command("yayin") & filters.private)
async def yayin_toggle(_: Client, msg: Message):
    global yayin_modu

    if msg.from_user.id != OWNER_ID:
        return await msg.reply("⛔ Bu özellik sadece owner için.")

    yayin_modu = not yayin_modu

    if yayin_modu:
        return await msg.reply("📡 <b>Yayın modu açıldı!</b>\nDosya gönder → link otomatik gelecek.", quote=True)
    else:
        return await msg.reply("🛑 <b>Yayın modu kapatıldı.</b>\nArtık dosya gönderince link üretilmeyecek.", quote=True)


# ==========================
# DOSYA GELİNCE → Link Üret
# ==========================
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(client: Client, msg: Message):
    if msg.from_user.id != OWNER_ID:
        return

    global yayin_modu

    # Yayın modu kapalıysa işlem yapma
    if not yayin_modu:
        return

    file = msg.document or msg.video or msg.audio
    file_id = file.file_id
    file_name = file.file_name

    ram_storage[file_id] = {
        "msg": msg,
        "size": file.file_size,
        "name": file_name
    }

    stream_url = f"{BASE_URL}/dl/{file_id}"

    return await msg.reply(
        f"✔ Yayın Linki Hazır!\n\n"
        f"📄 <b>{file_name}</b>\n"
        f"🔗 `{stream_url}`\n"
        f"⏳ RAM üzerinden stream yapılacak.",
        quote=True
    )


# ==========================
# FASTAPI
# ==========================
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/dl/{file_id}")
async def stream_file(file_id: str):
    if file_id not in ram_storage:
        return Response("Not Found", status_code=404)

    msg = ram_storage[file_id]["msg"]
    size = ram_storage[file_id]["size"]
    filename = ram_storage[file_id]["name"]

    async def iterfile():
        chunk_size = 1024 * 512  # 512 KB
        offset = 0

        while offset < size:
            chunk = await bot.download_media(
                msg,
                file_name=None,
                file_offset=offset,
                file_size=min(chunk_size, size - offset)
            )
            if not chunk:
                break

            yield chunk
            offset += len(chunk)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }

    return StreamingResponse(iterfile(), headers=headers)


# ==========================
# BOT + API BAŞLAT
# ==========================
async def main():
    await bot.start()
    print("Bot çalışıyor...")

    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)

    await server.serve()


asyncio.run(main())
