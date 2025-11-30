# yayin.py

import os
import math
import secrets
import mimetypes
from typing import Tuple, Optional
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote

from fastapi import FastAPI, APIRouter, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from dotenv import load_dotenv

# ---------------- Load Config ----------------
load_dotenv()  # .env veya config.env dosyasından yükler

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

# ----------------- Telegram Bot -----------------
app_bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Custom Owner Filter
from pyrogram.filters import Filter

class OwnerFilter(Filter):
    async def __call__(self, client, message: Message):
        return message.from_user.id == OWNER_ID

CustomFiltersOwner = OwnerFilter()

@app_bot.on_message(filters.command("start") & filters.private & CustomFiltersOwner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        addon_url = f"{BASE_URL}/stremio/manifest.json"
        await message.reply_text(
            'Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.\n\n'
            f'<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n',
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )
    except Exception as e:
        await message.reply_text(f"⚠️ Error: {e}")
        print(f"Error in /start handler: {e}")

# ----------------- FastAPI App -----------------
app = FastAPI(title="Telegram Stremio Addon")

# Dummy Database & Media
db = {
    "movies": [],
    "tv_shows": []
}

PAGE_SIZE = 15
GENRES = ["Aile","Aksiyon","Animasyon","Belgesel","Biyografi","Bilim Kurgu",
          "Bilim Kurgu & Fantazi","Dram","Fantastik","Gerçeklik","Gerilim",
          "Gizem","Haber","Korku","Komedi","Macera","Müzik","Pembe Dizi",
          "Romantik","Savaş","Savaş & Politik","Spor","Suç","Talk","TV film",
          "Vahşi Batı","Western","Çocuklar"]

router = APIRouter(prefix="/stremio", tags=["Stremio Addon"])

# ----------------- Manifest -----------------
@router.get("/manifest.json")
async def get_manifest():
    return {
        "id": "telegram.media",
        "version": "1.0",
        "name": "Telegram Addon",
        "logo": "https://i.postimg.cc/XqWnmDXr/Picsart-25-10-09-08-09-45-867.png",
        "description": "Diziler ve filmler",
        "types": ["movie","series"],
        "resources": ["catalog","meta","stream"],
        "catalogs": [
            {"type":"movie","id":"latest_movies","name":"Latest",
             "extra":[{"name":"genre","isRequired":False,"options":GENRES},{"name":"skip"}],
             "extraSupported":["genre","skip"]},
            {"type":"series","id":"latest_series","name":"Latest",
             "extra":[{"name":"genre","isRequired":False,"options":GENRES},{"name":"skip"}],
             "extraSupported":["genre","skip"]}
        ],
        "idPrefixes":[""],
        "behaviorHints":{"configurable":False,"configurationRequired":False}
    }

app.include_router(router)

# ----------------- Media Streaming -----------------
def parse_range_header(range_header: str, file_size: int) -> Tuple[int,int]:
    if not range_header:
        return 0, file_size-1
    try:
        range_value = range_header.replace("bytes=","")
        from_str, until_str = range_value.split("-")
        from_bytes = int(from_str)
        until_bytes = int(until_str) if until_str else file_size-1
    except:
        raise HTTPException(status_code=400, detail="Invalid Range header")
    if (until_bytes>file_size-1) or (from_bytes<0) or (until_bytes<from_bytes):
        raise HTTPException(status_code=416, detail="Requested Range Not Satisfiable", headers={"Content-Range": f"bytes */{file_size}"})
    return from_bytes, until_bytes

# Dummy streamer
@app.get("/dl/{file_id}/{file_name}")
async def stream_file(request: Request, file_id: str, file_name: str):
    # Örnek: yerel test için bir dummy dosya
    file_path = f"./files/{file_name}"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    file_size = os.path.getsize(file_path)
    from_bytes, until_bytes = parse_range_header(request.headers.get("Range",""), file_size)
    chunk_size = 1024*1024
    offset = from_bytes-(from_bytes%chunk_size)
    first_cut = from_bytes-offset
    last_cut = (until_bytes%chunk_size)+1
    req_len = until_bytes-from_bytes+1

    def file_iterator():
        with open(file_path,"rb") as f:
            f.seek(offset)
            remaining = req_len
            while remaining>0:
                read_size = min(chunk_size, remaining)
                data = f.read(read_size)
                if not data:
                    break
                yield data
                remaining -= len(data)
    mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
    headers = {"Content-Type": mime_type,"Content-Length":str(req_len),"Content-Disposition": f'inline; filename="{file_name}"',"Accept-Ranges":"bytes"}
    if request.headers.get("Range"):
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
        return StreamingResponse(file_iterator(), status_code=206, headers=headers)
    return StreamingResponse(file_iterator(), headers=headers)

# ----------------- Bot Startup -----------------
if __name__ == "__main__":
    import asyncio
    import uvicorn

    # Run bot + API concurrently
    async def main():
        bot_task = asyncio.create_task(app_bot.start())
        api_task = asyncio.create_task(uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT",8000))))
        await asyncio.gather(bot_task, api_task)

    asyncio.run(main())
