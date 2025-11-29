from pyrogram import filters, Client, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram, Config

from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB bağlantısı artık doğru kaynaktan alınıyor
mongo = AsyncIOMotorClient(Config.DATABASE)


async def get_database_stats():
    stats = []

    # storage_* veritabanlarını tara
    for name in await mongo.list_database_names():
        if name.startswith("storage_"):
            database = mongo[name]
            db_stats = await database.command("dbStats")

            movie_count = await database.movies.count_documents({})
            tv_count = await database.tv_shows.count_documents({})

            stats.append({
                "db_name": name,
                "movie_count": movie_count,
                "tv_count": tv_count,
                "storage": db_stats.get("storageSize", 0)
            })

    return stats


@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        # DB İSTATİSTİKLERİ
        db_stats = await get_database_stats()

        total_movies = sum(item["movie_count"] for item in db_stats)
        total_tv = sum(item["tv_count"] for item in db_stats)
        total_storage = sum(item["storage"] for item in db_stats)

        storage_mb = round(total_storage / 1024 / 1024, 2)

        # ADDON URL
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # MESAJ
        await message.reply_text(
            f"""
Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.

<b>Eklenti adresin:</b>
<code>{addon_url}</code>

📊 <b>Bot İstatistikleri</b>
🎬 Filmler: <b>{total_movies}</b>
📺 Diziler: <b>{total_tv}</b>
💾 Depolama: <b>{storage_mb} MB</b>
            """,
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Error: {e}")
        print(f"Error in /start handler: {e}")
