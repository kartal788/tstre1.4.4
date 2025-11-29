from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

from Backend.logger import LOGGER
from Backend.config import Telegram
from Backend.helper.database import DBManager # DBManager'ın get_instance() ile erişim sağladığını varsayar
from Backend.helper.modal import User
from Backend.helper.utils import get_formatted_bot_stats # Basit bir istatistik fonksiyonu varsayar

# Kullanılan ana database instance'ı
try:
    DB = DBManager.get_instance()
except Exception as e:
    LOGGER.error(f"Failed to get DB instance at module load: {e}")
    DB = None 

# Botun başlangıç mesajı
START_TEXT = """
👋 **Merhaba, ben {bot_name}!**

Ben, güçlü bir medya içeriği yönetim botuyum. 
Büyük bir film ve dizi arşivini yönetmek, 
yeni içerikleri otomatik olarak indekslemek ve 
kullanıcılara hızlı erişim sağlamak için tasarlandım.

✨ **Özellikler:**
* **Çoklu Veritabanı Desteği:** Sınırsız depolama için birden fazla MongoDB bağlantısını yönetir.
* **Akıllı İndeksleme:** TMDB/IMDB bilgileriyle filmleri ve dizileri otomatik olarak indeksler.
* **Hızlı Arama:** Arşivde anında sonuçlar sunar.
* **Kullanıcı Yönetimi:** Kullanıcıları kaydeder ve yetkilendirir.

🤖 **Kullanım:**
* `/start` - Bu mesajı gösterir.
* `/stats` - Botun genel istatistiklerini (kayıtlı medya sayısı, kullanıcı sayısı) gösterir.
* `/search <sorgu>` - Arşivde arama yapar. (Yönetici komutu olarak da kullanılabilir)
* `/addmovie <tmdb_id> <chat_id> <msg_id>` - Yeni bir film ekler (Yönetici Komutu).
* `/addtv <tmdb_id> <chat_id> <msg_id>` - Yeni bir dizi bölümü ekler (Yönetici Komutu).

⚙️ **Botunuzu tamamen kurmak için admin komutlarına göz atın!**
"""

# Başlangıç mesajının altındaki butonlar
START_BUTTONS = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton(text="🔎 Hızlı Arama", switch_inline_query_current_chat=""),
            InlineKeyboardButton(text="⚙️ Ayarlar", callback_data="settings_menu")
        ],
        [
            InlineKeyboardButton(text="👨‍💻 Geliştirici", url=Telegram.DEV_CONTACT),
            InlineKeyboardButton(text="📢 Kanal", url=Telegram.BOT_CHANNEL)
        ]
    ]
)

@Client.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    """
    /start komutunu işler. Kullanıcıyı kaydeder/günceller ve hoş geldin mesajını gönderir.
    """
    if DB is None:
        await message.reply_text("⛔ Veritabanı bağlantısı kurulamadı. Lütfen logları kontrol edin.")
        LOGGER.error(f"Database instance is None. Cannot process /start for user {message.from_user.id}")
        return

    user_id = message.from_user.id
    
    # 1. Kullanıcıyı veritabanına kaydet/güncelle
    try:
        user_data = User(
            user_id=user_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            is_premium=message.from_user.is_premium or False,
            last_active=datetime.utcnow(),
            is_admin=user_id in Telegram.ADMINS # Admin listesinden kontrol
        )
        
        # Kullanıcıyı DB'ye ekle veya mevcut bilgileri güncelle
        await DB.add_or_update_user(user_data)
        
    except Exception as e:
        LOGGER.error(f"Error adding/updating user {user_id} in DB: {e}")
        # Hata olsa bile kullanıcıya mesaj göndermeye devam et

    # 2. Hoş geldin mesajını gönder
    bot_name = (await client.get_me()).first_name
    
    await message.reply_text(
        START_TEXT.format(bot_name=bot_name),
        reply_markup=START_BUTTONS,
        parse_mode=ParseMode.MARKDOWN
    )
    
    LOGGER.info(f"User {user_id} started the bot.")


@Client.on_message(filters.command("stats") & filters.user(Telegram.ADMINS))
async def stats_command(client: Client, message: Message):
    """
    /stats komutunu işler. Yalnızca yöneticilerin kullanımına açıktır.
    """
    if DB is None:
        await message.reply_text("⛔ Veritabanı bağlantısı kurulamadı.")
        return

    try:
        stats_text = await get_formatted_bot_stats(DB) # İstatistikleri toplayıp biçimlendirir
        
        await message.reply_text(
            f"📊 **Bot İstatistikleri**\n\n{stats_text}",
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        LOGGER.error(f"Error fetching stats for admin {message.from_user.id}: {e}")
        await message.reply_text("❌ İstatistikler alınırken bir hata oluştu.")


# Not: DB sınıfına 'add_or_update_user' metodu eklenmelidir.
# DBManager'da User koleksiyonu genellikle 'tracking' DB'sinde yer alır.
# Örnek DB.add_or_update_user implementasyonu:
"""
async def add_or_update_user(self, user_data: User):
    tracking_db = self.dbs["tracking"]
    await tracking_db["users"].update_one(
        {"user_id": user_data.user_id},
        {"$set": user_data.dict()},
        upsert=True
    )
"""
