from pyrogram import filters, Client, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from Backend.config import Telegram
# Veritabanı istatistiklerini çekecek varsayımsal fonksiyonunuzu içe aktarın
from Backend.db.stats import get_db_stats 

@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    """
    Kullanıcıya Stremio eklenti adresini ve sistem istatistiklerini (Film/Dizi sayısı, Depolama) gönderir.
    """
    try:
        # 1. Dashboard verilerini MongoDB'den çekin
        # Bu fonksiyonun, total_movies, total_tv_shows ve formatted_storage gibi 
        # formatlanmış verileri döndürdüğü varsayılmıştır.
        stats = await get_db_stats() 
        
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # 2. Mesaj metnini istatistiklerle birlikte oluşturun
        message_text = (
            '🎉 **Telegram Stremio Medya Sunucunuza Hoş Geldiniz!**\n\n'
            
            'Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.\n\n'
            f'🔗 **Eklenti Adresin:**\n<code>{addon_url}</code>\n\n'
            
            '--- **Sistem İstatistikleri** ---\n'
            f'🎬 **Toplam Film:** <code>{stats["formatted_movies"]}</code>\n'
            f'📺 **Toplam Dizi:** <code>{stats["formatted_tv"]}</code>\n'
            f'💾 **Kullanılan Depolama:** <code>{stats["formatted_storage"]}</code>\n'
            '--------------------------------\n\n'
            
            '💡 *Medya dosyalarını kanalınıza yükledikten sonra katalog otomatik olarak güncellenecektir.*'
        )

        await message.reply_text(
            message_text,
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        # Hata durumunda kullanıcıya ve konsola bilgi verin
        await message.reply_text(f"⚠️ Hata oluştu: İstatistikler alınamadı veya sunucu yapılandırılamadı.\n\nHata Detayı: `{e}`")
        print(f"Error in /start handler: {e}")
