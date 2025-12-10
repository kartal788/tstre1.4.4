from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters # İstenen içe aktarma eklendi
from pymongo import MongoClient
import os
import importlib.util
import sys

# ----------------- YAPILANDIRMA VE SABİTLER -----------------
# Bu yolu kendi yapılandırma dosyanızın konumuna göre güncelleyin.
CONFIG_PATH = "/home/debian/dfbot/config.py" 
# NOT: CustomFilters, Backend/helper/custom_filter.py dosyanızda tanımlı olmalıdır.

# ----------------- DATABASE YÖNETİMİ -----------------

def read_database_from_config():
    """config.py dosyasından DATABASE URL'sini okur."""
    if not os.path.exists(CONFIG_PATH):
        print(f"Hata: Config dosyası bulunamadı: {CONFIG_PATH}", file=sys.stderr)
        return None
    try:
        spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return getattr(config, "DATABASE", None)
    except Exception as e:
        print(f"Hata: Config dosyası okunurken sorun oluştu: {e}", file=sys.stderr)
        return None

def get_db_urls():
    """Ortam değişkenlerinden veya config dosyasından DB URL'lerini listeler."""
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
    # Birden fazla DB URL'si virgülle ayrılmışsa, hepsini döndür.
    return [u.strip() for u in db_raw.split(",") if u.strip()]

def get_db_client_and_collections(url: str):
    """MongoDB istemcisini ve 'movie', 'tv' koleksiyonlarını döndürür."""
    try:
        client = MongoClient(url)
        # Genellikle ilk veritabanı adını kullanırız
        db_name_list = client.list_database_names()
        if not db_name_list:
            client.close() 
            return None, None, None
            
        db = client[db_name_list[0]]
        return client, db["movie"], db["tv"]
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}", file=sys.stderr)
        return None, None, None

# ----------------- SİLME İŞLEVİ -----------------

def delete_file_from_db(db_url: str, filename: str):
    """
    Verilen dosya adını veritabanındaki 'movie' ve 'tv' koleksiyonlarında arar.
    Eşleşen ilk belgeyi siler.
    """
    client, movie_col, tv_col = get_db_client_and_collections(db_url)
    
    if not client:
        return 0, "Veritabanı bağlantısı kurulamadı (URL: {}).".format(db_url)

    delete_filter = {"file_name": filename}
    
    try:
        # 1. movie koleksiyonunda arama ve silme
        movie_result = movie_col.delete_one(delete_filter)
        if movie_result.deleted_count > 0:
            client.close()
            return 1, f"'{filename}' **movie** koleksiyonundan başarıyla silindi."

        # 2. tv koleksiyonunda arama ve silme
        tv_result = tv_col.delete_one(delete_filter)
        if tv_result.deleted_count > 0:
            client.close()
            return 1, f"'{filename}' **tv** koleksiyonundan başarıyla silindi."
        
        client.close()
        return 0, f"'{filename}' veritabanında bulunamadı."
        
    except Exception as e:
        client.close()
        return 0, f"Veritabanı silme işlemi sırasında hata oluştu: {e}"


# ----------------- TELEGRAM KOMUTU -----------------

@Client.on_message(filters.command("silinecek") & filters.private & CustomFilters.owner)
async def delete_file_command(client: Client, message: Message):
    """
    /silinecek komutunu işler.
    Örn: /silinecek Ocean's Eight (2018) 1080p MAX WEB-DL [EN-TR] DDP5.1 Atmos H.264-TURG.mkv
    """
    try:
        # Komuttan sonra parametre kontrolü
        if len(message.command) < 2:
            await message.reply_text(
                "⚠️ Lütfen silinecek dosya adını komuttan sonra belirtin. Örn: `/silinecek DosyaAdı.mkv`", 
                quote=True, 
                parse_mode=enums.ParseMode.MARKDOWN
            )
            return

        db_urls = get_db_urls()
        
        # Kodun beklentisi ikinci DB URL'sinin kullanılmasıdır (db_urls[1])
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı adresi bulunamadı. Lütfen yapılandırmayı kontrol edin.", quote=True)
            return

        # Komuttan sonraki tüm metni dosya adı olarak al
        filename_to_delete = " ".join(message.command[1:])
        
        await message.reply_text(f"⏳ **'{filename_to_delete}'** veritabanında aranıyor...", quote=True, parse_mode=enums.ParseMode.MARKDOWN)
        
        # İkinci MongoDB URL'sini kullanarak silme işlemini gerçekleştir
        deleted_count, status_message = delete_file_from_db(db_urls[1], filename_to_delete)

        if deleted_count > 0:
            text = f"✅ **Başarılı:**\n{status_message}"
        else:
            text = f"❌ **Başarısız:**\n{status_message}"

        await message.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Dosya silme komutu işlenirken kritik hata oluştu: `{e}`", quote=True, parse_mode=enums.ParseMode.MARKDOWN)
        print("delete_file_command hata:", e, file=sys.stderr)
