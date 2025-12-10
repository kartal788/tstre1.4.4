import asyncio
import time
from pyrogram import Client, filters
# Hata yönetimi için FloodWait'i içe aktarıyoruz
from pyrogram.errors import FloodWait 
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util
import json
import datetime
import tempfile

# ------------ DATABASE Bağlantısı ------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

# ------------ GLOBAL FLAG İPTAL ------------
cancel_process = False

# ------------ /vtindir Komutu (Düzeltildi) ------------
@Client.on_message(filters.command("vtindir") & filters.private & CustomFilters.owner)
async def download_database(client, message: Message):
    global cancel_process
    cancel_process = False

    start_msg = await message.reply_text("💾 Database hazırlanıyor, lütfen bekleyin...")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"veritabanı_{timestamp}.json"
    
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp_file_path = tmp_file.name
    tmp_file.close()

    # 💡 THROTTLING AYARI: Minimum 5 saniyede bir mesajı güncelle
    MIN_UPDATE_INTERVAL = 5 

    try:
        collections = db.list_collection_names()
        # count_documents yerine estimated_document_count kullanabilirsiniz (daha hızlı, ama tahmini sonuç verir)
        total_docs = sum(db[col].count_documents({}) for col in collections) 
        processed_docs = 0
        start_time = time.time()
        last_update_time = time.time() # Son güncelleme zamanı

        with open(tmp_file_path, "w", encoding="utf-8") as f:
            f.write("{")
            for i, col_name in enumerate(collections):
                if cancel_process:
                    await start_msg.edit_text("❌ İşlem kullanıcı tarafından iptal edildi.")
                    return

                if i != 0:
                    f.write(",")

                f.write(f'"{col_name}": [')
                col_cursor = db[col_name].find({})
                first_doc = True
                for doc in col_cursor:
                    if cancel_process:
                        await start_msg.edit_text("❌ İşlem kullanıcı tarafından iptal edildi.")
                        return

                    if not first_doc:
                        f.write(",")
                    else:
                        first_doc = False

                    # MongoDB'deki ObjectId ve diğer özel tipleri JSON uyumlu hale getirir
                    f.write(json.dumps(doc, default=str, ensure_ascii=False)) 
                    processed_docs += 1

                    # 🔑 Düzeltme: Zaman tabanlı kısıtlama (Throttling) koşulu
                    current_time = time.time()
                    
                    # Sadece son belgede veya 50 belge ve minimum 5 saniye geçmişse güncelle
                    if processed_docs == total_docs or (processed_docs % 50 == 0 and current_time - last_update_time >= MIN_UPDATE_INTERVAL):
                        elapsed = current_time - start_time
                        remaining = (elapsed / processed_docs) * (total_docs - processed_docs) if processed_docs > 0 else 0
                        
                        try:
                            await start_msg.edit_text(
                                f"💾 Database hazırlanıyor...\n"
                                f"İlerleme: **{processed_docs} / {total_docs}** belgeler\n"
                                f"Tahmini kalan süre: {int(remaining)} saniye"
                            )
                            # Başarılı güncellemeden sonra zamanı sıfırla
                            last_update_time = current_time 

                        # 🚨 KRİTİK DÜZELTME: FloodWait hatasını yakala ve bekle
                        except FloodWait as e:
                            wait_time = e.value # Telegram'ın istediği bekleme süresi (saniye)
                            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] TELEGRAM FLOOD WAIT: {wait_time} saniye bekleniyor...")
                            await asyncio.sleep(wait_time)
                            # Bekledikten sonra bir sonraki döngüde devam edecek
                            last_update_time = time.time()
                        
                        except Exception as e_gen:
                            # Mesaj silinmiş/düzenlenemiyor olabilir, devam et
                            pass

                f.write("]")
            f.write("}")

        # Telegram'a gönder
        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_file_path,
            file_name=file_name,
            caption=f"📂 Veritabanı: **{db_name}** ({timestamp})"
        )

        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ Database indirilemedi.\nHata: `{e}`")

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)

# ------------ /iptal Komutu ------------
@Client.on_message(filters.command("iptal") & filters.private & CustomFilters.owner)
async def cancel_database_export(client, message: Message):
    global cancel_process
    cancel_process = True
    await message.reply_text("❌ Database indirme işlemi iptal ediliyor...")
