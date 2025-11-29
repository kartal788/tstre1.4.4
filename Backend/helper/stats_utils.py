import locale
from typing import Dict
# LÜTFEN BU SATIRIN KENDİ PROJENİZDEKİ DOĞRU DB BAĞLANTI SINIFINA İŞARET ETTİĞİNDEN EMİN OLUN.
from Backend.db.database import Database  

# Türkçe formatlama için locale ayarı (Sisteminiz desteklemiyorsa bu satırı silebilirsiniz)
# try:
#     locale.setlocale(locale.LC_ALL, 'tr_TR.UTF-8')
# except locale.Error:
#     pass

async def get_db_stats() -> Dict[str, str]:
    """
    Tüm aktif depolama veritabanlarından (Storage DB) film ve dizi sayılarını toplar,
    toplam kullanılan depolama alanını hesaplar ve formatlanmış bir sözlük döndürür.
    """
    stats = {
        'total_movies': 0,
        'total_tv_shows': 0,
        'total_storage_mb': 0.0
    }
    
    # Veritabanı bağlantılarını almaya çalışın (Çoklu DB/Bot desteği için)
    db_instances = []
    try:
        # get_all_instances metodu tüm aktif storage DB örneklerini döndürür
        db_instances = Database.get_all_instances()
    except AttributeError:
        # Eğer bu metod yoksa, sadece ana DB'yi deneyin.
        print("Database.get_all_instances() bulunamadı. Sadece varsayılan DB denenecek.")
        try:
            db_instances = [Database.get_instance()]
        except Exception:
            pass # Bağlantı hatası durumunda boş liste döner
    except Exception as e:
        print(f"Veritabanı örnekleri alınırken beklenmedik hata: {e}")

    # İstatistikleri toplama döngüsü
    for db_instance in db_instances:
        try:
            # 1. Koleksiyon Sayılarını Çekme
            movie_count = await db_instance.Movie.count_documents({})
            tv_count = await db_instance.TVShow.count_documents({})
            
            stats['total_movies'] += movie_count
            stats['total_tv_shows'] += tv_count
            
            # 2. Depolama Boyutunu Çekme (dbstats komutu ile)
            db_stats = await db_instance.client.admin.command('dbstats', db=db_instance.name)
            
            storage_size_bytes = db_stats.get('storageSize', 0)
            
            # Byte'tan Megabyte'a çevirme
            stats['total_storage_mb'] += storage_size_bytes / (1024 * 1024)
            
        except Exception as e:
            print(f"'{db_instance.name}' veritabanı istatistikleri çekilirken hata: {e}")
            continue

    # 3. Verileri Telegram için formatlama

    # Depolama formatı: 123.45 MB
    stats['formatted_storage'] = f"{stats['total_storage_mb']:.2f} MB"
    
    # Sayı formatı: Binlik ayırıcı (Örn: 10.250)
    # {sayı:,} formatı binlik ayırıcı ekler, sonra Türkçe için virgülü noktaya çeviririz.
    formatted_movies = f"{stats['total_movies']:,}".replace(",", ".")
    formatted_tv = f"{stats['total_tv_shows']:,}".replace(",", ".")
    
    return {
        'formatted_movies': formatted_movies,
        'formatted_tv': formatted_tv,
        'formatted_storage': stats['formatted_storage']
    }
