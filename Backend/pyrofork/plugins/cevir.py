from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import os
import importlib.util
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

# ------------ AYARLAR ------------
CONFIG_PATH = "/home/debian/tstre1.4.4/config.env"
BATCH_SIZE = 120            # Batch boyutu (20 -> 120 ile verim artar)
MAX_WORKERS = 20            # Paralel çeviri işçi sayısı (network-bound)
PROGRESS_UPDATE_INTERVAL = 30  # saniye

# ------------ CONFIG okuma ------------
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

# ------------ DATABASE Bağlantısı ------------
db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client = MongoClient(MONGO_URL)
db_name = client.list_database_names()[0]
db = client[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ------------ Çeviri motoru & executor ------------
translator = GoogleTranslator(source='en', target='tr')
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# ------------ Yardımcı fonksiyonlar ------------
def format_time(seconds):
    """Saniyeyi HH:MM:SS formatına çevirir."""
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

def looks_like_turkish(text: str) -> bool:
    """Basit heuristic: Türkçeye özgü karakter içeriyorsa çevirmeye gerek yok."""
    if not text:
        return True
    turkish_chars = set("ıİşşĞğÜüÖöÇç")
    return any((c in turkish_chars) for c in text)

# ---------- Thread içinde çalışacak çeviri çağrısı ----------
def _do_translate(text: str) -> str:
    try:
        # translator.translate hata verirse orijinali döndür
        return translator.translate(str(text))
    except Exception:
        return str(text)

# ---------- Async wrapper: non-blocking olarak çeviri yapar ----------
async def translate_text_async(text: str) -> str:
    if text is None:
        return ""
    t = str(text).strip()
    if t == "":
        return ""
    # Eğer Türkçe karakter içeriyorsa tekrar çeviri yapma (heuristic)
    if looks_like_turkish(t):
        return t
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, _do_translate, t)

# ---------- Çoklu çeviri için yardımcı (aynı anda birçok çeviri) ----------
async def translate_many_async(texts):
    """texts: liste/iterable. Döndürür: listede karşılık gelen çeviriler."""
    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(executor, _do_translate, str(t)) if (t and not looks_like_turkish(str(t))) else asyncio.sleep(0, result=(t or "")) for t in texts]
    # yukarıdaki trick: türkçe ya da boşsa asyncio.sleep ile hazır sonuç veriyoruz
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return results

# ------------ Koleksiyon İşleyici (Bulk + ETA + Paralel Çeviri) ------------
async def process_collection_interactive(collection, name, message):
    cursor = collection.find({})
    data = list(cursor)  # veritabanı küçük değilse cursor ile page'leme yapılabilir
    total = len(data)
    if total == 0:
        # hemen bir güncelleme yap
        try:
            await message.edit_text(f"{name}: 0/0\n{progress_bar(0,0)}\nKalan: 0, Hatalar: 0\n⏳ Kalan tahmini süre (ETA): 00:00:00")
        except Exception:
            pass
        return 0, 0, 0, 0.0

    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    updates = []  # bulk_update için UpdateOne listesi

    # İşlem: batch'ler halinde
    while done < total:
        batch = data[done:done + BATCH_SIZE]
        # Hazırlık: bu batch içindeki çevrilecek tüm metinleri toplayalım
        # Her row için description + (seasons -> episodes -> title/overview)
        # Yapı: for batch, toplu translate_many_async çağıracağız
        desc_texts = []
        desc_map = []  # (row_index_in_batch) mapping
        ep_title_texts = []
        ep_overview_texts = []
        ep_refs = []  # list of tuples (batch_idx, season_idx, episode_idx, field_name)
        # also keep row-level placeholders
        for i, row in enumerate(batch):
            # description
            d = row.get("description")
            if d is not None:
                desc_texts.append(d)
                desc_map.append(i)
            # seasons -> episodes
            seasons = row.get("seasons")
            if seasons and isinstance(seasons, list):
                for s_idx, season in enumerate(seasons):
                    episodes = season.get("episodes")
                    if episodes and isinstance(episodes, list):
                        for e_idx, ep in enumerate(episodes):
                            # title
                            t = ep.get("title")
                            if t is not None:
                                ep_title_texts.append(t)
                                ep_refs.append((i, s_idx, e_idx, "title"))
                            # overview
                            o = ep.get("overview")
                            if o is not None:
                                ep_overview_texts.append(o)
                                ep_refs.append((i, s_idx, e_idx, "overview"))

        # Paralel çeviriler: description'ları, başlıkları ve özetleri ayrı ayrı çevir
        # (büyük listesinin tamamını tek seferde translate_many_async ile gönderiyoruz)
        # Desc
        desc_results = await translate_many_async(desc_texts) if desc_texts else []
        # Titles
        title_results = await translate_many_async(ep_title_texts) if ep_title_texts else []
        # Overviews
        overview_results = await translate_many_async(ep_overview_texts) if ep_overview_texts else []

        # Sonuçları batch içinde yerlerine yerleştir
        # descriptions
        for idx, translated in enumerate(desc_results):
            batch_idx = desc_map[idx]
            if "description" not in batch[batch_idx]:
                batch[batch_idx]["description"] = translated
            else:
                batch[batch_idx]["description"] = translated

        # For episodes: we used ep_refs but note we appended both title & overview refs to same list.
        # To simplify mapping, we'll iterate episodes again and pop from title_results/overview_results.
        t_iter = iter(title_results)
        o_iter = iter(overview_results)
        for i, row in enumerate(batch):
            seasons = row.get("seasons")
            if seasons and isinstance(seasons, list):
                for s_idx, season in enumerate(seasons):
                    episodes = season.get("episodes")
                    if episodes and isinstance(episodes, list):
                        for e_idx, ep in enumerate(episodes):
                            if ep.get("title") is not None:
                                try:
                                    ep["title"] = next(t_iter)
                                except StopIteration:
                                    pass
                            if ep.get("overview") is not None:
                                try:
                                    ep["overview"] = next(o_iter)
                                except StopIteration:
                                    pass

        # Hazırlanan update_dict'leri topla (sadece değişen row'lar için)
        for row in batch:
            update_dict = {}
            # description
            if "description" in row:
                update_dict["description"] = row["description"]
            # seasons (tamamını set ediyoruz çünkü içindeki episode title/overview güncellendi)
            if "seasons" in row:
                update_dict["seasons"] = row["seasons"]

            if update_dict:
                try:
                    updates.append(UpdateOne({"_id": row["_id"]}, {"$set": update_dict}))
                except Exception as e:
                    errors += 1
                    print(f"Update hazırlama hatası: {e}")

        # Bulk yazma (her batch sonunda)
        if updates:
            loop = asyncio.get_running_loop()
            try:
                # pymongo bulk_write blocking olduğu için executor içinde çalıştırıyoruz
                def _bulk():
                    try:
                        collection.bulk_write(updates, ordered=False)
                        return None
                    except Exception as ex:
                        return ex
                bulk_result = await loop.run_in_executor(executor, _bulk)
                if bulk_result is not None:
                    # bulk_result bir Exception döndürdüyse
                    errors += len(updates)
                    print(f"Bulk write hata: {bulk_result}")
            except Exception as e:
                errors += len(updates)
                print(f"Bulk yazma hatası (outer): {e}")
            updates = []

        # Batch tamamlandı sayısını arttır
        prev_done = done
        done += len(batch)

        # ETA hesaplama
        current_time = time.time()
        elapsed = current_time - start_time
        rate = done / elapsed if elapsed > 0 else 0
        remaining_items = total - done
        eta = remaining_items / rate if rate > 0 else 0

        # Mesaj güncellemesi
        if current_time - last_update > PROGRESS_UPDATE_INTERVAL or done == total:
            bar = progress_bar(done, total)
            text = (
                f"{name}: {done}/{total}\n"
                f"{bar}\n"
                f"Kalan: {remaining_items}, Hatalar: {errors}\n"
                f"⏳ Kalan tahmini süre (ETA): {format_time(eta)}"
            )
            try:
                await message.edit_text(text)
            except Exception:
                pass
            last_update = current_time

    total_elapsed = time.time() - start_time
    return total, done, errors, total_elapsed

# ------------ /cevir Komutu ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    start_msg = await message.reply_text(
        "🇹🇷 Film ve dizi açıklamaları Türkçeye çevriliyor…\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN
    )

    # Filmler
    movie_total, movie_done, movie_errors, movie_time = await process_collection_interactive(
        movie_col, "Filmler", start_msg
    )

    # Diziler
    series_total, series_done, series_errors, series_time = await process_collection_interactive(
        series_col, "Diziler", start_msg
    )

    # -------- Özet --------
    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = movie_time + series_time

    summary = (
        "🎉 *Film & Dizi Türkçeleştirme Sonuçları*\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n"
        f"{progress_bar(movie_done, movie_total)}\n"
        f"Kalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
        f"📌 Diziler: {series_done}/{series_total}\n"
        f"{progress_bar(series_done, series_total)}\n"
        f"Kalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
        f"📊 Genel Özet\n"
        f"Toplam içerik : {total_all}\n"
        f"Başarılı     : {done_all - errors_all}\n"
        f"Hatalı       : {errors_all}\n"
        f"Kalan        : {remaining_all}\n"
        f"⏱ Toplam süre : {format_time(total_time)}\n"
    )

    await start_msg.edit_text(summary, parse_mode=enums.ParseMode.MARKDOWN)
