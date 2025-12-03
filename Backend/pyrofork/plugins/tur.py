import time
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from main import stop_event, movie_col, series_col  # stop_event ve koleksiyonlar ana dosyadan import edilmeli

@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_duzelt(client: Client, message):
    stop_event.clear()

    start_msg = await message.reply_text(
        "🎬 Türler düzenleniyor…\nİlerleme tek mesajda gösterilecektir.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    genre_map = {
        "Bilimkurgu": "Bilim kurgu",
        "batılı": "Vahşi Batı",
        "Fantezi": "Fantastik",
        "romantik": "Romantik"
    }

    collections = [
        (movie_col, "Filmler"),
        (series_col, "Diziler")
    ]

    total_fixed = 0
    last_update = 0

    for col, name in collections:
        ids_cursor = col.find({"genres": {"$in": list(genre_map.keys())}}, {"_id": 1, "genres": 1})
        ids = [d["_id"] for d in ids_cursor]
        idx = 0

        while idx < len(ids):
            if stop_event.is_set():
                break

            doc_id = ids[idx]
            doc = col.find_one({"_id": doc_id})
            genres = doc.get("genres", [])
            updated = False

            new_genres = []
            for g in genres:
                if g in genre_map:
                    new_genres.append(genre_map[g])
                    updated = True
                else:
                    new_genres.append(g)

            if updated:
                col.update_one({"_id": doc_id}, {"$set": {"genres": new_genres}})
                total_fixed += 1

            idx += 1

            # Her 5 saniyede bir ilerleme güncellemesi
            if time.time() - last_update > 5:
                try:
                    await start_msg.edit_text(
                        f"{name}: Güncellenen kayıtlar: {total_fixed}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                    )
                except:
                    pass
                last_update = time.time()

    # Tamamlandığında özet
    try:
        await start_msg.edit_text(
            f"✅ *Tür güncellemesi tamamlandı!*\n\n"
            f"Toplam değiştirilen kayıt: *{total_fixed}*\n\n"
            f"📌 Yapılan Dönüşümler:\n"
            f"• Bilimkurgu → Bilim kurgu\n"
            f"• batılı → Vahşi Batı\n"
            f"• Fantezi → Fantastik\n"
            f"• romantik → Romantik",
            parse_mode=enums.ParseMode.MARKDOWN
        )
    except:
        pass
