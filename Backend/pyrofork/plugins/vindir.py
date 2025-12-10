from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters  # Owner filtresi burada

# ---------------- /vtindir Komutu ----------------
@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def vtindir_handler(client: Client, message: Message):
    await message.reply_text("Merhaba")
