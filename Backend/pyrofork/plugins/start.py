from pyrogram import filters, Client, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram

@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        await message.reply_text(
            '<b>Telegram stremio botuna hoş geldin.</b>\n\n'
            'Stremio eklentisini yüklemek için aşağıdaki URLyi kopyalayın ve Stremio eklentilerine ekleyin:\n\n'
            f'<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n'
    '<b>Eklenti adresini başkalarına paylaşma!</b>\n'
            'Paylaşırsan video izlerken donmalar yaşayabilirsin. \n'
            'Sonra ‘Kartal abi video donuyor’ dersin. 😃',
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Error: {e}")
        print(f"Error in /start handler: {e}")
