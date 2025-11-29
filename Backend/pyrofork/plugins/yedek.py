import psutil
import time
from pyrofork import Plugin

plugin = Plugin(name="system_info")

@plugin.command("status")
async def status(ctx):
    """
    Bot sistem durumunu gösterir: CPU, RAM ve uptime (hh:mm:ss).
    """
    # CPU ve RAM bilgisi
    cpu_percent = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory()

    # Uptime hesaplama
    uptime_seconds = time.time() - psutil.boot_time()
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Mesaj formatı
    msg = (
        f"CPU: {cpu_percent}% | FREE: {ram.available / (1024**3):.2f}GB\n"
        f"RAM: {ram.percent}% | UPTIME: {int(hours)}h{int(minutes)}m{int(seconds)}s"
    )

    # Telegram cevabı
    await ctx.reply(msg)
