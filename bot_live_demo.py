# bot_live_demo.py - نظام بث ذكي ومستمر بدون توقف
import time
import subprocess
import asyncio
import json
import os
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import threading
from aiohttp import web

CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "BOT_TOKEN": os.environ.get("BOT_TOKEN", ""),
    "YOUR_USER_ID": os.environ.get("YOUR_USER_ID", ""),
    "CHANNEL_ID": os.environ.get("CHANNEL_ID", ""),
    "SOURCE_URL": os.environ.get("SOURCE_URL", ""),
    "CLIP_SECONDS": 17,
    "SLEEP_BETWEEN": 0,
    "BOTTOM_WATERMARK_TEXT": "t.me/xl9rr",
    "BOTTOM_WATERMARK_ENABLED": True,
    "BUFFER_SIZE": 5,
    "KEYFRAME_INTERVAL": 2
}

class ConfigManager:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        self.lock = threading.Lock()

    def load_config(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    loaded = json.load(f)
                    return {**DEFAULT_CONFIG, **loaded}
            except:
                pass
        return DEFAULT_CONFIG.copy()

    def get(self, key, default=None):
        return self.config.get(key, default)

    def set(self, key, value):
        with self.lock:
            self.config[key] = value

config = ConfigManager(CONFIG_FILE)

required_vars = ["BOT_TOKEN", "YOUR_USER_ID", "CHANNEL_ID", "SOURCE_URL"]
missing_vars = [var for var in required_vars if not config.get(var)]

if missing_vars:
    print("Variables requeridas ❌")
    for var in missing_vars:
        print(f"   {var}")
    exit(1)

bot = Bot(token=config.get("BOT_TOKEN"))
clip_queue = Queue(maxsize=config.get("BUFFER_SIZE", 5))
stats = {"clips_sent": 0, "clips_failed": 0, "uptime_start": time.time()}
broadcast_running = False
active_users = []
stream_position = 0.0
stream_lock = threading.Lock()
producer_running = False
consumer_running = False

channel_id = str(config.get("CHANNEL_ID")).strip()
if not channel_id.startswith("-100") and not channel_id.startswith("@"):
    if channel_id.startswith("-"):
        pass
    else:
        channel_id = f"-100{channel_id}"
    config.set("CHANNEL_ID", channel_id)

owner_id = str(config.get("YOUR_USER_ID"))
if owner_id not in active_users:
    active_users.append(owner_id)

print(f"👥 المشتركين: {len(active_users)}")
print(f"📺 القناة: {channel_id}")

# Web Server
async def handle_health(request):
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Bot Status</title>
    </head>
    <body style="margin:0;padding:0;display:flex;justify-content:center;align-items:center;min-height:100vh;font-family:Arial,sans-serif;">
        <div style="text-align:center;">
            <h1 style="font-size:36px;margin:0;">🤖 is bot live</h1>
            <p style="font-size:18px;margin:10px 0 0 0;">by: @xl9rr</p>
        </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 5000)
    await site.start()
    print("🌐 http://0.0.0.0:5000")

# أوامر البوت
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    user_id = str(update.effective_user.id)
    if user_id not in active_users:
        active_users.append(user_id)

    status = "🟢 يعمل" if broadcast_running else "🔴 متوقف"
    await update.message.reply_text(
        f"✅ أهلاً بك\n\n"
        f"البث: {status}\n"
        f"المشتركين: {len(active_users)}\n\n"
        f"/help - عرض الأوامر"
    )

async def startlive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    global broadcast_running, stream_position
    user_id = str(update.effective_user.id)

    if user_id != config.get("YOUR_USER_ID"):
        await update.message.reply_text("❌ للمالك فقط")
        return

    if broadcast_running:
        await update.message.reply_text("⚠️ البث يعمل")
        return

    broadcast_running = True
    stream_position = 0.0

    while not clip_queue.empty():
        try:
            clip_queue.get_nowait()
        except:
            break

    await update.message.reply_text("🎬 جاري بدء البث الذكي...")
    asyncio.create_task(broadcast_loop())
    await asyncio.sleep(2)
    await update.message.reply_text(
        f"✅ البث نشط (ذكي وسلس 100%)\n"
        f"المشتركين: {len(active_users)}\n"
        f"المدة: {config.get('CLIP_SECONDS')}ث\n"
        f"Buffer: {config.get('BUFFER_SIZE')} مقاطع"
    )

async def stoplive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    global broadcast_running
    user_id = str(update.effective_user.id)

    if user_id != config.get("YOUR_USER_ID"):
        await update.message.reply_text("❌ للمالك فقط")
        return

    if not broadcast_running:
        await update.message.reply_text("⚠️ البث متوقف")
        return

    broadcast_running = False
    await update.message.reply_text("🛑 جاري الإيقاف...")
    await asyncio.sleep(2)
    await update.message.reply_text("✅ تم إيقاف البث")

async def setbottom_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    user_id = str(update.effective_user.id)
    if user_id != config.get("YOUR_USER_ID"):
        await update.message.reply_text("❌ للمالك فقط")
        return

    if not context.args:
        await update.message.reply_text(
            f"العلامة السفلية: {config.get('BOTTOM_WATERMARK_TEXT')}\n\n"
            "مثال: /setbottom Telegram | @media_ayham"
        )
        return

    new_text = " ".join(context.args)
    config.set("BOTTOM_WATERMARK_TEXT", new_text)
    await update.message.reply_text(f"✅ تم تغيير العلامة السفلية إلى:\n{new_text}")

async def wbottom_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    user_id = str(update.effective_user.id)
    if user_id != config.get("YOUR_USER_ID"):
        await update.message.reply_text("❌ للمالك فقط")
        return

    current = config.get('BOTTOM_WATERMARK_ENABLED', True)
    new_status = not current
    config.set('BOTTOM_WATERMARK_ENABLED', new_status)

    status_text = "🟢 مفعلة" if new_status else "🔴 معطلة"
    await update.message.reply_text(f"✅ العلامة السفلية: {status_text}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    user_id = str(update.effective_user.id)
    if user_id != config.get("YOUR_USER_ID"):
        return

    uptime = time.time() - stats["uptime_start"]
    hours = int(uptime // 3600)
    minutes = int((uptime % 3600) // 60)
    status = "🟢 يعمل" if broadcast_running else "🔴 متوقف"

    bottom_status = "🟢" if config.get('BOTTOM_WATERMARK_ENABLED') else "🔴"

    queue_size = clip_queue.qsize()

    await update.message.reply_text(
        f"📊 الإحصائيات\n\n"
        f"البث: {status}\n"
        f"الموضع: {stream_position:.1f}ث\n"
        f"Buffer: {queue_size}/{config.get('BUFFER_SIZE')}\n"
        f"المشتركين: {len(active_users)}\n"
        f"المقاطع: {stats['clips_sent']}\n"
        f"فشل: {stats['clips_failed']}\n"
        f"الوقت: {hours}س {minutes}د\n\n"
        f"العلامة المائية:\n"
        f"{bottom_status} السفلية: {config.get('BOTTOM_WATERMARK_TEXT')}"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    await update.message.reply_text(
        "📋 قائمة الأوامر\n\n"
        "للجميع:\n"
        "/start - بدء البوت\n"
        "/help - قائمة الأوامر\n\n"
        "للمالك فقط:\n"
        "/startLIVE - تشغيل البث 🟢\n"
        "/stopLIVE - إيقاف البث 🔴\n\n"
        "العلامة المائية:\n"
        "/setbottom - تغيير نص العلامة المتحركة 🔄\n"
        "/wbottom - تفعيل/تعطيل العلامة\n"
        "/stats - الإحصائيات\n\n"
        "✨ بث ذكي: سلس 100% بدون توقف أو تقطيع"
    )

async def any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    user_id = str(update.effective_user.id)
    if user_id not in active_users:
        active_users.append(user_id)
        await update.message.reply_text("✅ تم تسجيلك في البث")
    else:
        await update.message.reply_text("✅ أنت مسجل")

# معالجة الفيديو الذكية
def build_smart_ffmpeg_cmd(src, out, start_pos, duration, bottom_text="", bottom_enabled=True):
    cmd = [
        "ffmpeg", "-y",
        "-hide_banner", "-loglevel", "error",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-timeout", "10000000",
        "-i", src,
        "-t", str(duration),
        "-async", "1",
        "-vsync", "passthrough",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-crf", "23",
        "-g", "30",
        "-keyint_min", "30",
        "-sc_threshold", "0",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        "-shortest",
        "-avoid_negative_ts", "make_zero",
        "-fflags", "+genpts+igndts+discardcorrupt",
        "-max_delay", "0",
        "-movflags", "+faststart"
    ]

    if bottom_enabled and bottom_text:
        escaped_bottom = bottom_text.replace(":", "\\:").replace("'", "\\'")
        bottom_filter = (
            f"drawtext=text='{escaped_bottom}':x=w-mod(100*t\\,w+tw):y=h-th-80:"
            f"fontsize=32:fontcolor=white@0.95:"
            f"fontfile=/tmp/fonts/Tajawal-Regular.ttf:"
            f"borderw=0.8:bordercolor=black@0.6"
        )
        cmd += ["-vf", bottom_filter]

    cmd.append(out)
    return cmd

def smart_clip_recorder(output_path, start_position):
    if os.path.exists(output_path):
        try:
            os.remove(output_path)
        except:
            pass

    cmd = build_smart_ffmpeg_cmd(
        config.get("SOURCE_URL"),
        output_path,
        0,
        config.get("CLIP_SECONDS"),
        config.get("BOTTOM_WATERMARK_TEXT", ""),
        config.get("BOTTOM_WATERMARK_ENABLED", True)
    )

    for attempt in range(3):
        process = None
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE
            )

            process.wait(timeout=90)

            if process.returncode == 0 and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                if file_size > 5120:
                    return True

            if attempt < 2:
                time.sleep(2)

        except subprocess.TimeoutExpired:
            if process:
                process.kill()
            if attempt < 2:
                time.sleep(2)
        except Exception:
            if attempt < 2:
                time.sleep(2)

    return False

async def smart_send_clip(clip_path):
    if not os.path.exists(clip_path):
        return False

    success_count = 0

    try:
        with open(clip_path, "rb") as f:
            await bot.send_video(
                chat_id=config.get("CHANNEL_ID"),
                video=f,
                supports_streaming=True,
                read_timeout=300,
                write_timeout=300
            )
        success_count += 1
        print("✅ القناة")
    except Exception as e:
        print(f"❌ القناة: {str(e)[:50]}")

    for user_id in active_users:
        try:
            with open(clip_path, "rb") as f:
                await bot.send_video(
                    chat_id=user_id,
                    video=f,
                    supports_streaming=True,
                    read_timeout=300,
                    write_timeout=300
                )
            success_count += 1
        except:
            pass
        await asyncio.sleep(0.1)

    try:
        if os.path.exists(clip_path):
            os.remove(clip_path)
    except:
        pass

    stats["clips_sent"] += 1
    print(f"📊 {success_count}/{len(active_users) + 1}")
    return success_count > 0

async def send_start_message():
    try:
        await bot.send_message(
            chat_id=config.get("CHANNEL_ID"),
            text="🎬 البث الذكي بدأ\n✨ سلس 100% بدون توقف"
        )
    except:
        pass

    for user_id in active_users:
        try:
            await bot.send_message(
                chat_id=user_id,
                text="🎬 البث الذكي بدأ\n✨ سلس 100% بدون توقف"
            )
        except:
            pass
        await asyncio.sleep(0.1)

def smart_producer():
    global stream_position, producer_running
    producer_running = True
    clip_counter = 0
    clip_duration = float(config.get("CLIP_SECONDS"))
    failures = 0

    print("🎬 المنتج الذكي: بدء العمل")

    while broadcast_running:
        try:
            clip_counter += 1

            with stream_lock:
                current_position = stream_position

            output_path = f"/tmp/smart_clip_{clip_counter}.mp4"

            print(f"⏺️  تسجيل #{clip_counter} من [{current_position:.1f}ث]")

            start_time = time.time()
            success = smart_clip_recorder(output_path, current_position)
            elapsed = time.time() - start_time

            if success and os.path.exists(output_path) and broadcast_running:
                with stream_lock:
                    stream_position += clip_duration

                clip_queue.put((output_path, current_position, clip_counter), timeout=5)
                print(f"✅ #{clip_counter} ({elapsed:.1f}ث) → التالي: {stream_position:.1f}ث | Q:{clip_queue.qsize()}")
                failures = 0
            else:
                stats["clips_failed"] += 1
                failures += 1
                print(f"❌ فشل #{clip_counter}")

                if failures >= 3:
                    print("⚠️ فشل متكرر، انتظار 15ث")
                    time.sleep(15)
                    failures = 0
                else:
                    time.sleep(3)

        except Exception as e:
            print(f"🚨 خطأ producer: {str(e)[:50]}")
            failures += 1
            time.sleep(3 if failures < 3 else 15)

    producer_running = False
    print("🛑 المنتج: توقف")

async def smart_consumer():
    global consumer_running
    consumer_running = True
    print("📤 المستهلك الذكي: بدء الإرسال")

    while broadcast_running:
        try:
            try:
                clip_path, position, counter = clip_queue.get(timeout=1)
            except Empty:
                await asyncio.sleep(0.3)
                continue

            print(f"📤 إرسال #{counter} (من {position:.1f}ث)")

            try:
                await smart_send_clip(clip_path)
            except Exception as e:
                print(f"❌ خطأ إرسال #{counter}: {str(e)[:50]}")
                try:
                    if os.path.exists(clip_path):
                        os.remove(clip_path)
                except:
                    pass

            sleep_time = config.get("SLEEP_BETWEEN", 0)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        except Exception as e:
            print(f"🚨 خطأ consumer: {str(e)[:50]}")
            await asyncio.sleep(1)

    consumer_running = False
    print("🛑 المستهلك: توقف")

async def broadcast_loop():
    print("🎬 بدء البث الذكي...")
    await send_start_message()
    await asyncio.sleep(1)

    executor = ThreadPoolExecutor(max_workers=3)
    loop = asyncio.get_event_loop()

    loop.run_in_executor(executor, smart_producer)
    await smart_consumer()

async def main():
    asyncio.create_task(start_web_server())

    while True:
        try:
            application = Application.builder().token(config.get("BOT_TOKEN")).build()

            application.add_handler(CommandHandler("start", start_command))
            application.add_handler(CommandHandler("startLIVE", startlive_command))
            application.add_handler(CommandHandler("stopLIVE", stoplive_command))
            application.add_handler(CommandHandler("help", help_command))
            application.add_handler(CommandHandler("stats", stats_command))
            application.add_handler(CommandHandler("setbottom", setbottom_command))
            application.add_handler(CommandHandler("wbottom", wbottom_command))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, any_message))

            await application.initialize()
            await application.start()

            if application.updater:
                await application.updater.start_polling(
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES
                )

            print("✅ البوت يعمل")
            print("⏸️  استخدم /startLIVE للبدء")

            await asyncio.Event().wait()

        except Exception as e:
            print(f"🚨 خطأ: {str(e)[:100]}")
            print("🔄 إعادة المحاولة بعد 30ث")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
