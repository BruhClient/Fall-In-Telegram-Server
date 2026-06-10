import asyncio
import logging
import msvcrt
import os
import sys

import requests
from dotenv import load_dotenv
from supabase import acreate_client, AsyncClient
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)

BOT_TOKEN       = os.environ["TELEGRAM_BOT_TOKEN"]
FALL_IN_URL     = os.environ["FALL_IN_URL"].rstrip("/")
REGISTER_SECRET = os.environ["TELEGRAM_REGISTER_SECRET"]
SUPABASE_URL    = os.environ["SUPABASE_URL"]
SUPABASE_KEY    = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")

NOTIFICATION_LABELS = {
    "announcement": "📢 Announcement",
    "event_reminder_tomorrow": "⏰ Event Tomorrow",
    "event_reminder_weekly": "🗓️ This Week",
    "event_created": "🆕 New Event",
    "added_to_event": "➕ Added to Event",
    "added_to_group": "👥 Added to Group",
    "event_updated": "✏️ Event Updated",
}
DEFAULT_NOTIFICATION_LABEL = "🔔 Notification"

db: AsyncClient
_app: Application


# ── Single-instance lock ───────────────────────────────────────────────────────

def acquire_single_instance_lock():
    """Ensure only one copy of this bot runs at a time.

    Two instances would both subscribe to notification INSERTs and both poll
    Telegram, causing every notification to be delivered twice.
    """
    lock_file = open(LOCK_PATH, "w")
    try:
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        logging.error("Another instance of this bot is already running — exiting.")
        sys.exit(1)
    return lock_file


# ── /start command ─────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    token   = context.args[0] if context.args else None
    chat_id = update.effective_chat.id

    if not token:
        await update.message.reply_text(
            "Open the Fall In app, go to your profile, and tap 'Tap to activate'."
        )
        return

    resp = await asyncio.to_thread(
        requests.post,
        f"{FALL_IN_URL}/api/telegram/register",
        json={"token": token, "chat_id": chat_id},
        headers={"Authorization": f"Bearer {REGISTER_SECRET}"},
        timeout=10,
    )
    if resp.ok:
        await update.message.reply_text(
            "Your account is linked. You'll receive Fall In notifications here."
        )
    else:
        await update.message.reply_text(
            "Link expired or not found. Please try again from your profile page."
        )


# ── Notification forwarding ────────────────────────────────────────────────────

def handle_notification(payload: dict) -> None:
    record       = (payload.get("data") or {}).get("record") or {}
    recipient_id = record.get("recipient_id")
    body         = record.get("body")
    notif_type   = record.get("type")
    if not recipient_id or not body:
        logging.warning("Ignoring notification with missing recipient_id/body: %r", record)
        return
    logging.info("Received %s notification for recipient %s", notif_type, recipient_id)
    asyncio.create_task(forward_notification(recipient_id, notif_type, body))


async def forward_notification(recipient_id: str, notif_type: str, body: str) -> None:
    try:
        result = (
            await db.table("profiles")
            .select("telegram_chat_id")
            .eq("id", recipient_id)
            .maybe_single()
            .execute()
        )
        chat_id = (result.data or {}).get("telegram_chat_id") if result else None
        if not chat_id:
            logging.info("Recipient %s has no linked Telegram chat — skipping", recipient_id)
            return

        label = NOTIFICATION_LABELS.get(notif_type, DEFAULT_NOTIFICATION_LABEL)
        await _app.bot.send_message(chat_id=chat_id, text=f"{label}\n{body}")
        logging.info("Forwarded %s notification to chat %s", notif_type, chat_id)
    except Exception:
        logging.exception("Failed to forward %s notification to recipient %s", notif_type, recipient_id)


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    global db, _app

    _lock_file = acquire_single_instance_lock()

    db = await acreate_client(SUPABASE_URL, SUPABASE_KEY)

    _app = Application.builder().token(BOT_TOKEN).build()
    _app.add_handler(CommandHandler("start", cmd_start))

    def on_subscribe(status, error):
        if error:
            logging.error("Realtime subscription error: %s (%s)", status, error)
        else:
            logging.info("Realtime subscription status: %s", status)

    channel = db.realtime.channel("notification-inserts").on_postgres_changes(
        event="INSERT",
        schema="public",
        table="notification",
        callback=handle_notification,
    )
    await channel.subscribe(on_subscribe)

    async with _app:
        await _app.start()
        await _app.updater.start_polling(drop_pending_updates=True)
        logging.info("Bot running — polling for /start commands and forwarding notifications.")
        await asyncio.Event().wait()
        await _app.updater.stop()
        await _app.stop()


if __name__ == "__main__":
    asyncio.run(main())
