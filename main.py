# language: Python, file: main.py, runtime: Python 3.10+
# Host-ready: reads BOT_TOKEN / API_ID / API_HASH / OWNER_IDS / OWNER_USERNAME /
#             FORCE_CHANNEL / FORCE_GROUP from .env
#
# Hardened: Bot API send failures (network, TimedOut, Forbidden, RetryAfter)
# never terminate a background otp_worker task. Only intentional stops do:
# long FloodWait, banned/invalid number, or /stop (CancelledError).

import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TimedOut,
    TelegramError,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    PhoneNumberInvalidError,
    PhoneNumberBannedError,
)

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "jameendar")

# Comma-separated owner user IDs, e.g. 123456789,987654321
OWNER_IDS = {
    int(x.strip())
    for x in os.getenv("OWNER_IDS", "").split(",")
    if x.strip().isdigit()
}

# Force-join targets (@username or full https://t.me/... link)
FORCE_CHANNEL = os.getenv("FORCE_CHANNEL", "").strip()
FORCE_GROUP = os.getenv("FORCE_GROUP", "").strip()

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise SystemExit("Missing BOT_TOKEN / API_ID / API_HASH in environment or .env")
if not OWNER_IDS:
    raise SystemExit("Missing OWNER_IDS in environment or .env (at least one owner required)")

MIN_LIMIT_HOURS = 7
SEND_INTERVAL = 2
DATA_FILE = Path("bot_data.json")

# Transient Bot API errors that must never kill a background task
BOT_TRANSIENT = (NetworkError, TimedOut, RetryAfter, Forbidden, BadRequest, TelegramError)

# ─── STATE ────────────────────────────────────────────────────────────────────
active_jobs: dict[int, dict[str, asyncio.Task]] = defaultdict(dict)
PHONE_RE = re.compile(r"^\+\d{8,15}$")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("otp_bot")

# ─── PERSISTENCE ──────────────────────────────────────────────────────────────
def load_data() -> dict:
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"access": {}, "open_mode": False, "known_users": []}


def save_data(data: dict) -> None:
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def get_access() -> dict[str, float]:
    return load_data().get("access", {})


def set_access(access: dict[str, float]) -> None:
    data = load_data()
    data["access"] = access
    save_data(data)


def is_open_mode() -> bool:
    return bool(load_data().get("open_mode", False))


def set_open_mode(value: bool) -> None:
    data = load_data()
    data["open_mode"] = value
    save_data(data)


def get_known_users() -> set[int]:
    data = load_data()
    return {int(x) for x in data.get("known_users", []) if str(x).isdigit()}


def add_known_user(user_id: int) -> None:
    data = load_data()
    users = set(data.get("known_users", []))
    users.add(user_id)
    data["known_users"] = list(users)
    save_data(data)


def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS


def has_access(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    if is_open_mode():
        return True
    access = get_access()
    key = str(user_id)
    if key not in access:
        return False
    if time.time() > access[key]:
        del access[key]
        set_access(access)
        return False
    return True


def days_left(user_id: int) -> float:
    access = get_access()
    key = str(user_id)
    if key not in access:
        return 0.0
    remaining = access[key] - time.time()
    return max(0.0, remaining / 86400)


# ─── SAFE SEND ────────────────────────────────────────────────────────────────
async def safe_send(bot, chat_id: int, text: str, **kwargs) -> bool:
    """Send a message; never raise. Returns True on success."""
    try:
        await bot.send_message(chat_id, text, **kwargs)
        return True
    except RetryAfter as e:
        log.warning("RetryAfter %ss while sending to %s — sleeping", e.retry_after, chat_id)
        await asyncio.sleep(int(e.retry_after) + 1)
        try:
            await bot.send_message(chat_id, text, **kwargs)
            return True
        except Exception as ex:
            log.warning("safe_send retry failed for %s: %s", chat_id, ex)
            return False
    except BOT_TRANSIENT as e:
        log.warning("safe_send failed for %s: %s: %s", chat_id, type(e).__name__, e)
        return False
    except Exception as e:
        log.warning("safe_send unexpected for %s: %s: %s", chat_id, type(e).__name__, e)
        return False


# ─── FORCE JOIN ───────────────────────────────────────────────────────────────
async def is_member(bot, chat_id_or_username: str, user_id: int) -> bool:
    if not chat_id_or_username:
        return True
    try:
        target = chat_id_or_username
        if "t.me/" in target:
            part = target.split("t.me/")[-1].split("?")[0]
            if part.startswith("+"):
                return True  # private invite — cannot hard-check
            target = "@" + part if not part.startswith("@") else part

        member = await bot.get_chat_member(target, user_id)
        return member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.RESTRICTED,
        )
    except (BadRequest, Forbidden) as e:
        log.warning("Membership check failed for %s / %s: %s", chat_id_or_username, user_id, e)
        return False
    except Exception as e:
        log.warning("Unexpected membership error: %s", e)
        return False


def make_join_link(value: str) -> str | None:
    if not value:
        return None
    value = value.strip()
    if value.startswith("https://t.me/") or value.startswith("http://t.me/"):
        return value
    if value.startswith("@"):
        return f"https://t.me/{value[1:]}"
    if value.startswith("t.me/"):
        return f"https://{value}"
    return None


async def check_force_join(bot, user_id: int) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    if not is_open_mode():
        return True, "", None

    missing = []
    buttons = []

    if FORCE_CHANNEL:
        is_in = await is_member(bot, FORCE_CHANNEL, user_id)
        if not is_in:
            missing.append("Channel")
            link = make_join_link(FORCE_CHANNEL)
            if link:
                buttons.append(InlineKeyboardButton("📢 Join Channel", url=link))

    if FORCE_GROUP:
        is_in = await is_member(bot, FORCE_GROUP, user_id)
        if not is_in:
            missing.append("Group")
            link = make_join_link(FORCE_GROUP)
            if link:
                buttons.append(InlineKeyboardButton("👥 Join Group", url=link))

    if not missing:
        return True, "", None

    text = (
        "🔒 You must join the required chats before using this bot.\n\n"
        "👇 Tap the buttons below, join both, then send /start again."
    )
    markup = InlineKeyboardMarkup([buttons]) if buttons else None
    return False, text, markup


def access_denied_text() -> str:
    return (
        "🚫 Access denied.\n"
        f"📩 Contact @{OWNER_USERNAME} to request access."
    )


# ─── WORKER ───────────────────────────────────────────────────────────────────
async def notify_owners(bot, text: str):
    """Deliver operational alerts to every configured owner. Never raises."""
    for oid in OWNER_IDS:
        await safe_send(bot, oid, text)


async def otp_worker(user_id: int, phone: str, chat_id: int, bot):
    """
    Continuously request login codes for `phone`.

    Delivery failures (Bot API network / TimedOut / Forbidden / RetryAfter)
    are logged and the loop continues. The task only ends on:
      - long FloodWait (>= MIN_LIMIT_HOURS)
      - PhoneNumberInvalid / PhoneNumberBanned
      - CancelledError (/stop)
    """
    try:
        while True:
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            try:
                await client.connect()
                await client.send_code_request(phone)
                log.info("Login code requested → %s (user %s)", phone, user_id)
            except FloodWaitError as e:
                wait = int(e.seconds)
                hours = wait / 3600.0
                if hours >= MIN_LIMIT_HOURS:
                    user_msg = (
                        f"🛑 Process stopped\n"
                        f"📱 Number: `{phone}`\n"
                        f"⏳ Reason: Telegram rate limit of approximately "
                        f"{hours:.1f} hours ({wait} seconds).\n"
                        f"✅ No further codes will be requested for this number."
                    )
                    await safe_send(bot, chat_id, user_msg, parse_mode="Markdown")
                    await notify_owners(
                        bot,
                        f"Rate limit (≥{MIN_LIMIT_HOURS}h) on `{phone}`\n"
                        f"User ID: `{user_id}`\n"
                        f"Wait: {wait}s (~{hours:.1f}h)\n"
                        f"Job terminated automatically.",
                    )
                    break
                else:
                    log.info(
                        "Short FloodWait %ss (%.2fh) on %s — sleeping then resume",
                        wait, hours, phone,
                    )
                    await asyncio.sleep(wait)
                    continue
            except (PhoneNumberInvalidError, PhoneNumberBannedError) as e:
                err = str(e) or e.__class__.__name__
                user_msg = (
                    f"🛑 Process stopped\n"
                    f"📱 Number: `{phone}`\n"
                    f"❌ Reason: Telegram rejected this number.\n"
                    f"🔎 Detail: {err}"
                )
                await safe_send(bot, chat_id, user_msg, parse_mode="Markdown")
                await notify_owners(
                    bot,
                    f"Number rejected by Telegram\n"
                    f"Number: `{phone}`\n"
                    f"User ID: `{user_id}`\n"
                    f"Error: {err}",
                )
                break
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                # Telethon / network transient — report once, backoff, continue
                log.warning("Error on %s: %s: %s", phone, type(ex).__name__, ex)
                await notify_owners(
                    bot,
                    f"Unexpected error while requesting code\n"
                    f"Number: `{phone}`\n"
                    f"User ID: `{user_id}`\n"
                    f"Error: {type(ex).__name__}: {ex}",
                )
                await asyncio.sleep(5)
                continue
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass

            await asyncio.sleep(SEND_INTERVAL)
    except asyncio.CancelledError:
        log.info("Worker cancelled for %s (user %s)", phone, user_id)
        raise
    except Exception as ex:
        # Absolute last-resort guard — should never reach here
        log.error("Worker fatal for %s (user %s): %s: %s", phone, user_id, type(ex).__name__, ex)
        await notify_owners(
            bot,
            f"Worker crashed unexpectedly\n"
            f"Number: `{phone}`\n"
            f"User ID: `{user_id}`\n"
            f"Error: {type(ex).__name__}: {ex}",
        )
    finally:
        if phone in active_jobs.get(user_id, {}):
            del active_jobs[user_id][phone]
            if not active_jobs[user_id]:
                del active_jobs[user_id]


# ─── HELPERS ──────────────────────────────────────────────────────────────────
def is_valid_phone(text: str) -> bool:
    return bool(PHONE_RE.match(text.strip()))


async def start_job(user_id: int, phone: str, chat_id: int, bot) -> str:
    phone = phone.strip()
    if not is_valid_phone(phone):
        return (
            "❌ Invalid phone number format.\n"
            "📌 Include the country code (example: `+919876543210`)."
        )

    if phone in active_jobs.get(user_id, {}):
        return (
            f"⚠️ A process is already active for `{phone}`.\n"
            f"🛑 Use `/stop {phone}` to end it first."
        )

    task = asyncio.create_task(otp_worker(user_id, phone, chat_id, bot))
    active_jobs[user_id][phone] = task

    return (
        f"🚀 Process started\n"
        f"📱 Number: `{phone}`\n"
        f"✅ Codes are being requested.\n"
        f"🛑 Use /stop to end it anytime."
    )


async def stop_jobs(user_id: int, phone: str | None = None) -> str:
    jobs = active_jobs.get(user_id, {})
    if not jobs:
        return "ℹ️ No active process found."

    if phone:
        phone = phone.strip()
        candidates = [phone]
        if phone.startswith("+"):
            candidates.append(phone[1:])
        else:
            candidates.append("+" + phone)
        matched = None
        for cand in candidates:
            if cand in jobs:
                matched = cand
                break
        if matched is None:
            for key in list(jobs.keys()):
                if key.lstrip("+") == phone.lstrip("+"):
                    matched = key
                    break
        if matched is None:
            return "ℹ️ No active process found."

        task = jobs.pop(matched, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if not jobs:
            del active_jobs[user_id]
        return f"🛑 Stopped sending codes on {matched}."

    numbers = list(jobs.keys())
    count = len(numbers)
    for t in list(jobs.values()):
        t.cancel()
    for t in list(jobs.values()):
        try:
            await t
        except asyncio.CancelledError:
            pass
    del active_jobs[user_id]

    if count == 1:
        return f"🛑 Stopped sending codes on {numbers[0]}."
    return f"🛑 The bot stopped sending codes to {count} numbers."


# ─── ACCESS GATE ──────────────────────────────────────────────────────────────
async def gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    add_known_user(uid)

    if is_owner(uid):
        return True

    if is_open_mode():
        ok, msg, markup = await check_force_join(context.bot, uid)
        if not ok:
            await safe_send(context.bot, update.effective_chat.id, msg, reply_markup=markup)
            return False
        return True

    if not has_access(uid):
        await safe_send(context.bot, update.effective_chat.id, access_denied_text())
        return False
    return True


# ─── HANDLERS ─────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    add_known_user(uid)

    if not await gate(update, context):
        return

    text = (
        "👋 Welcome.\n\n"
        "⚡ Send a phone number and the bot will start requesting login codes for it.\n\n"
        "💬 Private chat: just send the number (example: +919876543210)\n"
        "👥 Group chat: /spam +919876543210\n\n"
        "🛑 /stop — stop all your active processes\n"
        "🛑 /stop +919876543210 — stop one number\n"
    )
    if is_owner(uid):
        text += (
            "\n👑 Owner commands\n"
            "➕ /adduser <user_id> <days>\n"
            "➖ /removeuser <user_id>\n"
            "📋 /showuser\n"
            "🔓 /open — allow all users (force-join still required)\n"
            "🔒 /lock — restrict to granted users only\n"
            "📢 /broadcast <message> — message all known users\n"
        )
    await safe_send(context.bot, update.effective_chat.id, text)


async def cmd_spam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await gate(update, context):
        return

    if not context.args:
        await safe_send(context.bot, update.effective_chat.id, "📌 Usage: /spam +919876543210")
        return

    phone = context.args[0]
    reply = await start_job(
        update.effective_user.id,
        phone,
        update.effective_chat.id,
        context.bot,
    )
    await safe_send(context.bot, update.effective_chat.id, reply, parse_mode="Markdown")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await gate(update, context):
        return

    phone = context.args[0] if context.args else None
    reply = await stop_jobs(update.effective_user.id, phone)
    await safe_send(context.bot, update.effective_chat.id, reply)


async def plain_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    if not await gate(update, context):
        return

    text = (update.message.text or "").strip()
    if not is_valid_phone(text):
        return

    reply = await start_job(
        update.effective_user.id,
        text,
        update.effective_chat.id,
        context.bot,
    )
    await safe_send(context.bot, update.effective_chat.id, reply, parse_mode="Markdown")


# ─── OWNER COMMANDS ───────────────────────────────────────────────────────────
async def cmd_adduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if len(context.args) < 2:
        await safe_send(context.bot, update.effective_chat.id, "📌 Usage: /adduser <user_id> <days>")
        return
    try:
        uid = int(context.args[0])
        days = float(context.args[1])
    except ValueError:
        await safe_send(context.bot, update.effective_chat.id, "❌ user_id and days must be numbers.")
        return

    access = get_access()
    access[str(uid)] = time.time() + days * 86400
    set_access(access)
    add_known_user(uid)
    await safe_send(context.bot, update.effective_chat.id, f"✅ User `{uid}` granted access for {days} day(s).")


async def cmd_removeuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if not context.args:
        await safe_send(context.bot, update.effective_chat.id, "📌 Usage: /removeuser <user_id>")
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await safe_send(context.bot, update.effective_chat.id, "❌ user_id must be a number.")
        return

    access = get_access()
    if str(uid) in access:
        del access[str(uid)]
        set_access(access)
        await safe_send(context.bot, update.effective_chat.id, f"✅ User `{uid}` removed.")
    else:
        await safe_send(context.bot, update.effective_chat.id, f"⚠️ User `{uid}` was not in the access list.")


async def cmd_showuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    access = get_access()
    if not access:
        await safe_send(context.bot, update.effective_chat.id, "ℹ️ No users in access list.")
        return
    lines = []
    now = time.time()
    for uid, exp in access.items():
        left = max(0.0, (exp - now) / 86400)
        lines.append(f"• {uid} — {left:.1f} days left")
    await safe_send(context.bot, update.effective_chat.id, "📋 Access list:\n" + "\n".join(lines))


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    set_open_mode(True)
    await safe_send(
        context.bot,
        update.effective_chat.id,
        "🔓 Access mode: open.\n"
        "✅ Any user who joins the required channel and group may use the bot.",
    )


async def cmd_lock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    set_open_mode(False)
    await safe_send(
        context.bot,
        update.effective_chat.id,
        "🔒 Access mode: locked.\n"
        "👑 Only owners and users granted with /adduser may use the bot.",
    )


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return

    text = " ".join(context.args).strip() if context.args else ""
    if not text and update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    if not text:
        await safe_send(
            context.bot,
            update.effective_chat.id,
            "📌 Usage:\n"
            "📢 /broadcast <message>\n"
            "↩️ or reply to a message with /broadcast",
        )
        return

    users = get_known_users()
    for uid_str in get_access():
        users.add(int(uid_str))
    users.update(OWNER_IDS)

    sent = 0
    failed = 0
    for uid in users:
        ok = await safe_send(context.bot, uid, text)
        if ok:
            sent += 1
        else:
            failed += 1
        await asyncio.sleep(0.05)

    await safe_send(
        context.bot,
        update.effective_chat.id,
        f"📢 Broadcast complete.\n✅ Delivered: {sent} | ❌ Failed: {failed}",
    )


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("spam", cmd_spam))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("adduser", cmd_adduser))
    app.add_handler(CommandHandler("removeuser", cmd_removeuser))
    app.add_handler(CommandHandler("showuser", cmd_showuser))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("lock", cmd_lock))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, plain_phone))

    log.info("Bot starting (VPS)…")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
