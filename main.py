"""
╔══════════════════════════════════════════════════════════════════╗
║      ᴀᴅᴠᴀɴᴄᴇᴅ ᴍᴜʟᴛɪ-ᴀᴄᴄᴏᴜɴᴛ ᴛᴇʟᴇɢʀᴀᴍ ᴀᴜᴛᴏᴍᴀᴛᴏʀ             ║
║                   ᴠ3.0 — ᴘʀᴏ ᴇᴅɪᴛɪᴏɴ                          ║
║   Features: Proxy | Media | FloodWait | HealthCheck | CRON     ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import random
import csv
import io
import logging
from datetime import datetime
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from pyrogram.errors import (
    FloodWait, UserBlocked, AuthKeyUnregistered,
    SessionExpired, PhoneNumberBanned, PeerIdInvalid,
    ChatWriteForbidden, UserNotParticipant, UserDeactivated,
    UserDeactivatedBan
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from motor.motor_asyncio import AsyncIOMotorClient

# ════════════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════════════
BOT_TOKEN   = "8996403565:AAHsR8DiDZKWRk0jMKudDfozp0B6VTULRC0"
API_ID      = 37729457
API_HASH    = "bb68973b7efcbbd074cda984c95502d6"
MONGO_URL   = "mongodb://localhost:27017"
OWNER_ID    = 7670992701
ADMIN_IDS   = [OWNER_ID]
LOG_CHANNEL = -1003993752133  # set to channel ID to enable

# ════════════════════════════════════════════════════════════════
#  SMALL CAPS HELPER
# ════════════════════════════════════════════════════════════════
_SC = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘQʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘQʀsᴛᴜᴠᴡxʏᴢ"
)

def sc(text: str) -> str:
    """Convert string to small caps Unicode"""
    return text.translate(_SC)

# ════════════════════════════════════════════════════════════════
#  ENUMS & LOGGING
# ════════════════════════════════════════════════════════════════
class RotationMode(Enum):
    SEQUENTIAL  = "sequential"
    RANDOM      = "random"
    ROUND_ROBIN = "round_robin"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════
#  DATABASE MANAGER
# ════════════════════════════════════════════════════════════════
class DB:
    def __init__(self, url: str):
        client      = AsyncIOMotorClient(url)
        d           = client["telegram_automator"]
        self.acc    = d["accounts"]
        self.cfg    = d["settings"]
        self.tmpl   = d["templates"]
        self.stats  = d["stats"]
        self.bl     = d["blacklist"]
        self.logs   = d["logs"]

    async def get(self, key: str, default=None):
        doc = await self.cfg.find_one({"key": key})
        return doc["value"] if doc else default

    async def set(self, key: str, value):
        await self.cfg.update_one(
            {"key": key},
            {"$set": {"value": value, "updated": datetime.utcnow()}},
            upsert=True
        )

    async def log(self, event: str, data: dict):
        await self.logs.insert_one({
            "event": event, "data": data,
            "ts": datetime.utcnow()
        })

    async def acc_stats(self, acc_id, sent=0, failed=0, floods=0):
        await self.acc.update_one(
            {"_id": acc_id},
            {"$inc": {
                "stats.sent": sent,
                "stats.failed": failed,
                "stats.floods": floods
            }, "$set": {"stats.last_active": datetime.utcnow()}}
        )

    async def global_stats(self, sent=0, failed=0):
        day = datetime.utcnow().strftime("%Y-%m-%d")
        await self.stats.update_one(
            {"date": day},
            {"$inc": {"sent": sent, "failed": failed},
             "$setOnInsert": {"date": day}},
            upsert=True
        )

# ════════════════════════════════════════════════════════════════
#  ACCOUNT MANAGER
# ════════════════════════════════════════════════════════════════
class AccountManager:
    def __init__(self, db: DB):
        self.db  = db
        self._rr = 0

    async def add(self, api_id: int, api_hash: str, session: str,
                  proxy: Optional[dict] = None, label: str = None) -> dict:
        ub = Client(
            name="validate_tmp",
            api_id=api_id, api_hash=api_hash,
            session_string=session, in_memory=True
        )
        try:
            await ub.start()
            me = await ub.get_me()
            await ub.stop()
        except Exception as e:
            return {"ok": False, "error": str(e)}

        doc = {
            "api_id": api_id, "api_hash": api_hash,
            "session": session, "proxy": proxy,
            "label": label or me.first_name,
            "username": me.username or "N/A",
            "phone": me.phone_number or "N/A",
            "active": True, "banned": False,
            "added": datetime.utcnow(),
            "stats": {"sent": 0, "failed": 0, "floods": 0, "last_active": None}
        }
        await self.db.acc.update_one(
            {"session": session}, {"$set": doc}, upsert=True
        )
        return {"ok": True, "name": me.first_name,
                "username": me.username, "phone": me.phone_number}

    async def active_list(self) -> list:
        return await self.db.acc.find(
            {"active": True, "banned": False}
        ).to_list(None)

    async def mark_banned(self, acc_id):
        await self.db.acc.update_one(
            {"_id": acc_id},
            {"$set": {"banned": True, "active": False}}
        )

    async def toggle(self, acc_id) -> bool:
        acc = await self.db.acc.find_one({"_id": acc_id})
        new = not acc.get("active", True)
        await self.db.acc.update_one({"_id": acc_id}, {"$set": {"active": new}})
        return new

    async def pick(self, accounts: list, mode: RotationMode) -> Optional[dict]:
        if not accounts:
            return None
        if mode == RotationMode.RANDOM:
            return random.choice(accounts)
        # Default to ROUND_ROBIN for SEQUENTIAL as well in the one-by-one loop
        self._rr %= len(accounts)
        acc = accounts[self._rr]
        self._rr += 1
        return acc

    async def health_check(self) -> dict:
        accounts = await self.active_list()
        alive, dead, details = 0, 0, []

        for acc in accounts:
            ub = Client(
                name=f"hc_{acc['_id']}",
                api_id=acc["api_id"], api_hash=acc["api_hash"],
                session_string=acc["session"], in_memory=True,
                proxy=acc.get("proxy")
            )
            try:
                await ub.start()
                me = await ub.get_me()
                await ub.stop()
                alive += 1
                details.append({
                    "label": acc.get("label"),
                    "status": sc("ALIVE"),
                    "note": f"@{me.username}"
                })
            except (AuthKeyUnregistered, SessionExpired,
                    UserDeactivated, UserDeactivatedBan, PhoneNumberBanned) as e:
                await self.mark_banned(acc["_id"])
                dead += 1
                details.append({
                    "label": acc.get("label"),
                    "status": sc("DEAD") + f" ({type(e).__name__})",
                    "note": sc("auto-banned in db")
                })
            except Exception as e:
                dead += 1
                details.append({
                    "label": acc.get("label"),
                    "status": sc("ERROR"),
                    "note": str(e)[:40]
                })

        return {"alive": alive, "dead": dead, "details": details}

    def make_client(self, acc: dict, name_suffix: str = "") -> Client:
        return Client(
            name=f"ub_{acc['_id']}{name_suffix}",
            api_id=acc["api_id"], api_hash=acc["api_hash"],
            session_string=acc["session"], in_memory=True,
            proxy=acc.get("proxy")
        )

# ════════════════════════════════════════════════════════════════
#  BROADCAST ENGINE
# ════════════════════════════════════════════════════════════════
class Engine:
    def __init__(self, db: DB, am: AccountManager, bot: Client):
        self.db         = db
        self.am         = am
        self.bot        = bot
        self.running    = False
        self.live_stats = {"sent": 0, "failed": 0, "started": None}

    async def pick_template(self) -> Optional[dict]:
        mode  = await self.db.get("tmpl_mode", "random")
        query = {"active": True}
        total = await self.db.tmpl.count_documents(query)
        if total == 0:
            return None

        if mode == "random":
            skip = random.randint(0, total - 1)
        else:
            idx  = await self.db.get("tmpl_idx", 0)
            skip = idx % total
            await self.db.set("tmpl_idx", (idx + 1) % total)

        cur = self.db.tmpl.find(query).skip(skip).limit(1)
        res = await cur.to_list(1)
        return res[0] if res else None

    async def smart_send(self, ub: Client, target, tmpl: dict,
                         retries: int = 3) -> dict:
        PERMANENT_ERRORS = (
            ChatWriteForbidden, UserNotParticipant, PeerIdInvalid
        )
        for attempt in range(retries):
            try:
                t = tmpl.get("type", "text")
                if t == "text":
                    await ub.send_message(
                        target, tmpl["content"],
                        parse_mode=enums.ParseMode.MARKDOWN,
                        disable_web_page_preview=tmpl.get("no_preview", False)
                    )
                elif t == "photo":
                    await ub.send_photo(target, tmpl["file_id"],
                                        caption=tmpl.get("caption", ""))
                elif t == "video":
                    await ub.send_video(target, tmpl["file_id"],
                                        caption=tmpl.get("caption", ""))
                elif t == "document":
                    await ub.send_document(target, tmpl["file_id"],
                                           caption=tmpl.get("caption", ""))
                elif t == "poll":
                    await ub.send_poll(
                        target,
                        question=tmpl["question"],
                        options=tmpl["options"],
                        is_anonymous=tmpl.get("anonymous", True)
                    )
                return {"ok": True}

            except FloodWait as fw:
                wait = fw.value + random.randint(5, 15)
                log.warning(f"FloodWait {fw.value}s -> sleeping {wait}s")
                await self.db.log("flood_wait",
                                  {"target": str(target), "wait": fw.value})
                await asyncio.sleep(wait)

            except PERMANENT_ERRORS as e:
                return {"ok": False, "error": str(e), "permanent": True}

            except Exception as e:
                if attempt == retries - 1:
                    return {"ok": False, "error": str(e)}
                await asyncio.sleep(2 ** attempt)

        return {"ok": False, "error": "Max retries exceeded"}

    async def broadcast(self):
        if self.running:
            log.warning("Broadcast already running — skipping cycle")
            return

        self.running    = True
        self.live_stats = {"sent": 0, "failed": 0, "started": datetime.utcnow()}
        clients = {}

        try:
            targets    = await self.db.get("targets", [])
            bl         = await self.db.bl.distinct("target")
            targets    = [t for t in targets if str(t) not in [str(b) for b in bl]]
            mode_str   = await self.db.get("rotation", "sequential")
            mode       = RotationMode(mode_str)
            min_d      = await self.db.get("min_delay", 3)
            max_d      = await self.db.get("max_delay", 8)
            accounts   = await self.am.active_list()
            template   = await self.pick_template()

            if not targets:
                log.warning("No targets — aborting."); return
            if not accounts:
                log.warning("No active accounts — aborting."); return
            if not template:
                log.warning("No templates — aborting."); return

            log.info(f"Broadcast | Accounts:{len(accounts)} Targets:{len(targets)}")

            # Pre-start all clients
            for acc in accounts:
                client = self.am.make_client(acc, "_brd")
                try:
                    await client.start()
                    clients[str(acc["_id"])] = client
                except (AuthKeyUnregistered, SessionExpired, UserDeactivated, UserDeactivatedBan, PhoneNumberBanned) as e:
                    log.error(f"[{acc.get('label')}] Dead session on start: {e}")
                    await self.am.mark_banned(acc["_id"])
                except Exception as e:
                    log.error(f"Failed to start client for {acc.get('label')}: {e}")

            if not clients:
                log.error("No clients started — aborting.")
                return

            # Main one-by-one loop
            for target in targets:
                # Re-pick only from accounts whose clients successfully started
                available_accs = [a for a in accounts if str(a["_id"]) in clients]
                if not available_accs:
                    log.error("All accounts became unavailable.")
                    break

                acc = await self.am.pick(available_accs, mode)
                if not acc: break

                client = clients.get(str(acc["_id"]))
                try:
                    res = await self.smart_send(client, target, template)
                    if res["ok"]:
                        self.live_stats["sent"] += 1
                        await self.db.acc_stats(acc["_id"], sent=1)
                    else:
                        self.live_stats["failed"] += 1
                        await self.db.acc_stats(acc["_id"], failed=1)
                        if res.get("permanent"):
                            log.warning(f"Perm error -> {target}: {res['error']}")
                except (AuthKeyUnregistered, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
                    log.error(f"[{acc.get('label')}] Session died during broadcast: {e}")
                    await self.am.mark_banned(acc["_id"])
                    try: await client.stop()
                    except: pass
                    clients.pop(str(acc["_id"]), None)
                    continue
                except Exception as e:
                    log.error(f"Error sending from {acc.get('label')}: {e}")
                    self.live_stats["failed"] += 1
                    await self.db.acc_stats(acc["_id"], failed=1)

                await asyncio.sleep(random.uniform(min_d, max_d))

            dur = (datetime.utcnow() - self.live_stats["started"]).seconds
            summary = (
                f"**{sc('Broadcast Complete')}**\n\n"
                f"`{sc('sent')}   ` `{self.live_stats['sent']}`\n"
                f"`{sc('failed')} ` `{self.live_stats['failed']}`\n"
                f"`{sc('time')}   ` `{dur}s`"
            )
            log.info(f"Broadcast done | sent={self.live_stats['sent']} failed={self.live_stats['failed']} dur={dur}s")
            await self.db.global_stats(self.live_stats["sent"],
                                       self.live_stats["failed"])
            if LOG_CHANNEL:
                try:
                    await self.bot.send_message(LOG_CHANNEL, summary)
                except Exception:
                    pass

        except Exception as e:
            log.error(f"Broadcast crashed: {e}")
        finally:
            for c in clients.values():
                try: await c.stop()
                except: pass
            self.running = False

# ════════════════════════════════════════════════════════════════
#  INIT
# ════════════════════════════════════════════════════════════════
bot       = Client("Automator_v3", api_id=API_ID, api_hash=API_HASH,
                   bot_token=BOT_TOKEN)
db        = DB(MONGO_URL)
am        = AccountManager(db)
engine    = Engine(db, am, bot)
scheduler = AsyncIOScheduler()

# ════════════════════════════════════════════════════════════════
#  DECORATORS
# ════════════════════════════════════════════════════════════════
def admin_only(fn):
    async def wrapper(client, message: Message):
        if message.from_user.id not in ADMIN_IDS:
            return
        return await fn(client, message)
    wrapper.__name__ = fn.__name__
    return wrapper

def cb_admin(fn):
    async def wrapper(client, cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            return
        return await fn(client, cb)
    wrapper.__name__ = fn.__name__
    return wrapper

# ════════════════════════════════════════════════════════════════
#  UI HELPERS  —  small-caps buttons, no emojis
# ════════════════════════════════════════════════════════════════
def main_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(sc("Accounts"),   callback_data="m:accounts"),
            InlineKeyboardButton(sc("Templates"),  callback_data="m:templates"),
            InlineKeyboardButton(sc("Targets"),    callback_data="m:targets"),
        ],
        [
            InlineKeyboardButton(sc("Scheduler"),  callback_data="m:scheduler"),
            InlineKeyboardButton(sc("Settings"),   callback_data="m:settings"),
            InlineKeyboardButton(sc("Stats"),      callback_data="m:stats"),
        ],
        [
            InlineKeyboardButton(sc("Health Check"), callback_data="healthcheck"),
            InlineKeyboardButton(sc("Run Now"),       callback_data="runnow"),
        ],
        [
            InlineKeyboardButton(sc("Blacklist"),     callback_data="m:blacklist"),
            InlineKeyboardButton(sc("Export Data"),   callback_data="m:export"),
        ],
    ])

def back_btn(dest="m:home"):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(sc("Back"), callback_data=dest)
    ]])

def back_home():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(sc("Back to Menu"), callback_data="m:home")
    ]])

# ════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ════════════════════════════════════════════════════════════════

@bot.on_message(filters.command("start") & filters.private)
@admin_only
async def cmd_start(client: Client, message: Message):
    acc_count  = await db.acc.count_documents({"active": True, "banned": False})
    tmpl_count = await db.tmpl.count_documents({"active": True})
    targets    = await db.get("targets", [])
    job        = scheduler.get_job("broadcast_job")
    sched_info = f"`{job.next_run_time.strftime('%H:%M UTC')}`" if job else f"`{sc('off')}`"

    await message.reply_text(
        f"**{sc('Multi-Account Automator')}**\n"
        f"**{sc('v3.0 Pro Edition')}**\n"
        f"{'─' * 28}\n\n"
        f"`{sc('accounts')}  ` `{acc_count}` {sc('active')}\n"
        f"`{sc('templates')} ` `{tmpl_count}`\n"
        f"`{sc('targets')}   ` `{len(targets)}`\n"
        f"`{sc('scheduler')} ` {sched_info}\n"
        f"`{sc('engine')}    ` `{'ʀᴜɴɴɪɴɢ' if engine.running else 'ɪᴅʟᴇ'}`",
        reply_markup=main_menu()
    )


# ─── ACCOUNT COMMANDS ──────────────────────────────────────────

@bot.on_message(filters.command("add_account") & filters.private)
@admin_only
async def cmd_add_account(client: Client, message: Message):
    """
    /add_account API_ID|API_HASH|SESSION[|PROXY][|LABEL]
    Proxy format: socks5://user:pass@host:port
    """
    try:
        raw   = message.text.split(maxsplit=1)[1]
        parts = [p.strip() for p in raw.split("|")]
        api_id, api_hash, session = int(parts[0]), parts[1], parts[2]

        proxy = None
        if len(parts) > 3 and parts[3].startswith(("socks", "http")):
            p = urlparse(parts[3])
            proxy = {"scheme": p.scheme, "hostname": p.hostname,
                     "port": p.port, "username": p.username,
                     "password": p.password}

        label = parts[4] if len(parts) > 4 else (
                parts[3] if len(parts) > 3 and not parts[3].startswith(("socks","http")) else None)

        wait = await message.reply_text(f"`{sc('validating session...')}`")
        res  = await am.add(api_id, api_hash, session, proxy, label)

        if res["ok"]:
            await wait.edit_text(
                f"**{sc('Account Added')}**\n"
                f"{'─' * 22}\n\n"
                f"`{sc('name')}    ` `{res['name']}`\n"
                f"`{sc('username')}` @{res['username'] or 'none'}\n"
                f"`{sc('phone')}   ` `{res['phone']}`\n"
                f"`{sc('label')}   ` `{label or res['name']}`\n"
                f"`{sc('proxy')}   ` `{'yes' if proxy else 'none'}`",
                reply_markup=back_home()
            )
        else:
            await wait.edit_text(
                f"**{sc('Validation Failed')}**\n\n`{res['error']}`",
                reply_markup=back_home()
            )

    except (IndexError, ValueError):
        await message.reply_text(
            f"**{sc('Usage')}**\n\n"
            f"`/add_account API_ID|API_HASH|SESSION`\n"
            f"`/add_account API_ID|API_HASH|SESSION|socks5://user:pass@1.2.3.4:1080`\n"
            f"`/add_account API_ID|API_HASH|SESSION|PROXY|MyLabel`"
        )


@bot.on_message(filters.command("list_accounts") & filters.private)
@admin_only
async def cmd_list_accounts(client: Client, message: Message):
    accounts = await am.active_list()
    if not accounts:
        return await message.reply_text(f"`{sc('no active accounts found')}`")

    lines = []
    for i, acc in enumerate(accounts, 1):
        s = acc.get("stats", {})
        lines.append(
            f"`{i:02d}` **{acc.get('label')}** · @{acc.get('username','?')}\n"
            f"     `{sc('sent')} {s.get('sent',0)}` · `{sc('failed')} {s.get('failed',0)}`"
        )

    await message.reply_text(
        f"**{sc('Active Accounts')}** `[{len(accounts)}]`\n"
        f"{'─' * 28}\n\n" + "\n".join(lines),
        reply_markup=back_home()
    )


# ─── TEMPLATE COMMANDS ─────────────────────────────────────────

@bot.on_message(filters.command("add_template") & filters.private)
@admin_only
async def cmd_add_template(client: Client, message: Message):
    """
    /add_template Message text          -> text template
    /add_template [caption]             -> reply to photo/video/doc
    /add_template poll|Question?|Opt1|Opt2
    """
    try:
        raw  = message.text.split(maxsplit=1)
        body = raw[1] if len(raw) > 1 else ""
        doc  = None

        if body.startswith("poll|"):
            parts = body.split("|")
            doc = {
                "type": "poll",
                "question": parts[1],
                "options": parts[2:],
                "anonymous": True,
                "active": True,
                "created": datetime.utcnow(),
                "used": 0
            }
        elif message.reply_to_message:
            reply = message.reply_to_message
            if reply.photo:
                fid, mtype = reply.photo.file_id, "photo"
            elif reply.video:
                fid, mtype = reply.video.file_id, "video"
            elif reply.document:
                fid, mtype = reply.document.file_id, "document"
            else:
                return await message.reply_text(
                    f"`{sc('reply to a photo, video or document')}`"
                )
            doc = {
                "type": mtype, "file_id": fid, "caption": body,
                "active": True, "created": datetime.utcnow(), "used": 0
            }
        else:
            if not body:
                raise IndexError
            doc = {
                "type": "text", "content": body, "no_preview": False,
                "active": True, "created": datetime.utcnow(), "used": 0
            }

        res   = await db.tmpl.insert_one(doc)
        total = await db.tmpl.count_documents({"active": True})
        await message.reply_text(
            f"**{sc('Template Saved')}**\n"
            f"{'─' * 22}\n\n"
            f"`{sc('id')}    ` `{res.inserted_id}`\n"
            f"`{sc('type')}  ` `{doc['type'].upper()}`\n"
            f"`{sc('total')} ` `{total}` {sc('active templates')}",
            reply_markup=back_home()
        )

    except IndexError:
        await message.reply_text(
            f"**{sc('Template Usage')}**\n\n"
            f"**{sc('text')}**\n`/add_template {sc('your message here')}`\n\n"
            f"**{sc('media')}** _({sc('reply to photo/video')})_\n`/add_template {sc('caption')}`\n\n"
            f"**{sc('poll')}**\n`/add_template poll|{sc('question')}?|{sc('option')} 1|{sc('option')} 2`"
        )


@bot.on_message(filters.command("list_templates") & filters.private)
@admin_only
async def cmd_list_templates(client: Client, message: Message):
    templates = await db.tmpl.find({"active": True}).to_list(None)
    if not templates:
        return await message.reply_text(f"`{sc('no templates found')}`")

    lines = []
    for i, t in enumerate(templates, 1):
        preview = t.get("content", t.get("caption", t.get("question", "")))[:38]
        lines.append(f"`{i:02d}` `[{sc(t['type'])}]` {preview}...")

    await message.reply_text(
        f"**{sc('Templates')}** `[{len(templates)}]`\n"
        f"{'─' * 28}\n\n" + "\n".join(lines),
        reply_markup=back_home()
    )


@bot.on_message(filters.command("del_template") & filters.private)
@admin_only
async def cmd_del_template(client: Client, message: Message):
    try:
        n = int(message.text.split()[1]) - 1
        templates = await db.tmpl.find({"active": True}).to_list(None)
        if n < 0 or n >= len(templates):
            return await message.reply_text(f"`{sc('invalid number')}`")
        await db.tmpl.update_one({"_id": templates[n]["_id"]},
                                  {"$set": {"active": False}})
        await message.reply_text(
            f"**{sc('Template Deleted')}**\n`#{n+1}` {sc('removed')}",
            reply_markup=back_home()
        )
    except (IndexError, ValueError):
        await message.reply_text(f"`{sc('usage')}` `/del_template 2`")


# ─── TARGET COMMANDS ───────────────────────────────────────────

@bot.on_message(filters.command("set_targets") & filters.private)
@admin_only
async def cmd_set_targets(client: Client, message: Message):
    """
    /set_targets @g1, -10012345, @user2
    Or reply to .txt file (one per line or comma-separated)
    """
    try:
        if message.reply_to_message and message.reply_to_message.document:
            file  = await client.download_media(
                message.reply_to_message.document.file_id, in_memory=True)
            text  = bytes(file.getbuffer()).decode("utf-8")
            raw   = text.replace("\n", ",")
        else:
            raw = message.text.split(maxsplit=1)[1]

        targets = []
        for t in raw.split(","):
            t = t.strip()
            if not t: continue
            try: targets.append(int(t))
            except ValueError: targets.append(t)

        await db.set("targets", targets)
        preview = "\n".join([f"  `{t}`" for t in targets[:5]])
        extra   = f"\n  `+{len(targets)-5} {sc('more')}`" if len(targets) > 5 else ""
        await message.reply_text(
            f"**{sc('Targets Saved')}** `[{len(targets)}]`\n"
            f"{'─' * 22}\n\n{preview}{extra}",
            reply_markup=back_home()
        )

    except (IndexError, Exception) as e:
        await message.reply_text(
            f"**{sc('Usage')}**\n\n"
            f"`/set_targets @g1, -10012345, @user`\n\n"
            f"_{sc('or attach a .txt file (comma/newline separated)')}_"
        )


@bot.on_message(filters.command("add_target") & filters.private)
@admin_only
async def cmd_add_target(client: Client, message: Message):
    try:
        raw      = message.text.split(maxsplit=1)[1]
        new      = []
        for t in raw.split(","):
            t = t.strip()
            if not t: continue
            try: new.append(int(t))
            except ValueError: new.append(t)

        existing = await db.get("targets", [])
        merged   = list({str(t): t for t in (existing + new)}.values())
        await db.set("targets", merged)
        await message.reply_text(
            f"**{sc('Targets Added')}** `[+{len(new)}]`\n"
            f"`{sc('total')}` `{len(merged)}`",
            reply_markup=back_home()
        )
    except IndexError:
        await message.reply_text(f"`{sc('usage')}` `/add_target @group1, @group2`")


@bot.on_message(filters.command("clear_targets") & filters.private)
@admin_only
async def cmd_clear_targets(client: Client, message: Message):
    await db.set("targets", [])
    await message.reply_text(
        f"**{sc('Targets Cleared')}**\n`{sc('target list is now empty')}`",
        reply_markup=back_home()
    )


@bot.on_message(filters.command("export_targets") & filters.private)
@admin_only
async def cmd_export_targets(client: Client, message: Message):
    targets = await db.get("targets", [])
    if not targets:
        return await message.reply_text(f"`{sc('no targets found')}`")
    content  = "\n".join([str(t) for t in targets])
    buf      = io.BytesIO(content.encode())
    buf.name = f"targets_{datetime.utcnow():%Y%m%d_%H%M%S}.txt"
    await message.reply_document(
        buf, caption=f"`{sc('exported')}` `{len(targets)}` {sc('targets')}"
    )


@bot.on_message(filters.command("export_accounts") & filters.private)
@admin_only
async def cmd_export_accounts(client: Client, message: Message):
    accounts = await am.active_list()
    if not accounts:
        return await message.reply_text(f"`{sc('no accounts found')}`")
    rows = [["Label","Username","Phone","Sent","Failed","Active"]]
    for a in accounts:
        s = a.get("stats", {})
        rows.append([a.get("label","N/A"), a.get("username","N/A"),
                     a.get("phone","N/A"), s.get("sent",0),
                     s.get("failed",0), a.get("active",True)])
    out = io.StringIO()
    csv.writer(out).writerows(rows)
    buf      = io.BytesIO(out.getvalue().encode())
    buf.name = f"accounts_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
    await message.reply_document(
        buf, caption=f"`{sc('exported')}` `{len(accounts)}` {sc('accounts')}"
    )


# ─── BLACKLIST COMMANDS ────────────────────────────────────────

@bot.on_message(filters.command("blacklist") & filters.private)
@admin_only
async def cmd_blacklist(client: Client, message: Message):
    """
    /blacklist list
    /blacklist add @g1, -1001234
    /blacklist remove @g1
    """
    try:
        parts  = message.text.split(maxsplit=2)
        action = parts[1].lower()

        if action == "list":
            items = await db.bl.find().to_list(None)
            if not items:
                return await message.reply_text(
                    f"`{sc('blacklist is empty')}`", reply_markup=back_home()
                )
            text  = f"**{sc('Blacklisted Targets')}**\n{'─' * 22}\n\n"
            text += "\n".join([f"  `{i['target']}`" for i in items])
            await message.reply_text(text, reply_markup=back_home())

        elif action == "add":
            targets = [t.strip() for t in parts[2].split(",") if t.strip()]
            for t in targets:
                await db.bl.update_one(
                    {"target": t},
                    {"$set": {"target": t, "added": datetime.utcnow()}},
                    upsert=True
                )
            await message.reply_text(
                f"**{sc('Blacklisted')}** `{len(targets)}` {sc('targets')}",
                reply_markup=back_home()
            )

        elif action == "remove":
            t = parts[2].strip()
            await db.bl.delete_one({"target": t})
            await message.reply_text(
                f"`{t}` **{sc('removed from blacklist')}**",
                reply_markup=back_home()
            )
        else:
            raise ValueError("unknown action")

    except (IndexError, ValueError):
        await message.reply_text(
            f"**{sc('Blacklist Commands')}**\n\n"
            f"`/blacklist list`\n"
            f"`/blacklist add @g1, @g2`\n"
            f"`/blacklist remove @g1`"
        )


# ─── SCHEDULER COMMANDS ────────────────────────────────────────

@bot.on_message(filters.command("start_job") & filters.private)
@admin_only
async def cmd_start_job(client: Client, message: Message):
    """
    /start_job interval 15          -> every 15 minutes
    /start_job cron 0 9 * * *       -> daily at 9:00 UTC
    /start_job cron 30 8,20 * * 1-5 -> weekdays 8:30 & 20:30
    """
    try:
        parts = message.text.split()
        jtype = parts[1].lower()

        if scheduler.get_job("broadcast_job"):
            scheduler.remove_job("broadcast_job")

        if jtype == "interval":
            mins = int(parts[2])
            scheduler.add_job(
                engine.broadcast, "interval",
                minutes=mins, id="broadcast_job",
                next_run_time=datetime.utcnow()
            )
            await message.reply_text(
                f"**{sc('Interval Job Active')}**\n"
                f"{'─' * 22}\n\n"
                f"`{sc('interval')} ` `{mins} {sc('minutes')}`\n"
                f"`{sc('first run')}` `{sc('now')}`",
                reply_markup=back_home()
            )

        elif jtype == "cron":
            expr   = " ".join(parts[2:])
            cparts = expr.split()
            if len(cparts) != 5:
                raise ValueError("Need exactly 5 cron parts")
            trigger = CronTrigger(
                minute=cparts[0], hour=cparts[1],
                day=cparts[2], month=cparts[3], day_of_week=cparts[4]
            )
            scheduler.add_job(engine.broadcast, trigger, id="broadcast_job")
            job = scheduler.get_job("broadcast_job")
            await message.reply_text(
                f"**{sc('Cron Job Active')}**\n"
                f"{'─' * 22}\n\n"
                f"`{sc('expression')}` `{expr}`\n"
                f"`{sc('next run')}  ` `{job.next_run_time:%Y-%m-%d %H:%M UTC}`",
                reply_markup=back_home()
            )
        else:
            raise ValueError(f"unknown type: {jtype}")

    except Exception as e:
        await message.reply_text(
            f"**{sc('Usage')}**\n\n"
            f"`/start_job interval 15`\n"
            f"`/start_job cron 0 9 * * *`\n"
            f"`/start_job cron */30 * * * *`\n\n"
            f"`{sc('error')}` `{e}`"
        )


@bot.on_message(filters.command("stop_job") & filters.private)
@admin_only
async def cmd_stop_job(client: Client, message: Message):
    if scheduler.get_job("broadcast_job"):
        scheduler.remove_job("broadcast_job")
        await message.reply_text(
            f"**{sc('Job Stopped')}**\n`{sc('scheduler is now off')}`",
            reply_markup=back_home()
        )
    else:
        await message.reply_text(f"`{sc('no active job found')}`")


# ─── SETTINGS ──────────────────────────────────────────────────

@bot.on_message(filters.command("settings") & filters.private)
@admin_only
async def cmd_settings(client: Client, message: Message):
    """
    /settings                          -> show current settings
    /settings delay 3 10               -> min/max delay seconds
    /settings rotation sequential|random|round_robin
    /settings tmpl_mode random|sequential
    /settings no_preview on|off
    """
    try:
        parts = message.text.split()
        if len(parts) == 1:
            min_d = await db.get("min_delay", 3)
            max_d = await db.get("max_delay", 8)
            rot   = await db.get("rotation", "sequential")
            tmpl  = await db.get("tmpl_mode", "random")
            prev  = await db.get("no_preview", False)
            job   = scheduler.get_job("broadcast_job")
            return await message.reply_text(
                f"**{sc('Settings')}**\n"
                f"{'─' * 28}\n\n"
                f"`{sc('delay')}       ` `{min_d}s – {max_d}s`\n"
                f"`{sc('rotation')}    ` `{sc(rot)}`\n"
                f"`{sc('tmpl mode')}   ` `{sc(tmpl)}`\n"
                f"`{sc('no preview')}  ` `{sc(str(prev).lower())}`\n"
                f"`{sc('scheduler')}   ` `{'ᴀᴄᴛɪᴠᴇ' if job else 'ᴏꜰꜰ'}`\n\n"
                f"_{sc('change via /settings <key> <value>')}_",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(sc("Rotation"), callback_data="set:rotation"),
                        InlineKeyboardButton(sc("Tmpl Mode"), callback_data="set:tmpl_mode"),
                    ],
                    [InlineKeyboardButton(sc("Back"), callback_data="m:home")]
                ])
            )

        key = parts[1].lower()
        if key == "delay":
            await db.set("min_delay", int(parts[2]))
            await db.set("max_delay", int(parts[3]))
            await message.reply_text(
                f"**{sc('Delay Updated')}**\n`{parts[2]}s – {parts[3]}s`",
                reply_markup=back_home()
            )
        elif key == "rotation":
            m = parts[2].lower()
            if m not in ("sequential","random","round_robin"):
                raise ValueError("invalid mode")
            await db.set("rotation", m)
            await message.reply_text(
                f"**{sc('Rotation Updated')}**\n`{sc(m)}`",
                reply_markup=back_home()
            )
        elif key == "tmpl_mode":
            await db.set("tmpl_mode", parts[2].lower())
            await message.reply_text(
                f"**{sc('Template Mode Updated')}**\n`{sc(parts[2])}`",
                reply_markup=back_home()
            )
        elif key == "no_preview":
            val = parts[2].lower() == "on"
            await db.set("no_preview", val)
            await message.reply_text(
                f"**{sc('No Preview')}**\n`{sc(str(val).lower())}`",
                reply_markup=back_home()
            )
        else:
            raise ValueError(f"unknown key: {key}")

    except Exception as e:
        await message.reply_text(f"`{sc('error')}` `{e}`")


# ─── STATS & RUN ───────────────────────────────────────────────

@bot.on_message(filters.command("stats") & filters.private)
@admin_only
async def cmd_stats(client: Client, message: Message):
    total_acc   = await db.acc.count_documents({})
    active_acc  = await db.acc.count_documents({"active": True, "banned": False})
    banned_acc  = await db.acc.count_documents({"banned": True})
    tmpl_count  = await db.tmpl.count_documents({"active": True})
    targets     = await db.get("targets", [])
    blacklisted = await db.bl.count_documents({})
    today       = datetime.utcnow().strftime("%Y-%m-%d")
    today_s     = await db.stats.find_one({"date": today}) or {}
    pipeline    = [{"$group": {"_id": None,
                               "ts": {"$sum": "$sent"},
                               "tf": {"$sum": "$failed"}}}]
    all_time    = await db.stats.aggregate(pipeline).to_list(1)
    atl_sent    = all_time[0]["ts"] if all_time else 0
    atl_fail    = all_time[0]["tf"] if all_time else 0
    job         = scheduler.get_job("broadcast_job")
    next_run    = f"`{job.next_run_time:%H:%M UTC}`" if job else f"`{sc('not scheduled')}`"

    await message.reply_text(
        f"**{sc('System Statistics')}**\n"
        f"{'─' * 28}\n\n"
        f"`{sc('accounts')}   ` `{active_acc}/{total_acc}` · `{banned_acc}` {sc('banned')}\n"
        f"`{sc('templates')}  ` `{tmpl_count}`\n"
        f"`{sc('targets')}    ` `{len(targets)}` · `{blacklisted}` {sc('blacklisted')}\n\n"
        f"**{sc('Today')}** `{today}`\n"
        f"`{sc('sent')}    ` `{today_s.get('sent',0)}`\n"
        f"`{sc('failed')}  ` `{today_s.get('failed',0)}`\n\n"
        f"**{sc('All Time')}**\n"
        f"`{sc('sent')}    ` `{atl_sent}`\n"
        f"`{sc('failed')}  ` `{atl_fail}`\n\n"
        f"`{sc('engine')}   ` `{'ʀᴜɴɴɪɴɢ' if engine.running else 'ɪᴅʟᴇ'}`\n"
        f"`{sc('next run')} ` {next_run}",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(sc("Run Now"), callback_data="runnow"),
                InlineKeyboardButton(sc("Health Check"), callback_data="healthcheck"),
            ],
            [InlineKeyboardButton(sc("Back"), callback_data="m:home")]
        ])
    )


@bot.on_message(filters.command("run_now") & filters.private)
@admin_only
async def cmd_run_now(client: Client, message: Message):
    if engine.running:
        return await message.reply_text(
            f"**{sc('Already Running')}**\n`{sc('broadcast is in progress')}`"
        )
    asyncio.create_task(engine.broadcast())
    await message.reply_text(
        f"**{sc('Broadcast Started')}**\n`{sc('use /stats to monitor progress')}`",
        reply_markup=back_home()
    )


# ════════════════════════════════════════════════════════════════
#  CALLBACK QUERY HANDLERS
# ════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex(r"^m:"))
@cb_admin
async def cb_menu(client: Client, cb: CallbackQuery):
    page = cb.data.split(":")[1]

    if page == "home":
        acc_n  = await db.acc.count_documents({"active": True, "banned": False})
        tmpl_n = await db.tmpl.count_documents({"active": True})
        tgt_n  = len(await db.get("targets", []))
        job    = scheduler.get_job("broadcast_job")
        sched  = f"`{job.next_run_time:%H:%M UTC}`" if job else f"`{sc('off')}`"
        return await cb.edit_message_text(
            f"**{sc('Multi-Account Automator v3.0')}**\n"
            f"{'─' * 28}\n\n"
            f"`{sc('accounts')}  ` `{acc_n}`\n"
            f"`{sc('templates')} ` `{tmpl_n}`\n"
            f"`{sc('targets')}   ` `{tgt_n}`\n"
            f"`{sc('scheduler')} ` {sched}\n"
            f"`{sc('engine')}    ` `{'ʀᴜɴɴɪɴɢ' if engine.running else 'ɪᴅʟᴇ'}`",
            reply_markup=main_menu()
        )

    elif page == "accounts":
        accounts = await am.active_list()
        banned   = await db.acc.count_documents({"banned": True})
        total    = await db.acc.count_documents({})
        preview  = ""
        for a in accounts[:5]:
            s = a.get("stats", {})
            preview += f"  `{a['label']}` · {s.get('sent',0)}/{s.get('failed',0)}\n"
        await cb.edit_message_text(
            f"**{sc('Accounts')}**\n"
            f"{'─' * 22}\n\n"
            f"`{sc('active')} ` `{len(accounts)}`\n"
            f"`{sc('banned')} ` `{banned}`\n"
            f"`{sc('total')}  ` `{total}`\n\n"
            f"**{sc('Top Active')}**\n{preview or sc('none')}\n\n"
            f"`/add_account` · `/list_accounts`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(sc("Health Check"), callback_data="healthcheck")],
                [
                    InlineKeyboardButton(sc("Export CSV"), callback_data="export:accounts"),
                    InlineKeyboardButton(sc("Back"), callback_data="m:home"),
                ],
            ])
        )

    elif page == "templates":
        total    = await db.tmpl.count_documents({"active": True})
        by_type  = {}
        async for t in db.tmpl.find({"active": True}):
            k = t.get("type","text")
            by_type[k] = by_type.get(k,0) + 1
        breakdown = "  ·  ".join([f"`{sc(k)}:{v}`" for k,v in by_type.items()])
        await cb.edit_message_text(
            f"**{sc('Templates')}** `[{total}]`\n"
            f"{'─' * 22}\n\n"
            f"{breakdown or sc('none')}\n\n"
            f"`/add_template <text>`\n"
            f"`/list_templates`\n"
            f"`/del_template <n>`\n\n"
            f"_{sc('media: reply to photo/video then /add_template')}_",
            reply_markup=back_btn()
        )

    elif page == "targets":
        targets    = await db.get("targets", [])
        blacklisted = await db.bl.count_documents({})
        preview    = "\n".join([f"  `{t}`" for t in targets[:5]])
        extra      = f"\n  `+{len(targets)-5} {sc('more')}`" if len(targets) > 5 else ""
        await cb.edit_message_text(
            f"**{sc('Targets')}** `[{len(targets)}]`\n"
            f"{'─' * 22}\n\n"
            f"{preview or sc('none')}{extra}\n\n"
            f"`{sc('blacklisted')}` `{blacklisted}`\n\n"
            f"`/set_targets` · `/add_target` · `/clear_targets`\n"
            f"`/blacklist` · `/export_targets`",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(sc("Clear All"), callback_data="targets:clear"),
                    InlineKeyboardButton(sc("Export"), callback_data="export:targets"),
                ],
                [InlineKeyboardButton(sc("Back"), callback_data="m:home")],
            ])
        )

    elif page == "blacklist":
        items = await db.bl.find().to_list(None)
        text  = f"**{sc('Blacklist')}** `[{len(items)}]`\n{'─' * 22}\n\n"
        if items:
            text += "\n".join([f"  `{i['target']}`" for i in items[:15]])
            if len(items) > 15:
                text += f"\n  `+{len(items)-15} {sc('more')}`"
        else:
            text += f"`{sc('blacklist is empty')}`"
        await cb.edit_message_text(
            text + "\n\n`/blacklist add @g1` · `/blacklist remove @g1`",
            reply_markup=back_btn()
        )

    elif page == "scheduler":
        job = scheduler.get_job("broadcast_job")
        if job:
            info = (f"`{sc('status')}  ` `ᴀᴄᴛɪᴠᴇ`\n"
                    f"`{sc('next')}    ` `{job.next_run_time:%Y-%m-%d %H:%M UTC}`\n"
                    f"`{sc('trigger')} ` `{job.trigger}`")
        else:
            info = f"`{sc('status')}  ` `ᴏꜰꜰ`\n`{sc('no active job')}`"
        await cb.edit_message_text(
            f"**{sc('Scheduler')}**\n"
            f"{'─' * 22}\n\n"
            f"{info}\n\n"
            f"`/start_job interval 15`\n"
            f"`/start_job cron 0 9 * * *`\n"
            f"`/stop_job`",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(sc("Stop Job"), callback_data="job:stop"),
                    InlineKeyboardButton(sc("Run Now"), callback_data="runnow"),
                ],
                [InlineKeyboardButton(sc("Back"), callback_data="m:home")],
            ])
        )

    elif page == "stats":
        today   = datetime.utcnow().strftime("%Y-%m-%d")
        today_s = await db.stats.find_one({"date": today}) or {}
        pipe    = [{"$group": {"_id": None, "ts":{"$sum":"$sent"},"tf":{"$sum":"$failed"}}}]
        all_t   = await db.stats.aggregate(pipe).to_list(1)
        await cb.edit_message_text(
            f"**{sc('Statistics')}**\n"
            f"{'─' * 22}\n\n"
            f"**{sc('Today')}**\n"
            f"`{sc('sent')}  ` `{today_s.get('sent',0)}`\n"
            f"`{sc('fail')}  ` `{today_s.get('failed',0)}`\n\n"
            f"**{sc('All Time')}**\n"
            f"`{sc('sent')}  ` `{all_t[0]['ts'] if all_t else 0}`\n"
            f"`{sc('fail')}  ` `{all_t[0]['tf'] if all_t else 0}`\n\n"
            f"`{sc('engine')}  ` `{'ʀᴜɴɴɪɴɢ' if engine.running else 'ɪᴅʟᴇ'}`\n"
            f"`{sc('live sent')}` `{engine.live_stats['sent']}`\n"
            f"`{sc('live fail')}` `{engine.live_stats['failed']}`",
            reply_markup=back_btn()
        )

    elif page == "settings":
        min_d = await db.get("min_delay", 3)
        max_d = await db.get("max_delay", 8)
        rot   = await db.get("rotation", "sequential")
        tmpl  = await db.get("tmpl_mode", "random")
        prev  = await db.get("no_preview", False)
        await cb.edit_message_text(
            f"**{sc('Settings')}**\n"
            f"{'─' * 22}\n\n"
            f"`{sc('delay')}      ` `{min_d}s – {max_d}s`\n"
            f"`{sc('rotation')}   ` `{sc(rot)}`\n"
            f"`{sc('tmpl mode')}  ` `{sc(tmpl)}`\n"
            f"`{sc('no preview')} ` `{sc(str(prev).lower())}`\n\n"
            f"_{sc('change via /settings command')}_",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(sc("Rotation Mode"), callback_data="set:rotation"),
                    InlineKeyboardButton(sc("Tmpl Mode"), callback_data="set:tmpl_mode"),
                ],
                [InlineKeyboardButton(sc("Back"), callback_data="m:home")],
            ])
        )

    elif page == "export":
        await cb.edit_message_text(
            f"**{sc('Export Data')}**\n"
            f"{'─' * 22}\n\n"
            f"`/export_targets`   — {sc('targets .txt file')}\n"
            f"`/export_accounts`  — {sc('accounts .csv file')}",
            reply_markup=back_btn()
        )


# ─── SETTINGS INLINE CALLBACKS ─────────────────────────────────

@bot.on_callback_query(filters.regex(r"^set:"))
@cb_admin
async def cb_settings(client: Client, cb: CallbackQuery):
    key = cb.data.split(":")[1]

    if key == "rotation":
        await cb.edit_message_text(
            f"**{sc('Select Rotation Mode')}**\n`{sc('choose how accounts are cycled')}`",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(sc("Sequential"), callback_data="setval:rotation:sequential"),
                    InlineKeyboardButton(sc("Random"), callback_data="setval:rotation:random"),
                    InlineKeyboardButton(sc("Round Robin"), callback_data="setval:rotation:round_robin"),
                ],
                [InlineKeyboardButton(sc("Back"), callback_data="m:settings")],
            ])
        )
    elif key == "tmpl_mode":
        await cb.edit_message_text(
            f"**{sc('Select Template Mode')}**",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(sc("Random"), callback_data="setval:tmpl_mode:random"),
                    InlineKeyboardButton(sc("Sequential"), callback_data="setval:tmpl_mode:sequential"),
                ],
                [InlineKeyboardButton(sc("Back"), callback_data="m:settings")],
            ])
        )


@bot.on_callback_query(filters.regex(r"^setval:"))
@cb_admin
async def cb_setval(client: Client, cb: CallbackQuery):
    _, key, val = cb.data.split(":")
    await db.set(key, val)
    await cb.answer(f"{sc(key)} -> {sc(val)}", show_alert=False)
    await cb.edit_message_text(
        f"**{sc('Updated')}**\n\n`{sc(key)}` set to `{sc(val)}`",
        reply_markup=back_btn("m:settings")
    )


# ─── TARGETS QUICK ACTIONS ─────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^targets:"))
@cb_admin
async def cb_targets_action(client: Client, cb: CallbackQuery):
    action = cb.data.split(":")[1]
    if action == "clear":
        await db.set("targets", [])
        await cb.answer(sc("targets cleared"), show_alert=True)
        await cb.edit_message_text(
            f"**{sc('Targets Cleared')}**\n`{sc('target list is now empty')}`",
            reply_markup=back_home()
        )


# ─── JOB QUICK ACTIONS ─────────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^job:"))
@cb_admin
async def cb_job_action(client: Client, cb: CallbackQuery):
    action = cb.data.split(":")[1]
    if action == "stop":
        if scheduler.get_job("broadcast_job"):
            scheduler.remove_job("broadcast_job")
            await cb.answer(sc("job stopped"), show_alert=True)
        else:
            await cb.answer(sc("no active job"), show_alert=True)
        await cb.edit_message_text(
            f"**{sc('Scheduler')}**\n`{sc('status')}` `ᴏꜰꜰ`",
            reply_markup=back_home()
        )


# ─── HEALTH CHECK ──────────────────────────────────────────────

@bot.on_callback_query(filters.regex("^healthcheck$"))
@cb_admin
async def cb_health(client: Client, cb: CallbackQuery):
    await cb.answer(sc("checking..."), show_alert=False)
    await cb.edit_message_text(
        f"`{sc('running health check, please wait...')}`"
    )
    res  = await am.health_check()
    text = (
        f"**{sc('Health Check')}**\n"
        f"{'─' * 22}\n\n"
        f"`{sc('alive')}` `{res['alive']}`  ·  `{sc('dead')}` `{res['dead']}`\n\n"
        f"**{sc('Details')}**\n"
    )
    for d in res["details"][:12]:
        text += f"  `{d['label']}` {d['status']} — _{d['note']}_\n"
    if len(res["details"]) > 12:
        text += f"\n`+{len(res['details'])-12} {sc('more')}`"
    await cb.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(sc("Re-check"), callback_data="healthcheck"),
                InlineKeyboardButton(sc("Back"), callback_data="m:accounts"),
            ]
        ])
    )


# ─── RUN NOW ───────────────────────────────────────────────────

@bot.on_callback_query(filters.regex("^runnow$"))
@cb_admin
async def cb_run_now(client: Client, cb: CallbackQuery):
    if engine.running:
        return await cb.answer(sc("already running!"), show_alert=True)
    await cb.answer(sc("starting..."), show_alert=False)
    asyncio.create_task(engine.broadcast())
    await cb.edit_message_text(
        f"**{sc('Broadcast Started')}**\n\n"
        f"`{sc('use /stats to monitor live progress')}`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(sc("Back to Menu"), callback_data="m:home")
        ]])
    )


# ─── EXPORT CALLBACKS ──────────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^export:"))
@cb_admin
async def cb_export(client: Client, cb: CallbackQuery):
    kind = cb.data.split(":")[1]
    await cb.answer(sc("preparing export..."), show_alert=False)

    if kind == "targets":
        targets = await db.get("targets", [])
        if not targets:
            return await cb.answer(sc("no targets found"), show_alert=True)
        buf      = io.BytesIO("\n".join([str(t) for t in targets]).encode())
        buf.name = f"targets_{datetime.utcnow():%Y%m%d_%H%M%S}.txt"
        await cb.message.reply_document(
            buf, caption=f"`{sc('exported')}` `{len(targets)}` {sc('targets')}"
        )

    elif kind == "accounts":
        accounts = await am.active_list()
        if not accounts:
            return await cb.answer(sc("no accounts found"), show_alert=True)
        rows = [["Label","Username","Phone","Sent","Failed","Active"]]
        for a in accounts:
            s = a.get("stats", {})
            rows.append([a.get("label","N/A"), a.get("username","N/A"),
                         a.get("phone","N/A"), s.get("sent",0),
                         s.get("failed",0), a.get("active",True)])
        out = io.StringIO()
        csv.writer(out).writerows(rows)
        buf      = io.BytesIO(out.getvalue().encode())
        buf.name = f"accounts_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        await cb.message.reply_document(
            buf, caption=f"`{sc('exported')}` `{len(accounts)}` {sc('accounts')}"
        )


# ════════════════════════════════════════════════════════════════
#  MAIN RUNNER
# ════════════════════════════════════════════════════════════════
async def main():
    # Test MongoDB connection
    try:
        await db.cfg.find_one({})
        log.info("Connected to MongoDB")
    except Exception as e:
        log.critical(f"Could not connect to MongoDB: {e}")
        return

    scheduler.start()
    await bot.start()

    log.info("Advanced Multi-Account Automator v3.0 — Online")

    try:
        acc_n = await db.acc.count_documents({"active": True, "banned": False})
        await bot.send_message(
            OWNER_ID,
            f"**{sc('Bot Online')}**\n"
            f"{'─' * 22}\n\n"
            f"`{sc('time')}     ` `{datetime.utcnow():%Y-%m-%d %H:%M UTC}`\n"
            f"`{sc('accounts')} ` `{acc_n}` {sc('active')}",
            reply_markup=main_menu()
        )
    except Exception:
        pass

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
