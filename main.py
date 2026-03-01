import os
import json
import hmac
import hashlib
import base64
import re
import asyncio
import time
import logging
from collections import defaultdict
from urllib.parse import parse_qs, unquote
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════
#  Config & validation
# ══════════════════════════════════════
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
WEBAPP_URL   = os.getenv("WEBAPP_URL", "")
ADMIN_ID     = int(os.getenv("ADMIN_ID", "0"))
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

def validate_env():
    required = ["BOT_TOKEN", "WEBAPP_URL", "ADMIN_ID", "SUPABASE_URL", "SUPABASE_KEY"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

validate_env()

# ══════════════════════════════════════
#  Logging
# ══════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")

# ══════════════════════════════════════
#  App & middleware
# ══════════════════════════════════════
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ErrorMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            return await call_next(request)
        except Exception as e:
            log.error(f"Unhandled error: {e}", exc_info=True)
            return JSONResponse({"error": "Internal server error"}, 500)


app.add_middleware(ErrorMiddleware)

# ══════════════════════════════════════
#  Rate limiting
# ══════════════════════════════════════
rate_limits = defaultdict(list)
RATE_LIMIT = 30
RATE_WINDOW = 60


def check_rate_limit(user_id: int) -> bool:
    now = time.time()
    rate_limits[user_id] = [t for t in rate_limits[user_id] if now - t < RATE_WINDOW]
    if len(rate_limits[user_id]) >= RATE_LIMIT:
        log.warning(f"Rate limited: {user_id}")
        return False
    rate_limits[user_id].append(now)
    return True


# Cleanup old entries every 5 min
async def rate_limit_cleanup():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        expired = [uid for uid, ts in rate_limits.items() if not ts or now - ts[-1] > RATE_WINDOW]
        for uid in expired:
            del rate_limits[uid]


# ══════════════════════════════════════
#  Cache
# ══════════════════════════════════════
_cache = {}
CACHE_TTL = 60


async def get_cached(key, fetcher):
    if key in _cache and time.time() - _cache[key]["ts"] < CACHE_TTL:
        return _cache[key]["data"]
    data = await fetcher()
    _cache[key] = {"data": data, "ts": time.time()}
    return data


def invalidate_cache():
    _cache.clear()
    log.info("Cache invalidated")


# ══════════════════════════════════════
#  Supabase REST — async + pooled
# ══════════════════════════════════════
class SupabaseREST:
    def __init__(self, url, key):
        self.base = f"{url}/rest/v1"
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        self.client: httpx.AsyncClient | None = None

    async def start(self):
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=15,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        log.info("DB client started")

    async def stop(self):
        if self.client:
            await self.client.aclose()
            log.info("DB client stopped")

    async def _req(self, method, table, params=None, data=None, headers_extra=None):
        url = f"{self.base}/{table}"
        h = {**self.headers, **(headers_extra or {})}
        try:
            r = await self.client.request(method, url, params=params, json=data, headers=h)
            if r.status_code >= 400:
                log.error(f"DB {method} {table}: {r.status_code} {r.text}")
                return []
            try:
                return r.json()
            except Exception:
                return []
        except Exception as e:
            log.error(f"DB request error: {e}")
            return []

    async def select(self, table, filters=None, order=None, limit=None):
        p = {"select": "*"}
        if filters:
            p.update(filters)
        if order:
            p["order"] = order
        if limit:
            p["limit"] = str(limit)
        return await self._req("GET", table, params=p)

    async def insert(self, table, data):
        return await self._req("POST", table, data=data)

    async def update(self, table, data, filters):
        return await self._req("PATCH", table, params=filters, data=data)

    async def select_eq(self, table, col, val):
        return await self.select(table, {col: f"eq.{val}"})

    async def update_eq(self, table, data, col, val):
        return await self.update(table, data, {col: f"eq.{val}"})

    async def count(self, table, filters=None):
        p = {"select": "*"}
        if filters:
            p.update(filters)
        h = {**self.headers, "Prefer": "count=exact", "Range": "0-0"}
        try:
            r = await self.client.head(f"{self.base}/{table}", params=p, headers=h)
            cr = r.headers.get("content-range", "")
            if "/" in cr:
                return int(cr.split("/")[1])
            return 0
        except Exception as e:
            log.error(f"DB count error: {e}")
            return 0


db = SupabaseREST(SUPABASE_URL, SUPABASE_KEY)


# ══════════════════════════════════════
#  Telegram client — pooled
# ══════════════════════════════════════
tg_client: httpx.AsyncClient | None = None


async def get_tg_client():
    global tg_client
    if tg_client is None:
        tg_client = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{BOT_TOKEN}/",
            timeout=15,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
    return tg_client


def parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


async def tg(method, data=None):
    try:
        c = await get_tg_client()
        r = await c.post(method, json=data or {})
        return r.json()
    except Exception as e:
        log.error(f"TG error [{method}]: {e}")
        return {"ok": False}


async def send_msg(chat_id, text, markup=None):
    d = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        d["reply_markup"] = markup
    return await tg("sendMessage", d)


async def edit_msg(chat_id, msg_id, text, markup=None):
    d = {"chat_id": chat_id, "message_id": msg_id,
         "text": text, "parse_mode": "HTML"}
    if markup:
        d["reply_markup"] = markup
    return await tg("editMessageText", d)


async def answer_cb(cb_id, text="", alert=False):
    return await tg("answerCallbackQuery",
                    {"callback_query_id": cb_id, "text": text, "show_alert": alert})


async def download_file_b64(file_id):
    try:
        r = await tg("getFile", {"file_id": file_id})
        if not r.get("ok"):
            return ""
        path = r["result"]["file_path"]
        url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}"
        c = await get_tg_client()
        resp = await c.get(url)
        b64 = base64.b64encode(resp.content).decode()
        mime = "image/png" if path.endswith(".png") else "image/jpeg"
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""


# ══════════════════════════════════════
#  Channel / Bot parsing
# ══════════════════════════════════════
async def parse_channel(channel_input):
    channel_input = channel_input.strip()
    if "t.me/" in channel_input:
        channel_input = "@" + channel_input.split("t.me/")[-1].split("/")[0].split("?")[0]
    if not channel_input.startswith("@") and not channel_input.lstrip("-").isdigit():
        channel_input = "@" + channel_input

    r = await tg("getChat", {"chat_id": channel_input})
    if not r.get("ok"):
        return None
    chat = r["result"]
    if chat.get("type") not in ("channel", "supergroup"):
        return None

    info = {
        "channel_id": chat["id"], "type": "channel",
        "title": chat.get("title", ""), "username": chat.get("username", ""),
        "invite_link": "", "avatar_base64": "", "member_count": 0,
    }
    if info["username"]:
        info["invite_link"] = f"https://t.me/{info['username']}"
    elif chat.get("invite_link"):
        info["invite_link"] = chat["invite_link"]
    else:
        r2 = await tg("exportChatInviteLink", {"chat_id": chat["id"]})
        if r2.get("ok"):
            info["invite_link"] = r2["result"]

    r3 = await tg("getChatMemberCount", {"chat_id": chat["id"]})
    if r3.get("ok"):
        info["member_count"] = r3["result"]

    if chat.get("photo"):
        fid = chat["photo"].get("big_file_id") or chat["photo"].get("small_file_id")
        if fid:
            info["avatar_base64"] = await download_file_b64(fid)
    return info


async def parse_bot(bot_input):
    bot_input = bot_input.strip()
    if "t.me/" in bot_input:
        bot_input = bot_input.split("t.me/")[-1].split("/")[0].split("?")[0]
    bot_input = bot_input.lstrip("@")
    if not bot_input:
        return None

    r = await tg("getChat", {"chat_id": f"@{bot_input}"})
    if r.get("ok"):
        chat = r["result"]
        avatar = ""
        if chat.get("photo"):
            fid = chat["photo"].get("big_file_id") or chat["photo"].get("small_file_id")
            if fid:
                avatar = await download_file_b64(fid)
        return {
            "channel_id": chat["id"], "type": "bot",
            "title": chat.get("first_name") or chat.get("title") or bot_input,
            "username": chat.get("username", bot_input),
            "invite_link": f"https://t.me/{chat.get('username', bot_input)}",
            "avatar_base64": avatar, "member_count": 0,
        }

    try:
        c = await get_tg_client()
        resp = await c.get(f"https://t.me/{bot_input}",
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
                           follow_redirects=True)
        html = resp.text
        m_title = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', html)
        title = m_title.group(1) if m_title else bot_input
        avatar = ""
        m_img = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
        if m_img:
            img_url = m_img.group(1)
            if img_url and "telegram-logo" not in img_url and "telegram_logo" not in img_url:
                try:
                    img_resp = await c.get(img_url, timeout=10)
                    if img_resp.status_code == 200 and len(img_resp.content) > 100:
                        b64 = base64.b64encode(img_resp.content).decode()
                        mime = "image/png" if img_url.endswith(".png") else "image/jpeg"
                        avatar = f"data:{mime};base64,{b64}"
                except Exception:
                    pass
        uid = int(hashlib.md5(bot_input.encode()).hexdigest()[:15], 16)
        return {
            "channel_id": uid, "type": "bot", "title": title,
            "username": bot_input, "invite_link": f"https://t.me/{bot_input}",
            "avatar_base64": avatar, "member_count": 0,
        }
    except Exception as e:
        log.error(f"parse_bot error: {e}")
        uid = int(hashlib.md5(bot_input.encode()).hexdigest()[:15], 16)
        return {
            "channel_id": uid, "type": "bot", "title": bot_input,
            "username": bot_input, "invite_link": f"https://t.me/{bot_input}",
            "avatar_base64": "", "member_count": 0,
        }


async def check_member(channel_id, user_id):
    r = await tg("getChatMember", {"chat_id": channel_id, "user_id": user_id})
    if r.get("ok"):
        return r["result"]["status"] in ("member", "administrator", "creator")
    return False


# ══════════════════════════════════════
#  Auth
# ══════════════════════════════════════
def validate_init(raw):
    try:
        parsed = parse_qs(raw)
        h = parsed.get("hash", [None])[0]
        if not h:
            return None
        pairs = sorted(f"{k}={unquote(v[0])}" for k, v in parsed.items() if k != "hash")
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        check = hmac.new(secret, "\n".join(pairs).encode(), hashlib.sha256).hexdigest()
        if check != h:
            return None
        user_raw = parsed.get("user", [None])[0]
        return {"user": json.loads(unquote(user_raw))} if user_raw else None
    except Exception:
        return None


# ══════════════════════════════════════
#  DB helpers
# ══════════════════════════════════════
async def get_or_create(tg_id, info=None):
    rows = await db.select_eq("users", "telegram_id", tg_id)
    if rows:
        return rows[0]
    u = {
        "telegram_id": tg_id,
        "username": (info or {}).get("username", ""),
        "first_name": (info or {}).get("first_name", ""),
        "last_name": (info or {}).get("last_name", ""),
        "state": "new",
    }
    result = await db.insert("users", u)
    log.info(f"New user: {tg_id} @{u['username']}")
    return result[0] if result else u


async def get_sponsors():
    return await db.select("channels", {"is_active": "eq.true"}, order="added_at.asc")


async def get_sponsors_cached():
    return await get_cached("sponsors", get_sponsors)


async def get_prizes():
    return await db.select("prizes", {"is_active": "eq.true"}, order="sort_order.asc")


async def get_prizes_cached():
    return await get_cached("prizes", get_prizes)


async def count_referrals(tg_id):
    return await db.count("users", {"referred_by": f"eq.{tg_id}", "state": "eq.claimed"})


# ══════════════════════════════════════
#  Event logging
# ══════════════════════════════════════
async def log_event(telegram_id, event, data=None):
    try:
        await db.insert("events", {
            "telegram_id": telegram_id,
            "event": event,
            "data": json.dumps(data or {}),
        })
    except Exception as e:
        log.error(f"Event log error: {e}")


# ══════════════════════════════════════
#  API endpoints
# ══════════════════════════════════════
@app.get("/api/health")
async def health():
    r = await tg("getMe")
    db_ok = bool(await db.select("users", limit=1))
    return {
        "bot": r.get("ok", False),
        "bot_username": r.get("result", {}).get("username", ""),
        "db": db_ok,
        "broadcast_running": broadcast_status["running"],
        "cache_keys": list(_cache.keys()),
    }


@app.post("/api/get-user")
async def api_get_user(req: Request):
    body = await req.json()
    v = validate_init(body.get("initData", ""))
    if not v:
        return JSONResponse({"error": "Invalid initData"}, 401)

    tg_id = v["user"]["id"]
    if not check_rate_limit(tg_id):
        return JSONResponse({"error": "Too many requests"}, 429)

    user = await get_or_create(tg_id, v["user"])
    sponsors = await get_sponsors_cached()
    prizes = await get_prizes_cached()
    ref_count = await count_referrals(user["telegram_id"])

    sponsors_sorted = sorted(sponsors, key=lambda x: (0 if x.get("type", "channel") == "channel" else 1))

    claimed_at = user.get("claimed_at")
    claimed_at_unix = None
    if claimed_at:
        dt = parse_dt(claimed_at)
        if dt:
            claimed_at_unix = int(dt.timestamp())

    return {
        "ok": True,
        "user": {
            "telegram_id": user["telegram_id"],
            "first_name": user.get("first_name", ""),
            "state": user["state"],
            "prize_key": user.get("prize_key"),
            "prize_name": user.get("prize_name"),
            "claimed_at": claimed_at_unix,
            "referral_count": ref_count,
            "is_admin": user["telegram_id"] == ADMIN_ID,
            "forced_prize": user.get("forced_prize"),
        },
        "channels": [
            {
                "id": str(c["channel_id"]),
                "name": c["title"],
                "type": c.get("type", "channel"),
                "link": c["invite_link"] if c["invite_link"].startswith("http")
                        else f"https://t.me/{c['username']}" if c.get("username")
                        else c["invite_link"],
                "avatar": c.get("avatar_base64", ""),
            }
            for c in sponsors_sorted
        ],
        "prizes": [
            {"key": p["key"], "tgs": p["tgs_file"],
             "name": p["name"], "emoji": p["emoji"]}
            for p in prizes
        ],
    }


@app.post("/api/check-subscription")
async def api_check_sub(req: Request):
    body = await req.json()
    v = validate_init(body.get("initData", ""))
    if not v:
        return JSONResponse({"error": "Invalid initData"}, 401)

    tg_id = v["user"]["id"]
    if not check_rate_limit(tg_id):
        return JSONResponse({"error": "Too many requests"}, 429)

    user = await get_or_create(tg_id, v["user"])
    action = body.get("action", "check")

    if action == "save_roll":
        if user["state"] != "new":
            return JSONResponse({"error": "Already rolled"}, 400)
        prize_key = body.get("prize_key", "")
        prize_name = body.get("prize_name", "")
        await db.update_eq("users", {
            "state": "rolled",
            "prize_key": prize_key,
            "prize_name": prize_name,
        }, "telegram_id", tg_id)
        log.info(f"User {tg_id} rolled: {prize_key}")
        await log_event(tg_id, "roll", {"prize_key": prize_key, "prize_name": prize_name})
        return {"ok": True, "state": "rolled"}

    if action == "mark_bot_opened":
        bot_id = body.get("bot_id")
        if bot_id:
            bot_id_str = str(bot_id)
            fresh = await db.select_eq("users", "telegram_id", tg_id)
            opened = json.loads(fresh[0].get("opened_bots") or "[]") if fresh else []
            if bot_id_str not in opened:
                opened.append(bot_id_str)
                await db.update_eq("users", {"opened_bots": json.dumps(opened)},
                                   "telegram_id", tg_id)
        return {"ok": True}

    if action == "check":
        sponsors = await get_sponsors_cached()
        fresh = await db.select_eq("users", "telegram_id", tg_id)
        fresh_user = fresh[0] if fresh else user
        opened_bots = json.loads(fresh_user.get("opened_bots") or "[]")

        results = {}
        all_ok = True
        channel_checks = []

        for sp in sponsors:
            sp_type = sp.get("type", "channel")
            sp_id = str(sp["channel_id"])
            if sp_type == "bot":
                ok = sp_id in opened_bots
                results[sp_id] = ok
                if not ok:
                    all_ok = False
            else:
                channel_checks.append((sp_id, sp["channel_id"]))

        if channel_checks:
            tasks = [check_member(ch_id, tg_id) for _, ch_id in channel_checks]
            check_results = await asyncio.gather(*tasks, return_exceptions=True)
            for (sp_id, _), result in zip(channel_checks, check_results):
                ok = result is True
                results[sp_id] = ok
                if not ok:
                    all_ok = False

        new_state = fresh_user["state"]
        if all_ok and fresh_user["state"] == "rolled":
            now_iso = datetime.now(timezone.utc).isoformat()
            await db.update_eq("users", {
                "state": "claimed",
                "claimed_at": now_iso,
            }, "telegram_id", tg_id)
            new_state = "claimed"
            log.info(f"User {tg_id} claimed prize: {fresh_user.get('prize_key')}")
            await log_event(tg_id, "claim", {"prize_key": fresh_user.get("prize_key")})

            referred_by = fresh_user.get("referred_by")
            if referred_by:
                asyncio.create_task(notify_referrer(int(referred_by)))

        ref_count = await count_referrals(tg_id)
        return {
            "ok": True, "all_subscribed": all_ok,
            "results": results, "state": new_state,
            "referral_count": ref_count,
        }

    return JSONResponse({"error": "Unknown action"}, 400)


async def notify_referrer(referrer_id):
    try:
        ref_count = await count_referrals(referrer_id)
        speed = 2 ** (ref_count // 2)
        await send_msg(referrer_id,
            f"🎉 <b>Ваш друг получил подарок!</b>\n\n"
            f"👥 Приглашено друзей: <b>{ref_count}</b>\n"
            f"🚀 Скорость вывода: <b>x{speed}</b>\n\n"
            f"Продолжайте приглашать для ускорения! 🔥",
            {"inline_keyboard": [[
                {"text": "🎁 Открыть", "web_app": {"url": WEBAPP_URL}}
            ]]}
        )
        log.info(f"Notified referrer {referrer_id}: {ref_count} refs, x{speed}")
    except Exception as e:
        log.error(f"notify_referrer error: {e}")


@app.post("/api/get-page")
async def api_get_page(req: Request):
    body = await req.json()
    key = body.get("key", "")
    rows = await db.select_eq("pages", "key", key)
    return {"ok": True, "content": rows[0]["content"] if rows else ""}


@app.post("/api/save-page")
async def api_save_page(req: Request):
    body = await req.json()
    v = validate_init(body.get("initData", ""))
    if not v or v["user"]["id"] != ADMIN_ID:
        return JSONResponse({"error": "Forbidden"}, 403)
    key = body.get("key", "")
    content = body.get("content", "")
    existing = await db.select_eq("pages", "key", key)
    if existing:
        await db.update_eq("pages", {"content": content}, "key", key)
    else:
        await db.insert("pages", {"key": key, "content": content})
    return {"ok": True}


# ══════════════════════════════════════
#  Webhook
# ══════════════════════════════════════
@app.post("/api/webhook")
async def webhook(req: Request):
    body = await req.json()
    if "message" in body:
        await handle_message(body["message"])
    elif "callback_query" in body:
        await handle_callback(body["callback_query"])
    return {"ok": True}


async def handle_message(msg):
    uid = msg["from"]["id"]
    cid = msg["chat"]["id"]
    text = msg.get("text", "").strip()

    if text.startswith("/start"):
        parts = text.split()
        ref_id = None
        if len(parts) > 1 and parts[1].startswith("ref_"):
            try:
                ref_id = int(parts[1][4:])
            except Exception:
                pass

        name = msg["from"].get("first_name", "Друг")
        user = await get_or_create(uid, msg["from"])

        if ref_id and ref_id != uid and not user.get("referred_by"):
            await db.update_eq("users", {"referred_by": ref_id}, "telegram_id", uid)
            log.info(f"User {uid} referred by {ref_id}")

        await log_event(uid, "start", {"ref": ref_id})

        await tg("sendMessage", {
    "chat_id": cid,
    "text": (
        f"✨ <b>Привет, {name}!</b>\n\n"
        f"🎁 У нас для тебя особенный подарок — "
        f"<b>500 Telegram Stars</b> или <b>Telegram Premium</b>!\n\n"
        f"⭐ Всё что нужно — просто нарисовать звезду!\n\n"
        f"Чем точнее нарисуешь — тем быстрее получишь приз. "
        f"Это <b>бесплатно</b> и займёт меньше минуты!\n\n"
        f"Жми на кнопку ниже 👇"
    ),
    "parse_mode": "HTML",
    "message_effect_id": "5104841245755180586",
    "reply_markup": {"inline_keyboard": [[
        {"text": "⭐ Нарисовать звезду!", "web_app": {"url": WEBAPP_URL}}
    ]]}
})

    elif text == "/a" and uid == ADMIN_ID:
        await show_admin_menu(cid)

    elif text.startswith("/reset ") and uid == ADMIN_ID:
        try:
            target_id = int(text.split()[1])
            await db.update_eq("users", {
                "state": "new",
                "prize_key": None,
                "prize_name": None,
                "claimed_at": None,
                "opened_bots": "[]",
                "notified_1h": False,
                "notified_unsub": False,
            }, "telegram_id", target_id)
            await send_msg(cid, f"✅ Юзер <code>{target_id}</code> сброшен")
            log.info(f"Admin reset user {target_id}")
        except Exception:
            await send_msg(cid, "❌ Использование: <code>/reset 123456789</code>")

    elif uid == ADMIN_ID:
        user = await get_or_create(ADMIN_ID)
        st = user.get("admin_state", "")
        if st == "add_channel":
            await process_add_channel(cid, text)
            await db.update_eq("users", {"admin_state": ""}, "telegram_id", ADMIN_ID)
        elif st == "add_bot":
            await process_add_bot(cid, text)
            await db.update_eq("users", {"admin_state": ""}, "telegram_id", ADMIN_ID)
        elif st and st.startswith("edit_prize:"):
            key = st.split(":")[1]
            await db.update_eq("prizes", {"name": text}, "key", key)
            await db.update_eq("users", {"admin_state": ""}, "telegram_id", ADMIN_ID)
            await send_msg(cid, f"✅ Приз переименован в: <b>{text}</b>")
            invalidate_cache()
        elif st == "broadcast_text":
            await db.update_eq("users", {"admin_state": ""}, "telegram_id", ADMIN_ID)
            await prepare_broadcast(cid, msg)
        elif st == "broadcast_confirm":
            await db.update_eq("users", {"admin_state": ""}, "telegram_id", ADMIN_ID)
            await send_msg(cid, "Отменено. /a")


async def show_admin_menu(cid, msg_id=None):
    sponsors = await get_sponsors_cached()
    prs = await get_prizes_cached()
    total = await db.count("users")
    ch_count = sum(1 for s in sponsors if s.get("type", "channel") == "channel")
    bot_count = sum(1 for s in sponsors if s.get("type") == "bot")

    text = (
        f"⚙️ <b>Панель администратора</b>\n\n"
        f"📢 Каналов: <b>{ch_count}</b>\n"
        f"🤖 Ботов: <b>{bot_count}</b>\n"
        f"🎁 Призов: <b>{len(prs)}</b>\n"
        f"👥 Пользователей: <b>{total}</b>"
    )
    kb = {"inline_keyboard": [
        [{"text": f"📢 Каналы ({ch_count})", "callback_data": "adm_channels"}],
        [{"text": f"🤖 Боты ({bot_count})", "callback_data": "adm_bots"}],
        [{"text": f"🎁 Призы ({len(prs)})", "callback_data": "adm_prizes"}],
        [{"text": "📨 Рассылка", "callback_data": "adm_broadcast"}],
        [{"text": "📊 Статистика", "callback_data": "adm_stats"}],
        [{"text": "🔄 Обновить данные", "callback_data": "adm_refresh"}],
    ]}
    if msg_id:
        await edit_msg(cid, msg_id, text, kb)
    else:
        await send_msg(cid, text, kb)


async def process_add_channel(cid, text):
    await send_msg(cid, "⏳ Проверяю канал...")
    info = await parse_channel(text)
    if not info:
        await send_msg(cid, "❌ Не удалось найти канал.\nОтправьте @username ещё раз:")
        await db.update_eq("users", {"admin_state": "add_channel"}, "telegram_id", ADMIN_ID)
        return
    bot_info = await tg("getMe")
    bot_id = bot_info["result"]["id"] if bot_info.get("ok") else 0
    bm = await tg("getChatMember", {"chat_id": info["channel_id"], "user_id": bot_id})
    if not bm.get("ok") or bm["result"]["status"] not in ("administrator", "creator"):
        await send_msg(cid, f"⚠️ Бот не админ в «{info['title']}».")
        return
    existing = await db.select_eq("channels", "channel_id", info["channel_id"])
    if existing:
        await db.update_eq("channels", {**info, "is_active": True}, "channel_id", info["channel_id"])
    else:
        await db.insert("channels", info)
    invalidate_cache()
    log.info(f"Channel added: {info['title']}")
    await send_msg(cid, f"✅ Канал <b>{info['title']}</b> добавлен!")


async def process_add_bot(cid, text):
    await send_msg(cid, "⏳ Проверяю бота...")
    info = await parse_bot(text)
    if not info:
        await send_msg(cid, "❌ Не удалось найти бота.\nОтправьте @username ещё раз:")
        await db.update_eq("users", {"admin_state": "add_bot"}, "telegram_id", ADMIN_ID)
        return
    existing = await db.select_eq("channels", "channel_id", info["channel_id"])
    if existing:
        await db.update_eq("channels", {**info, "is_active": True}, "channel_id", info["channel_id"])
    else:
        await db.insert("channels", info)
    invalidate_cache()
    log.info(f"Bot added: {info['title']}")
    await send_msg(cid, f"✅ Бот <b>{info['title']}</b> добавлен!")


# ══════════════════════════════════════
#  Broadcast
# ══════════════════════════════════════
broadcast_status = {"running": False, "total": 0, "sent": 0, "failed": 0, "blocked": 0}

CONTENT_TYPE_NAMES = {
    "text": "📝 Текст", "photo": "🖼 Фото", "video": "🎬 Видео",
    "animation": "🎞 GIF", "sticker": "🎭 Стикер", "document": "📎 Документ",
    "voice": "🎤 Голосовое", "video_note": "⚪ Видеокружок", "audio": "🎵 Аудио",
    "contact": "👤 Контакт", "location": "📍 Локация", "venue": "🏢 Место",
    "poll": "📊 Опрос", "dice": "🎲 Кость",
}

SEND_MODE_NAMES = {
    "reconstruct": "📝 Своим сообщением",
    "copy":        "📋 Копией (без «переслано»)",
    "forward":     "↗️ Пересылкой (с «переслано от»)",
}


def extract_broadcast_content(msg):
    content = {}

    if msg.get("photo"):
        content = {
            "type": "photo", "file_id": msg["photo"][-1]["file_id"],
            "caption": msg.get("caption", ""),
            "caption_entities": msg.get("caption_entities", []),
            "has_spoiler": msg.get("has_media_spoiler", False),
            "show_caption_above_media": msg.get("show_caption_above_media", False),
        }
    elif msg.get("video"):
        content = {
            "type": "video", "file_id": msg["video"]["file_id"],
            "caption": msg.get("caption", ""),
            "caption_entities": msg.get("caption_entities", []),
            "has_spoiler": msg.get("has_media_spoiler", False),
            "show_caption_above_media": msg.get("show_caption_above_media", False),
            "duration": msg["video"].get("duration"),
            "width": msg["video"].get("width"),
            "height": msg["video"].get("height"),
            "supports_streaming": msg["video"].get("supports_streaming", True),
        }
    elif msg.get("animation"):
        content = {
            "type": "animation", "file_id": msg["animation"]["file_id"],
            "caption": msg.get("caption", ""),
            "caption_entities": msg.get("caption_entities", []),
            "has_spoiler": msg.get("has_media_spoiler", False),
            "show_caption_above_media": msg.get("show_caption_above_media", False),
        }
    elif msg.get("sticker"):
        content = {
            "type": "sticker", "file_id": msg["sticker"]["file_id"],
            "emoji": msg["sticker"].get("emoji", ""),
        }
    elif msg.get("document"):
        content = {
            "type": "document", "file_id": msg["document"]["file_id"],
            "caption": msg.get("caption", ""),
            "caption_entities": msg.get("caption_entities", []),
            "file_name": msg["document"].get("file_name", ""),
        }
    elif msg.get("voice"):
        content = {
            "type": "voice", "file_id": msg["voice"]["file_id"],
            "caption": msg.get("caption", ""),
            "caption_entities": msg.get("caption_entities", []),
            "duration": msg["voice"].get("duration"),
        }
    elif msg.get("video_note"):
        content = {
            "type": "video_note", "file_id": msg["video_note"]["file_id"],
            "duration": msg["video_note"].get("duration"),
            "length": msg["video_note"].get("length"),
        }
    elif msg.get("audio"):
        content = {
            "type": "audio", "file_id": msg["audio"]["file_id"],
            "caption": msg.get("caption", ""),
            "caption_entities": msg.get("caption_entities", []),
            "duration": msg["audio"].get("duration"),
            "performer": msg["audio"].get("performer", ""),
            "title": msg["audio"].get("title", ""),
        }
    elif msg.get("contact"):
        c = msg["contact"]
        content = {
            "type": "contact", "phone_number": c["phone_number"],
            "first_name": c.get("first_name", ""),
            "last_name": c.get("last_name", ""),
            "vcard": c.get("vcard", ""),
        }
    elif msg.get("location"):
        loc = msg["location"]
        content = {
            "type": "location", "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "horizontal_accuracy": loc.get("horizontal_accuracy"),
        }
    elif msg.get("venue"):
        v = msg["venue"]
        content = {
            "type": "venue",
            "latitude": v["location"]["latitude"],
            "longitude": v["location"]["longitude"],
            "title": v["title"], "address": v["address"],
            "foursquare_id": v.get("foursquare_id", ""),
            "google_place_id": v.get("google_place_id", ""),
        }
    elif msg.get("poll"):
        poll = msg["poll"]
        content = {
            "type": "poll", "question": poll["question"],
            "question_entities": poll.get("question_entities", []),
            "options": [{"text": o["text"], "text_entities": o.get("text_entities", [])}
                        for o in poll["options"]],
            "is_anonymous": poll.get("is_anonymous", True),
            "poll_type": poll.get("type", "regular"),
            "allows_multiple_answers": poll.get("allows_multiple_answers", False),
            "correct_option_id": poll.get("correct_option_id"),
            "explanation": poll.get("explanation", ""),
            "explanation_entities": poll.get("explanation_entities", []),
        }
    elif msg.get("dice"):
        content = {"type": "dice", "emoji": msg["dice"].get("emoji", "🎲")}
    else:
        content = {
            "type": "text", "text": msg.get("text", ""),
            "entities": msg.get("entities", []),
        }

    if msg.get("reply_markup"):
        content["reply_markup"] = msg["reply_markup"]

    return content


def get_content_preview(content):
    t = content.get("type", "unknown")
    cap = content.get("caption", "") or content.get("text", "")
    short = cap[:150] + ("..." if len(cap) > 150 else "") if cap else ""

    previews = {
        "text":       f"📝 {short}",
        "photo":      f"🖼 Фото" + (f"\n{short}" if short else ""),
        "video":      f"🎬 Видео" + (f"\n{short}" if short else ""),
        "animation":  f"🎞 GIF" + (f"\n{short}" if short else ""),
        "sticker":    f"🎭 Стикер {content.get('emoji', '')}",
        "document":   f"📎 {content.get('file_name', 'файл')}" + (f"\n{short}" if short else ""),
        "voice":      f"🎤 Голосовое ({content.get('duration', '?')}с)",
        "video_note": f"⚪ Видеокружок ({content.get('duration', '?')}с)",
        "audio":      f"🎵 {content.get('performer', '')} — {content.get('title', '')}",
        "contact":    f"👤 {content.get('first_name', '')} ({content.get('phone_number', '')})",
        "location":   f"📍 {content.get('latitude', '')}, {content.get('longitude', '')}",
        "venue":      f"🏢 {content.get('title', '')}",
        "poll":       f"📊 {content.get('question', '')[:100]}",
        "dice":       f"🎲 {content.get('emoji', '🎲')}",
    }
    return previews.get(t, f"❓ {t}")


async def send_reconstruct(chat_id, content):
    t = content["type"]
    payload = {"chat_id": chat_id}
    if content.get("reply_markup"):
        payload["reply_markup"] = content["reply_markup"]

    if t == "text":
        payload["text"] = content["text"]
        payload["entities"] = content.get("entities", [])
        r = await tg("sendMessage", payload)
    elif t == "photo":
        payload["photo"] = content["file_id"]
        payload["caption"] = content.get("caption", "")
        payload["caption_entities"] = content.get("caption_entities", [])
        if content.get("has_spoiler"): payload["has_spoiler"] = True
        if content.get("show_caption_above_media"): payload["show_caption_above_media"] = True
        r = await tg("sendPhoto", payload)
    elif t == "video":
        payload["video"] = content["file_id"]
        payload["caption"] = content.get("caption", "")
        payload["caption_entities"] = content.get("caption_entities", [])
        if content.get("has_spoiler"): payload["has_spoiler"] = True
        if content.get("show_caption_above_media"): payload["show_caption_above_media"] = True
        if content.get("duration"): payload["duration"] = content["duration"]
        if content.get("width"): payload["width"] = content["width"]
        if content.get("height"): payload["height"] = content["height"]
        if content.get("supports_streaming"): payload["supports_streaming"] = True
        r = await tg("sendVideo", payload)
    elif t == "animation":
        payload["animation"] = content["file_id"]
        payload["caption"] = content.get("caption", "")
        payload["caption_entities"] = content.get("caption_entities", [])
        if content.get("has_spoiler"): payload["has_spoiler"] = True
        if content.get("show_caption_above_media"): payload["show_caption_above_media"] = True
        r = await tg("sendAnimation", payload)
    elif t == "sticker":
        payload["sticker"] = content["file_id"]
        r = await tg("sendSticker", payload)
    elif t == "document":
        payload["document"] = content["file_id"]
        payload["caption"] = content.get("caption", "")
        payload["caption_entities"] = content.get("caption_entities", [])
        r = await tg("sendDocument", payload)
    elif t == "voice":
        payload["voice"] = content["file_id"]
        payload["caption"] = content.get("caption", "")
        payload["caption_entities"] = content.get("caption_entities", [])
        if content.get("duration"): payload["duration"] = content["duration"]
        r = await tg("sendVoice", payload)
    elif t == "video_note":
        payload["video_note"] = content["file_id"]
        if content.get("duration"): payload["duration"] = content["duration"]
        if content.get("length"): payload["length"] = content["length"]
        r = await tg("sendVideoNote", payload)
    elif t == "audio":
        payload["audio"] = content["file_id"]
        payload["caption"] = content.get("caption", "")
        payload["caption_entities"] = content.get("caption_entities", [])
        if content.get("duration"): payload["duration"] = content["duration"]
        if content.get("performer"): payload["performer"] = content["performer"]
        if content.get("title"): payload["title"] = content["title"]
        r = await tg("sendAudio", payload)
    elif t == "contact":
        payload["phone_number"] = content["phone_number"]
        payload["first_name"] = content.get("first_name", "")
        if content.get("last_name"): payload["last_name"] = content["last_name"]
        if content.get("vcard"): payload["vcard"] = content["vcard"]
        r = await tg("sendContact", payload)
    elif t == "location":
        payload["latitude"] = content["latitude"]
        payload["longitude"] = content["longitude"]
        if content.get("horizontal_accuracy"):
            payload["horizontal_accuracy"] = content["horizontal_accuracy"]
        r = await tg("sendLocation", payload)
    elif t == "venue":
        payload["latitude"] = content["latitude"]
        payload["longitude"] = content["longitude"]
        payload["title"] = content["title"]
        payload["address"] = content["address"]
        if content.get("foursquare_id"): payload["foursquare_id"] = content["foursquare_id"]
        if content.get("google_place_id"): payload["google_place_id"] = content["google_place_id"]
        r = await tg("sendVenue", payload)
    elif t == "poll":
        payload["question"] = content["question"]
        if content.get("question_entities"):
            payload["question_entities"] = content["question_entities"]
        payload["options"] = content["options"]
        payload["is_anonymous"] = content.get("is_anonymous", True)
        payload["type"] = content.get("poll_type", "regular")
        payload["allows_multiple_answers"] = content.get("allows_multiple_answers", False)
        if content.get("correct_option_id") is not None:
            payload["correct_option_id"] = content["correct_option_id"]
        if content.get("explanation"):
            payload["explanation"] = content["explanation"]
        if content.get("explanation_entities"):
            payload["explanation_entities"] = content["explanation_entities"]
        payload.pop("reply_markup", None)
        r = await tg("sendPoll", payload)
    elif t == "dice":
        payload["emoji"] = content.get("emoji", "🎲")
        r = await tg("sendDice", payload)
    else:
        return False

    return r.get("ok", False)


async def send_broadcast_msg(chat_id, content, mode="reconstruct"):
    try:
        if mode == "forward":
            r = await tg("forwardMessage", {
                "chat_id": chat_id,
                "from_chat_id": content["_original_chat_id"],
                "message_id": content["_original_message_id"],
            })
        elif mode == "copy":
            r = await tg("copyMessage", {
                "chat_id": chat_id,
                "from_chat_id": content["_original_chat_id"],
                "message_id": content["_original_message_id"],
            })
        else:
            return await send_reconstruct(chat_id, content)

        if not r.get("ok"):
            desc = r.get("description", "")
            if any(x in desc for x in [
                "blocked", "deactivated", "not found",
                "PEER_ID_INVALID", "bot was blocked",
                "user is deactivated", "chat not found",
            ]):
                return False
            log.warning(f"Broadcast error [{mode}]: {desc}")
            return False
        return True

    except Exception as e:
        log.error(f"Broadcast exception [{mode}]: {e}")
        return False


async def prepare_broadcast(cid, msg):
    content = extract_broadcast_content(msg)
    content["_original_chat_id"] = cid
    content["_original_message_id"] = msg["message_id"]
    content["_send_mode"] = "reconstruct"

    if msg.get("forward_from_chat"):
        content["_forward_from_chat_id"] = msg["forward_from_chat"]["id"]
        content["_forward_from_chat_title"] = msg["forward_from_chat"].get("title", "")
    if msg.get("forward_from"):
        content["_forward_from_user"] = msg["forward_from"].get("first_name", "")

    await db.update_eq("users", {
        "admin_state": "broadcast_confirm",
        "broadcast_data": json.dumps(content),
    }, "telegram_id", ADMIN_ID)

    total = await db.count("users")
    type_name = CONTENT_TYPE_NAMES.get(content["type"], content["type"])
    preview = get_content_preview(content)

    fwd_note = ""
    if content.get("_forward_from_chat_title"):
        fwd_note = f"\n↗️ Переслано из: <b>{content['_forward_from_chat_title']}</b>"
    elif content.get("_forward_from_user"):
        fwd_note = f"\n↗️ Переслано от: <b>{content['_forward_from_user']}</b>"

    await send_msg(cid,
        f"📨 <b>Рассылка — выберите режим отправки</b>\n\n"
        f"📦 Тип: <b>{type_name}</b>{fwd_note}\n"
        f"{preview}\n\n"
        f"👥 Получателей: <b>{total}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Как отправить?</b>\n\n"
        f"📝 <b>Своим</b> — бот отправит как своё сообщение\n"
        f"📋 <b>Копией</b> — точная копия без «переслано от»\n"
        f"↗️ <b>Пересылкой</b> — с заголовком «переслано от»\n"
        f"   <i>(идеально для giveaway, розыгрышей, системных сообщений)</i>",
        {"inline_keyboard": [
            [{"text": "📝 Своим сообщением", "callback_data": "adm_bmode_reconstruct"}],
            [{"text": "📋 Копией", "callback_data": "adm_bmode_copy"}],
            [{"text": "↗️ Пересылкой", "callback_data": "adm_bmode_forward"}],
            [{"text": "❌ Отмена", "callback_data": "adm_broadcast_cancel"}],
        ]}
    )


async def show_broadcast_confirm(cid, msg_id=None):
    admin = await db.select_eq("users", "telegram_id", ADMIN_ID)
    if not admin:
        return
    content = json.loads(admin[0].get("broadcast_data") or "{}")
    if not content:
        return

    mode = content.get("_send_mode", "reconstruct")
    mode_name = SEND_MODE_NAMES.get(mode, mode)
    type_name = CONTENT_TYPE_NAMES.get(content.get("type", ""), "")
    preview = get_content_preview(content)
    total = await db.count("users")

    text = (
        f"📨 <b>Подтверждение рассылки</b>\n\n"
        f"📦 Тип: <b>{type_name}</b>\n"
        f"📤 Режим: <b>{mode_name}</b>\n"
        f"{preview}\n\n"
        f"👥 Получателей: <b>{total}</b>\n\n"
        f"Всё верно?"
    )
    kb = {"inline_keyboard": [
        [{"text": "✅ Отправить всем", "callback_data": "adm_broadcast_go"},
         {"text": "❌ Отмена", "callback_data": "adm_broadcast_cancel"}],
        [{"text": "📨 Тест (мне)", "callback_data": "adm_broadcast_test"}],
        [{"text": "🔄 Сменить режим", "callback_data": "adm_broadcast_chmode"}],
    ]}
    if msg_id:
        await edit_msg(cid, msg_id, text, kb)
    else:
        await send_msg(cid, text, kb)


async def do_broadcast(admin_cid):
    global broadcast_status
    if broadcast_status["running"]:
        await send_msg(admin_cid, "⚠️ Рассылка уже идёт!")
        return
    admin = await db.select_eq("users", "telegram_id", ADMIN_ID)
    if not admin:
        return
    content = json.loads(admin[0].get("broadcast_data") or "{}")
    if not content:
        return

    mode = content.get("_send_mode", "reconstruct")
    mode_name = SEND_MODE_NAMES.get(mode, mode)

    users = await db.select("users")
    broadcast_status = {"running": True, "total": len(users),
                        "sent": 0, "failed": 0, "blocked": 0}

    type_name = CONTENT_TYPE_NAMES.get(content.get("type", ""), "")
    log.info(f"Broadcast started: {type_name} [{mode}] → {len(users)} users")

    sm = await send_msg(admin_cid,
        f"🚀 Рассылка запущена!\n"
        f"📦 {type_name}\n"
        f"📤 {mode_name}\n"
        f"👥 {broadcast_status['total']}")
    sm_id = sm.get("result", {}).get("message_id") if sm.get("ok") else None

    for i, u in enumerate(users):
        try:
            ok = await send_broadcast_msg(u["telegram_id"], content, mode)
            if ok:
                broadcast_status["sent"] += 1
            else:
                broadcast_status["blocked"] += 1
        except Exception:
            broadcast_status["failed"] += 1

        if sm_id and (i + 1) % 25 == 0:
            try:
                await edit_msg(admin_cid, sm_id,
                    f"🚀 Рассылка...\n"
                    f"✅ {broadcast_status['sent']}  "
                    f"🚫 {broadcast_status['blocked']}  "
                    f"❌ {broadcast_status['failed']}\n"
                    f"📊 {i+1}/{broadcast_status['total']}")
            except Exception:
                pass
        await asyncio.sleep(0.05)

    broadcast_status["running"] = False
    log.info(f"Broadcast done: sent={broadcast_status['sent']} "
             f"blocked={broadcast_status['blocked']} failed={broadcast_status['failed']}")

    final = (f"✅ <b>Рассылка завершена!</b>\n\n"
             f"📤 Режим: {mode_name}\n"
             f"✅ Доставлено: {broadcast_status['sent']}\n"
             f"🚫 Заблокировали: {broadcast_status['blocked']}\n"
             f"❌ Ошибки: {broadcast_status['failed']}\n"
             f"📊 Всего: {broadcast_status['total']}")
    if sm_id:
        await edit_msg(admin_cid, sm_id, final)
    else:
        await send_msg(admin_cid, final)
    await db.update_eq("users", {"admin_state": "", "broadcast_data": ""},
                       "telegram_id", ADMIN_ID)

    await log_event(ADMIN_ID, "broadcast", {
        "mode": mode, "sent": broadcast_status["sent"],
        "blocked": broadcast_status["blocked"], "failed": broadcast_status["failed"],
    })


# ══════════════════════════════════════
#  Admin callbacks
# ══════════════════════════════════════
async def handle_callback(cb):
    uid = cb["from"]["id"]
    data = cb["data"]
    cid = cb["message"]["chat"]["id"]
    mid = cb["message"]["message_id"]

    if uid != ADMIN_ID:
        await answer_cb(cb["id"], "⛔ Нет доступа", True)
        return
    await answer_cb(cb["id"])

    if data == "adm_menu":
        await show_admin_menu(cid, mid)

    elif data == "adm_channels":
        sponsors = await get_sponsors_cached()
        chs = [s for s in sponsors if s.get("type", "channel") == "channel"]
        text = "📢 <b>Каналы:</b>\n\n"
        if not chs:
            text += "Пусто."
        for i, c in enumerate(chs, 1):
            text += f"{i}. <b>{c['title']}</b> 👥{c.get('member_count',0)}\n"
        btns = [[{"text": f"❌ {c['title'][:20]}",
                  "callback_data": f"adm_del_sp:{c['channel_id']}"}] for c in chs]
        btns.append([{"text": "➕ Добавить", "callback_data": "adm_add_ch"}])
        btns.append([{"text": "← Назад", "callback_data": "adm_menu"}])
        await edit_msg(cid, mid, text, {"inline_keyboard": btns})

    elif data == "adm_add_ch":
        await db.update_eq("users", {"admin_state": "add_channel"}, "telegram_id", ADMIN_ID)
        await edit_msg(cid, mid, "📢 Отправьте @username канала\n⚠️ Бот должен быть админом!",
                       {"inline_keyboard": [[{"text": "← Отмена",
                                              "callback_data": "adm_channels"}]]})

    elif data == "adm_bots":
        sponsors = await get_sponsors_cached()
        bots = [s for s in sponsors if s.get("type") == "bot"]
        text = "🤖 <b>Боты:</b>\n\n"
        if not bots:
            text += "Пусто."
        for i, b in enumerate(bots, 1):
            text += f"{i}. <b>{b['title']}</b>\n"
        btns = [[{"text": f"❌ {b['title'][:20]}",
                  "callback_data": f"adm_del_sp:{b['channel_id']}"}] for b in bots]
        btns.append([{"text": "➕ Добавить", "callback_data": "adm_add_bot"}])
        btns.append([{"text": "← Назад", "callback_data": "adm_menu"}])
        await edit_msg(cid, mid, text, {"inline_keyboard": btns})

    elif data == "adm_add_bot":
        await db.update_eq("users", {"admin_state": "add_bot"}, "telegram_id", ADMIN_ID)
        await edit_msg(cid, mid, "🤖 Отправьте @username бота:",
                       {"inline_keyboard": [[{"text": "← Отмена",
                                              "callback_data": "adm_bots"}]]})

    elif data.startswith("adm_del_sp:"):
        sp_id = data.split(":")[1]
        items = await db.select_eq("channels", "channel_id", sp_id)
        sp_type = items[0].get("type", "channel") if items else "channel"
        await db.update_eq("channels", {"is_active": False}, "channel_id", sp_id)
        invalidate_cache()

        if sp_type == "bot":
            sponsors = await get_sponsors()
            bots = [s for s in sponsors if s.get("type") == "bot"]
            text = "🤖 <b>Боты:</b>\n\n"
            for i, b in enumerate(bots, 1):
                text += f"{i}. <b>{b['title']}</b>\n"
            if not bots:
                text += "Пусто."
            btns = [[{"text": f"❌ {b['title'][:20]}",
                      "callback_data": f"adm_del_sp:{b['channel_id']}"}] for b in bots]
            btns.append([{"text": "➕ Добавить", "callback_data": "adm_add_bot"}])
            btns.append([{"text": "← Назад", "callback_data": "adm_menu"}])
        else:
            sponsors = await get_sponsors()
            chs = [s for s in sponsors if s.get("type", "channel") == "channel"]
            text = "📢 <b>Каналы:</b>\n\n"
            for i, c in enumerate(chs, 1):
                text += f"{i}. <b>{c['title']}</b>\n"
            if not chs:
                text += "Пусто."
            btns = [[{"text": f"❌ {c['title'][:20]}",
                      "callback_data": f"adm_del_sp:{c['channel_id']}"}] for c in chs]
            btns.append([{"text": "➕ Добавить", "callback_data": "adm_add_ch"}])
            btns.append([{"text": "← Назад", "callback_data": "adm_menu"}])
        await edit_msg(cid, mid, text, {"inline_keyboard": btns})

    elif data == "adm_prizes":
        prs = await db.select("prizes", order="sort_order.asc")
        text = "🎁 <b>Призы:</b>\n\n"
        for p in prs:
            s = "✅" if p["is_active"] else "❌"
            text += f"{s} {p['emoji']} <b>{p['name']}</b>\n"
        btns = [[
            {"text": f"✏️ {p['name'][:15]}",
             "callback_data": f"adm_edit_pr:{p['key']}"},
            {"text": "🟢" if p["is_active"] else "🔴",
             "callback_data": f"adm_toggle_pr:{p['key']}"},
        ] for p in prs]
        btns.append([{"text": "← Назад", "callback_data": "adm_menu"}])
        await edit_msg(cid, mid, text, {"inline_keyboard": btns})

    elif data.startswith("adm_edit_pr:"):
        key = data.split(":")[1]
        await db.update_eq("users", {"admin_state": f"edit_prize:{key}"},
                           "telegram_id", ADMIN_ID)
        p = await db.select_eq("prizes", "key", key)
        name = p[0]["name"] if p else key
        await edit_msg(cid, mid,
                       f"✏️ Текущее: <b>{name}</b>\nОтправьте новое название:",
                       {"inline_keyboard": [[{"text": "← Отмена",
                                              "callback_data": "adm_prizes"}]]})

    elif data.startswith("adm_toggle_pr:"):
        key = data.split(":")[1]
        p = await db.select_eq("prizes", "key", key)
        if p:
            await db.update_eq("prizes", {"is_active": not p[0]["is_active"]}, "key", key)
        invalidate_cache()
        prs = await db.select("prizes", order="sort_order.asc")
        text = "🎁 <b>Призы:</b>\n\n"
        for p in prs:
            s = "✅" if p["is_active"] else "❌"
            text += f"{s} {p['emoji']} <b>{p['name']}</b>\n"
        btns = [[
            {"text": f"✏️ {p['name'][:15]}",
             "callback_data": f"adm_edit_pr:{p['key']}"},
            {"text": "🟢" if p["is_active"] else "🔴",
             "callback_data": f"adm_toggle_pr:{p['key']}"},
        ] for p in prs]
        btns.append([{"text": "← Назад", "callback_data": "adm_menu"}])
        await edit_msg(cid, mid, text, {"inline_keyboard": btns})

    elif data == "adm_stats":
        total = await db.count("users")
        new = await db.count("users", {"state": "eq.new"})
        rolled = await db.count("users", {"state": "eq.rolled"})
        claimed = await db.count("users", {"state": "eq.claimed"})
        with_ref = await db.count("users", {"referred_by": "neq.null"})
        recent = await db.select("users", order="created_at.desc", limit=5)

        text = (
            f"📊 <b>Статистика</b>\n\n"
            f"👥 Всего: <b>{total}</b>\n"
            f"🆕 Новые: <b>{new}</b>\n"
            f"🎰 Крутили: <b>{rolled}</b>\n"
            f"✅ Подписались: <b>{claimed}</b>\n"
            f"🔗 По рефералам: <b>{with_ref}</b>\n\n"
            f"📈 Конверсия: "
            f"<b>{round(((rolled+claimed)/total)*100) if total else 0}%</b> крутили → "
            f"<b>{round((claimed/total)*100) if total else 0}%</b> подписались\n"
        )
        if recent:
            text += "\n👤 <b>Последние:</b>\n"
            for u in recent:
                n = u.get("first_name") or u.get("username") or str(u["telegram_id"])
                text += f"  • {n} — {u['state']}"
                if u.get("prize_name"):
                    text += f" ({u['prize_name']})"
                if u.get("referred_by"):
                    text += " 🔗"
                text += "\n"
        await edit_msg(cid, mid, text,
                       {"inline_keyboard": [[{"text": "← Назад",
                                              "callback_data": "adm_menu"}]]})

    elif data == "adm_refresh":
        sponsors = await get_sponsors()
        for s in sponsors:
            if s.get("type") == "bot":
                info = await parse_bot(
                    f"@{s['username']}" if s.get("username") else str(s["channel_id"]))
            else:
                info = await parse_channel(str(s["channel_id"]))
            if info:
                await db.update_eq("channels", {
                    "title": info["title"], "username": info["username"],
                    "invite_link": info["invite_link"],
                    "avatar_base64": info["avatar_base64"],
                    "member_count": info.get("member_count", 0),
                }, "channel_id", s["channel_id"])
        invalidate_cache()
        await show_admin_menu(cid, mid)

    elif data == "adm_broadcast":
        if broadcast_status["running"]:
            await edit_msg(cid, mid,
                f"⏳ Рассылка идёт!\n"
                f"✅ {broadcast_status['sent']}  "
                f"🚫 {broadcast_status['blocked']}  "
                f"❌ {broadcast_status['failed']}\n"
                f"📊 {broadcast_status['sent']+broadcast_status['blocked']+broadcast_status['failed']}"
                f"/{broadcast_status['total']}",
                {"inline_keyboard": [[{"text": "← Назад",
                                       "callback_data": "adm_menu"}]]})
            return
        await db.update_eq("users", {"admin_state": "broadcast_text"},
                           "telegram_id", ADMIN_ID)
        await edit_msg(cid, mid,
            "📨 <b>Рассылка</b>\n\n"
            "Отправьте <b>любое</b> сообщение:\n\n"
            "📝 Текст\n"
            "🖼 Фото / 🎬 Видео / 🎞 GIF\n"
            "🎭 Стикер / 📎 Файл\n"
            "🎤 Голосовое / ⚪ Кружок / 🎵 Аудио\n"
            "👤 Контакт / 📍 Локация / 🏢 Место\n"
            "📊 Опрос / 🎲 Кость\n\n"
            "💡 <b>Совет:</b> Перешлите сюда сообщение из канала\n"
            "(розыгрыш, giveaway, системное) — бот предложит\n"
            "переслать его всем пользователям!",
            {"inline_keyboard": [[{"text": "← Отмена",
                                   "callback_data": "adm_menu"}]]})

    elif data.startswith("adm_bmode_"):
        mode = data.replace("adm_bmode_", "")
        admin = await db.select_eq("users", "telegram_id", ADMIN_ID)
        if admin and admin[0].get("broadcast_data"):
            content = json.loads(admin[0]["broadcast_data"])
            content["_send_mode"] = mode
            await db.update_eq("users", {"broadcast_data": json.dumps(content)},
                               "telegram_id", ADMIN_ID)
        await show_broadcast_confirm(cid, mid)

    elif data == "adm_broadcast_chmode":
        admin = await db.select_eq("users", "telegram_id", ADMIN_ID)
        if not admin or not admin[0].get("broadcast_data"):
            await show_admin_menu(cid, mid)
            return
        content = json.loads(admin[0]["broadcast_data"])
        current = content.get("_send_mode", "reconstruct")
        type_name = CONTENT_TYPE_NAMES.get(content.get("type", ""), "")

        def mk(label, key):
            mark = " ✓" if key == current else ""
            return {"text": f"{label}{mark}", "callback_data": f"adm_bmode_{key}"}

        await edit_msg(cid, mid,
            f"🔄 <b>Сменить режим отправки</b>\n\n"
            f"📦 Контент: {type_name}\n"
            f"📤 Текущий: <b>{SEND_MODE_NAMES.get(current, current)}</b>\n\n"
            f"📝 <b>Своим</b> — бот отправит как своё новое сообщение\n"
            f"📋 <b>Копией</b> — точная копия без «переслано от»\n"
            f"↗️ <b>Пересылкой</b> — с заголовком «переслано от»\n"
            f"   <i>(для giveaway, розыгрышей, системных сообщений)</i>",
            {"inline_keyboard": [
                [mk("📝 Своим сообщением", "reconstruct")],
                [mk("📋 Копией", "copy")],
                [mk("↗️ Пересылкой", "forward")],
                [{"text": "← Назад к подтверждению",
                  "callback_data": "adm_broadcast_back_confirm"}],
            ]}
        )

    elif data == "adm_broadcast_back_confirm":
        await show_broadcast_confirm(cid, mid)

    elif data == "adm_broadcast_test":
        admin = await db.select_eq("users", "telegram_id", ADMIN_ID)
        if admin and admin[0].get("broadcast_data"):
            content = json.loads(admin[0]["broadcast_data"])
            mode = content.get("_send_mode", "reconstruct")
            ok = await send_broadcast_msg(ADMIN_ID, content, mode)
            mode_name = SEND_MODE_NAMES.get(mode, mode)
            await send_msg(cid,
                f"{'✅ Тест отправлен!' if ok else '❌ Ошибка отправки'}\n"
                f"📤 Режим: {mode_name}\n\n"
                f"Проверьте сообщение выше ☝️",
                {"inline_keyboard": [
                    [{"text": "✅ Отправить всем",
                      "callback_data": "adm_broadcast_go"},
                     {"text": "❌ Отмена",
                      "callback_data": "adm_broadcast_cancel"}],
                    [{"text": "📨 Тест ещё раз",
                      "callback_data": "adm_broadcast_test"}],
                    [{"text": "🔄 Сменить режим",
                      "callback_data": "adm_broadcast_chmode"}],
                ]})

    elif data == "adm_broadcast_go":
        await db.update_eq("users", {"admin_state": ""}, "telegram_id", ADMIN_ID)
        asyncio.create_task(do_broadcast(cid))

    elif data == "adm_broadcast_cancel":
        await db.update_eq("users", {"admin_state": "", "broadcast_data": ""},
                           "telegram_id", ADMIN_ID)
        await show_admin_menu(cid, mid)


# ══════════════════════════════════════
#  Lifecycle
# ══════════════════════════════════════
@app.on_event("startup")
async def on_startup():
    await db.start()

    # Auto webhook
    webhook_url = f"{WEBAPP_URL}/api/webhook"
    r = await tg("setWebhook", {
        "url": webhook_url,
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": False,
    })
    if r.get("ok"):
        log.info(f"Webhook set: {webhook_url}")
    else:
        log.error(f"Webhook error: {r}")

    # Pre-warm cache
    await get_sponsors_cached()
    await get_prizes_cached()
    log.info("Cache pre-warmed")

    asyncio.create_task(background_worker())
    asyncio.create_task(rate_limit_cleanup())


@app.on_event("shutdown")
async def on_shutdown():
    if broadcast_status["running"]:
        log.warning("Broadcast running during shutdown, waiting...")
        for _ in range(30):
            if not broadcast_status["running"]:
                break
            await asyncio.sleep(1)

    await db.stop()
    global tg_client
    if tg_client:
        await tg_client.aclose()
        tg_client = None
    log.info("Shutdown complete")


# ══════════════════════════════════════
#  Background worker
# ══════════════════════════════════════
async def background_worker():
    await asyncio.sleep(60)
    while True:
        try:
            await check_reactivation()
        except Exception as e:
            log.error(f"[bg reactivation] {e}")
        try:
            await check_antifraud()
        except Exception as e:
            log.error(f"[bg antifraud] {e}")
        await asyncio.sleep(300)


async def check_reactivation():
    users = await db.select("users", {"state": "eq.new", "notified_1h": "eq.false"})
    now = datetime.now(timezone.utc)
    notified = 0
    for u in users:
        created = parse_dt(u.get("created_at"))
        if not created or (now - created).total_seconds() < 3600:
            continue
        try:
            await send_msg(u["telegram_id"],
                "🔥 <b>ОСТАЛОСЬ ОЧЕНЬ МАЛО ВРЕМЕНИ ДО КОНЦА РАЗДАЧИ ПОДАРКОВ!</b>\n\n"
                "⏰ Поспеши забрать свой подарок!\n"
                "Не упусти шанс — это бесплатно! 🎁",
                {"inline_keyboard": [[
                    {"text": "⭐ НАРИСОВАТЬ ЗВЕЗДУ!",
                     "web_app": {"url": WEBAPP_URL}}
                ]]}
            )
            notified += 1
        except Exception:
            pass
        await db.update_eq("users", {"notified_1h": True},
                           "telegram_id", u["telegram_id"])
        await asyncio.sleep(0.1)
    if notified:
        log.info(f"Reactivation: notified {notified} users")


async def check_antifraud():
    users = await db.select("users", {"state": "eq.claimed", "notified_unsub": "eq.false"})
    if not users:
        return
    now = datetime.now(timezone.utc)
    notified = 0
    for u in users:
        claimed = parse_dt(u.get("claimed_at"))
        if not claimed:
            continue
        ref_count = await count_referrals(u["telegram_id"])
        speed = 2 ** (ref_count // 2) if ref_count >= 2 else 1
        effective_duration = 86400 / speed
        elapsed = (now - claimed).total_seconds()
        if elapsed < effective_duration:
            continue
        try:
            await send_msg(u["telegram_id"],
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ <b>ПРИЗ НЕ МОЖЕТ БЫТЬ ВЫДАН!</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"⚠️ Обнаружена проблема с подписками.\n\n"
                f"📢 Проверьте что вы подписаны на <b>все</b> необходимые "
                f"каналы и попробуйте ещё раз!\n\n"
                f"👇 Нажмите кнопку ниже:",
                {"inline_keyboard": [[
                    {"text": "🔄 Проверить и получить приз",
                     "web_app": {"url": WEBAPP_URL}}
                ]]}
            )
            notified += 1
        except Exception:
            pass
        await db.update_eq("users", {"notified_unsub": True},
                           "telegram_id", u["telegram_id"])
        await asyncio.sleep(0.1)
    if notified:
        log.info(f"Antifraud: notified {notified} users")


# ══════════════════════════════════════
#  Static files
# ══════════════════════════════════════
app.mount("/assets", StaticFiles(directory="public/assets"), name="assets")


@app.get("/")
async def root():
    return FileResponse("public/index.html")


@app.get("/{path:path}")
async def catch_all(path: str):
    fp = f"public/{path}"
    if os.path.isfile(fp):
        return FileResponse(fp)
    return FileResponse("public/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8889)