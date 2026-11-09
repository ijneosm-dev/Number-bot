# -*- coding: utf-8 -*-
"""
advanced_features.py
====================
Drop this file next to bot-3.py.  Add ONE line near the top of bot-3.py
(right after the analytics_addon import block):

    from advanced_features import (
        check_rate_limit,
        get_text, set_user_language, get_user_language,
        init_advanced_tables,
        send_backup_to_channel,
        broadcast_targeted,
        format_referral_card,
        handle_advanced_callback,
        start_advanced_threads,
    )

Then apply the surgical patches listed at the bottom of this file.

Features
--------
  1. Token Bucket Rate Limiter   — per-user spam/flood protection
  2. Multi-Language i18n         — user-selectable UI language; bot menus
                                   and prompts respond in the user's language
  3. Auto Cloud Backup           — 24-hour schedule; sends to BACKUP_CHANNEL_ID
                                   (env var) in addition to the owner DM
  4. Targeted Broadcast          — admin can blast to ALL, VIP, or specific
                                   language cohorts without hitting TG rate limits
  5. Referral Display            — /ref command renders a styled referral card
  6. Analytics Dashboard Button  — already wired via analytics_addon; this
                                   module adds the missing Admin Panel button
                                   so no second file is needed
"""

import html
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  LAZY RESOLVER  (same pattern as analytics_addon)
# ─────────────────────────────────────────────────────────────────────────────

def _g(name):
    """Pull a global from the bot-3.py namespace at call-time."""
    for mod in sys.modules.values():
        if hasattr(mod, name) and hasattr(mod, "BOT_USERNAME"):
            return getattr(mod, name)
    raise RuntimeError(f"advanced_features: could not resolve '{name}'")


# ══════════════════════════════════════════════════════════════════════════════
#  FEATURE 1 — TOKEN BUCKET RATE LIMITER
#  Protects against per-user spam / flood at the handler level.
#  Each user gets a bucket of RATE_BUCKET_CAPACITY tokens that refills at
#  RATE_REFILL_RATE tokens/second.  Every handled message costs 1 token.
#  Penalty kicks in when the bucket is empty: we silently drop the update.
# ══════════════════════════════════════════════════════════════════════════════

RATE_BUCKET_CAPACITY  = int(os.environ.get("RATE_BUCKET_CAPACITY",  "10"))
RATE_REFILL_RATE      = float(os.environ.get("RATE_REFILL_RATE",    "1.0"))   # tokens/sec
RATE_BURST_WARN_AFTER = int(os.environ.get("RATE_BURST_WARN_AFTER", "5"))     # consecutive drops before log

_rate_buckets: dict[str, dict] = {}    # { user_id_str: {tokens, last_refill, drops} }
_rate_lock = threading.RLock()


def _refill(bucket: dict) -> None:
    """Refill tokens based on time elapsed since last refill."""
    now = time.monotonic()
    elapsed = now - bucket["last_refill"]
    gained = elapsed * RATE_REFILL_RATE
    bucket["tokens"] = min(RATE_BUCKET_CAPACITY, bucket["tokens"] + gained)
    bucket["last_refill"] = now


def check_rate_limit(user_id) -> bool:
    """
    Call at the top of every update handler.
    Returns True  → proceed normally.
    Returns False → drop this update (user is flooding).

    Usage in bot-3.py handle_message / handle_callback:
        if not check_rate_limit(chat_id):
            return
    """
    uid = str(user_id)
    with _rate_lock:
        if uid not in _rate_buckets:
            _rate_buckets[uid] = {
                "tokens":      RATE_BUCKET_CAPACITY,
                "last_refill": time.monotonic(),
                "drops":       0,
            }
        bucket = _rate_buckets[uid]
        _refill(bucket)
        if bucket["tokens"] >= 1.0:
            bucket["tokens"] -= 1.0
            bucket["drops"] = 0
            return True
        # bucket empty → rate-limited
        bucket["drops"] += 1
        if bucket["drops"] == RATE_BURST_WARN_AFTER:
            logger.warning("Rate-limit: user %s has been dropped %s times in a row",
                           uid, bucket["drops"])
        return False


def get_rate_stats(user_id) -> dict:
    """Return current bucket stats for a user (admin diagnostics)."""
    uid = str(user_id)
    with _rate_lock:
        if uid not in _rate_buckets:
            return {"tokens": RATE_BUCKET_CAPACITY, "drops": 0}
        b = _rate_buckets[uid]
        _refill(b)
        return {"tokens": round(b["tokens"], 2), "drops": b["drops"]}


# ══════════════════════════════════════════════════════════════════════════════
#  FEATURE 2 — MULTI-LANGUAGE i18n
#  Every UI string lives in TRANSLATIONS keyed by (lang_code, key).
#  get_text(user_id, key, **fmt) looks up the user's stored language first,
#  falls back to English.  set_user_language / get_user_language persist the
#  choice to SQLite (user_language column in the users table).
# ══════════════════════════════════════════════════════════════════════════════

#  ── Translation table ──────────────────────────────────────────────────────
#  Add more keys here as you expose more strings.  Format: {key: translated}.
TRANSLATIONS: dict[str, dict[str, str]] = {
    "EN": {
        "welcome":           "👋 Welcome, {name}! Use the menu below.",
        "balance":           "💰 Your Balance: <b>{balance} TK</b>",
        "refer_header":      "🎁 Refer & Earn",
        "refer_link_label":  "🔗 Your Link:",
        "refer_total":       "👥 Total Refers: <b>{total}</b>",
        "refer_reward":      "💸 Per Refer: <b>{reward} TK</b>",
        "lang_set":          "✅ Language set to English.",
        "rate_limited":      "⚠️ You are sending too fast. Please slow down.",
        "broadcast_done":    "📢 Broadcast complete.\n✅ Sent: {sent}\n❌ Failed: {failed}",
        "no_permission":     "❌ You don't have permission to use this.",
        "select_language":   "🌐 Select your language:",
        "help":              "ℹ️ Send /start to begin. Send /ref for your referral link.",
        "withdraw_off":      "⚠️ Withdrawals are currently disabled.",
        "choose_target":     "📤 Choose broadcast target:",
        # Menu buttons
        "menu_get_number":   "GET NUMBER",
        "menu_search":       "Search Number",
        "menu_traffic":      "TRAFFIC",
        "menu_2fa":          "2FA ONLINE",
        "menu_refer":        "Refer",
        "menu_withdrawal":   "WITHDRAWAL",
        "menu_support":      "SUPPORT",
        "menu_admin":        "Admin Panel",
    },
    "BN": {
        "welcome":           "👋 স্বাগতম, {name}! নিচের মেনু ব্যবহার করুন।",
        "balance":           "💰 আপনার ব্যালেন্স: <b>{balance} টাকা</b>",
        "refer_header":      "🎁 রেফার করুন ও আয় করুন",
        "refer_link_label":  "🔗 আপনার লিংক:",
        "refer_total":       "👥 মোট রেফার: <b>{total}</b>",
        "refer_reward":      "💸 প্রতি রেফারে: <b>{reward} টাকা</b>",
        "lang_set":          "✅ ভাষা বাংলায় সেট করা হয়েছে।",
        "rate_limited":      "⚠️ আপনি খুব দ্রুত বার্তা পাঠাচ্ছেন। একটু ধীরে পাঠান।",
        "broadcast_done":    "📢 ব্রডকাস্ট সম্পন্ন।\n✅ পাঠানো হয়েছে: {sent}\n❌ ব্যর্থ: {failed}",
        "no_permission":     "❌ আপনার এই কাজের অনুমতি নেই।",
        "select_language":   "🌐 আপনার ভাষা নির্বাচন করুন:",
        "help":              "ℹ️ শুরু করতে /start পাঠান। রেফারেল লিংকের জন্য /ref পাঠান।",
        "withdraw_off":      "⚠️ বর্তমানে উইথড্র বন্ধ আছে।",
        "choose_target":     "📤 ব্রডকাস্টের লক্ষ্য বেছে নিন:",
        # Menu buttons
        "menu_get_number":   "নম্বর নিন",
        "menu_search":       "নম্বর খুঁজুন",
        "menu_traffic":      "ট্র্যাফিক",
        "menu_2fa":          "2FA অনলাইন",
        "menu_refer":        "রেফার করুন",
        "menu_withdrawal":   "উইথড্র",
        "menu_support":      "সাপোর্ট",
        "menu_admin":        "অ্যাডমিন প্যানেল",
    },
    "AR": {
        "welcome":           "👋 مرحباً {name}! استخدم القائمة أدناه.",
        "balance":           "💰 رصيدك: <b>{balance} TK</b>",
        "refer_header":      "🎁 أحِل واربح",
        "refer_link_label":  "🔗 رابطك:",
        "refer_total":       "👥 إجمالي الإحالات: <b>{total}</b>",
        "refer_reward":      "💸 مكافأة الإحالة: <b>{reward} TK</b>",
        "lang_set":          "✅ تم ضبط اللغة على العربية.",
        "rate_limited":      "⚠️ أنت ترسل بسرعة كبيرة. يرجى الإبطاء.",
        "broadcast_done":    "📢 اكتمل البث.\n✅ مُرسَل: {sent}\n❌ فشل: {failed}",
        "no_permission":     "❌ ليس لديك إذن لاستخدام هذا.",
        "select_language":   "🌐 اختر لغتك:",
        "help":              "ℹ️ أرسل /start للبدء. أرسل /ref للحصول على رابط الإحالة.",
        "withdraw_off":      "⚠️ السحب معطّل حالياً.",
        "choose_target":     "📤 اختر هدف البث:",
        # Menu buttons
        "menu_get_number":   "احصل على رقم",
        "menu_search":       "بحث عن رقم",
        "menu_traffic":      "الحركة",
        "menu_2fa":          "2FA أونلاين",
        "menu_refer":        "أحِل صديقاً",
        "menu_withdrawal":   "سحب",
        "menu_support":      "الدعم",
        "menu_admin":        "لوحة الأدمن",
    },
    "HI": {
        "welcome":           "👋 स्वागत है, {name}! नीचे मेनू का उपयोग करें।",
        "balance":           "💰 आपका बैलेंस: <b>{balance} TK</b>",
        "refer_header":      "🎁 रेफर करें और कमाएं",
        "refer_link_label":  "🔗 आपका लिंक:",
        "refer_total":       "👥 कुल रेफर: <b>{total}</b>",
        "refer_reward":      "💸 प्रति रेफर: <b>{reward} TK</b>",
        "lang_set":          "✅ भाषा हिंदी में सेट की गई।",
        "rate_limited":      "⚠️ आप बहुत तेज़ संदेश भेज रहे हैं। धीरे भेजें।",
        "broadcast_done":    "📢 ब्रॉडकास्ट पूरा हुआ।\n✅ भेजा: {sent}\n❌ विफल: {failed}",
        "no_permission":     "❌ आपके पास इसकी अनुमति नहीं है।",
        "select_language":   "🌐 अपनी भाषा चुनें:",
        "help":              "ℹ️ शुरू करने के लिए /start भेजें। रेफरल लिंक के लिए /ref भेजें।",
        "withdraw_off":      "⚠️ अभी निकासी बंद है।",
        "choose_target":     "📤 ब्रॉडकास्ट लक्ष्य चुनें:",
        # Menu buttons
        "menu_get_number":   "नंबर लें",
        "menu_search":       "नंबर खोजें",
        "menu_traffic":      "ट्रैफ़िक",
        "menu_2fa":          "2FA ऑनलाइन",
        "menu_refer":        "रेफर करें",
        "menu_withdrawal":   "निकासी",
        "menu_support":      "सहायता",
        "menu_admin":        "एडमिन पैनल",
    },
    "RU": {
        "welcome":           "👋 Добро пожаловать, {name}! Используйте меню ниже.",
        "balance":           "💰 Ваш баланс: <b>{balance} TK</b>",
        "refer_header":      "🎁 Приглашай и зарабатывай",
        "refer_link_label":  "🔗 Ваша ссылка:",
        "refer_total":       "👥 Всего приглашений: <b>{total}</b>",
        "refer_reward":      "💸 За каждое: <b>{reward} TK</b>",
        "lang_set":          "✅ Язык установлен: Русский.",
        "rate_limited":      "⚠️ Вы отправляете слишком быстро. Подождите.",
        "broadcast_done":    "📢 Рассылка завершена.\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}",
        "no_permission":     "❌ У вас нет прав для этого.",
        "select_language":   "🌐 Выберите язык:",
        "help":              "ℹ️ /start — начать. /ref — реферальная ссылка.",
        "withdraw_off":      "⚠️ Вывод средств временно отключён.",
        "choose_target":     "📤 Выберите цель рассылки:",
        # Menu buttons
        "menu_get_number":   "Получить номер",
        "menu_search":       "Найти номер",
        "menu_traffic":      "Трафик",
        "menu_2fa":          "2FA Онлайн",
        "menu_refer":        "Пригласить",
        "menu_withdrawal":   "Вывод",
        "menu_support":      "Поддержка",
        "menu_admin":        "Панель админа",
    },
}

# Map ISO code → readable name (used in language picker keyboard)
LANG_DISPLAY: dict[str, str] = {
    "EN": "🇬🇧 English",
    "BN": "🇧🇩 বাংলা",
    "AR": "🇸🇦 العربية",
    "HI": "🇮🇳 हिंदी",
    "RU": "🇷🇺 Русский",
}

def get_all_menu_texts() -> set:
    """
    Return every possible translated menu button text across all languages.
    Used to build the MAIN_MENU_CMDS list so the bot resets state for any
    language's version of a menu button press.
    """
    keys = [
        "menu_get_number", "menu_search", "menu_traffic", "menu_2fa",
        "menu_refer", "menu_withdrawal", "menu_support", "menu_admin",
    ]
    texts = set()
    for lang_table in TRANSLATIONS.values():
        for key in keys:
            val = lang_table.get(key)
            if val:
                texts.add(val)
    return texts


# In-memory cache: { user_id_str: "EN" }
_lang_cache: dict[str, str] = {}
_lang_cache_lock = threading.RLock()


def init_advanced_tables() -> None:
    """
    Add user_language column to the users table if absent.
    Call once from inside init_db() — see Patch #1.
    """
    db_file = _g("DB_FILE")
    conn = sqlite3.connect(db_file, timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
        if "user_language" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN user_language TEXT DEFAULT 'EN'")
            conn.commit()
            logger.info("advanced_features: user_language column added to users table.")
        else:
            logger.info("advanced_features: user_language column already present.")
    finally:
        conn.close()


def get_user_language(user_id) -> str:
    """Return the stored language code for this user ('EN' if not set)."""
    uid = str(user_id)
    with _lang_cache_lock:
        if uid in _lang_cache:
            return _lang_cache[uid]
    db_file = _g("DB_FILE")
    conn = sqlite3.connect(db_file, timeout=5)
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        row = conn.execute(
            "SELECT user_language FROM users WHERE user_id = ?", (uid,)
        ).fetchone()
        lang = (row[0] or "EN").upper() if row else "EN"
    finally:
        conn.close()
    with _lang_cache_lock:
        _lang_cache[uid] = lang
    return lang


def set_user_language(user_id, lang_code: str) -> None:
    """Persist the user's language preference to SQLite + invalidate cache."""
    uid = str(user_id)
    lang = lang_code.upper()
    db_file = _g("DB_FILE")
    conn = sqlite3.connect(db_file, timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        conn.execute(
            "UPDATE users SET user_language = ? WHERE user_id = ?",
            (lang, uid),
        )
        conn.commit()
    finally:
        conn.close()
    with _lang_cache_lock:
        _lang_cache[uid] = lang
    logger.info("advanced_features: user %s language → %s", uid, lang)


def get_text(user_id, key: str, **fmt) -> str:
    """
    Resolve a UI string for user_id in their stored language.
    Falls back to English.  Fills any {placeholder} with **fmt.

    Example:
        get_text(chat_id, "welcome", name="Alice")
    """
    lang = get_user_language(user_id)
    table = TRANSLATIONS.get(lang) or TRANSLATIONS["EN"]
    en_table = TRANSLATIONS["EN"]
    template = table.get(key) or en_table.get(key, key)
    try:
        return template.format(**fmt) if fmt else template
    except KeyError:
        return template


def build_language_keyboard() -> dict:
    """Build an inline keyboard for the /setlang command."""
    buttons = [
        [{"text": label, "callback_data": f"setlang_{code}"}]
        for code, label in LANG_DISPLAY.items()
    ]
    return {"inline_keyboard": buttons}


# ══════════════════════════════════════════════════════════════════════════════
#  FEATURE 3 — AUTO CLOUD BACKUP TO CHANNEL
#  Set BACKUP_CHANNEL_ID env var to a channel/group ID where the bot is admin.
#  Every 24 hours (on the same cycle as the owner DM backup) the bot also
#  posts the .db file there, giving you a cloud-synced off-site copy.
# ══════════════════════════════════════════════════════════════════════════════

BACKUP_CHANNEL_ID: str = os.environ.get("BACKUP_CHANNEL_ID", "").strip()


def send_backup_to_channel(backup_path: str) -> bool:
    """
    Post the SQLite backup to BACKUP_CHANNEL_ID.
    Returns True on success.  Silently skips when env var is not set.
    """
    if not BACKUP_CHANNEL_ID:
        return False
    try:
        with open(backup_path, "rb") as fh:
            content = fh.read()
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        fname = f"cloud_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        BASE_URL   = _g("BASE_URL")
        INSTANCE_ID = _g("INSTANCE_ID")
        import requests as _req
        caption = (
            f"☁️ <b>Cloud Backup — {stamp}</b>\n"
            f"📦 Size: {len(content) // 1024} KB\n"
            f"🤖 Instance: <code>{INSTANCE_ID}</code>"
        )
        r = _req.post(
            f"{BASE_URL}/sendDocument",
            data={
                "chat_id":    BACKUP_CHANNEL_ID,
                "caption":    caption,
                "parse_mode": "HTML",
            },
            files={"document": (fname, content, "application/octet-stream")},
            timeout=60,
        )
        if r.ok:
            logger.info("advanced_features: cloud backup posted to channel (%s KB)",
                        len(content) // 1024)
            return True
        logger.warning("advanced_features: cloud backup to channel failed HTTP %s", r.status_code)
        return False
    except Exception:
        logger.exception("advanced_features: send_backup_to_channel error")
        return False


def cloud_backup_worker() -> None:
    """
    Background thread: runs every 24 hours and posts backup to channel.
    Start from main() — see Patch #5.
    """
    time.sleep(120)   # wait 2 min after startup before first attempt
    while True:
        try:
            create_database_backup = _g("create_database_backup")
            path = create_database_backup()
            send_backup_to_channel(path)
        except Exception:
            logger.exception("advanced_features: cloud_backup_worker error")
        time.sleep(24 * 60 * 60)


# ══════════════════════════════════════════════════════════════════════════════
#  FEATURE 4 — TARGETED BROADCAST
#  Admin can choose: ALL users | VIP (balance ≥ threshold) | by language code.
#  Uses the existing wait_for_broadcast_slot() / send_broadcast_request()
#  helpers from bot-3.py to stay inside TG rate limits.
# ══════════════════════════════════════════════════════════════════════════════

_BROADCAST_VIP_MIN_BALANCE = float(
    os.environ.get("BROADCAST_VIP_MIN_BALANCE", "10.0")
)


def _get_targeted_user_ids(target: str) -> list[str]:
    """
    target:  "all"          — every user in the DB
             "vip"          — users with balance ≥ _BROADCAST_VIP_MIN_BALANCE
             "lang_XX"      — users whose user_language = XX
    Returns a list of user_id strings.
    """
    db_file = _g("DB_FILE")
    conn = sqlite3.connect(db_file, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        if target == "all":
            rows = conn.execute(
                "SELECT user_id FROM users WHERE banned = 0 OR banned IS NULL"
            ).fetchall()
        elif target == "vip":
            rows = conn.execute(
                "SELECT user_id FROM users WHERE (banned = 0 OR banned IS NULL) "
                "AND balance >= ?",
                (_BROADCAST_VIP_MIN_BALANCE,),
            ).fetchall()
        elif target.startswith("lang_"):
            lang_code = target[5:].upper()
            rows = conn.execute(
                "SELECT user_id FROM users WHERE (banned = 0 OR banned IS NULL) "
                "AND UPPER(COALESCE(user_language,'EN')) = ?",
                (lang_code,),
            ).fetchall()
        else:
            rows = []
        return [str(r["user_id"]) for r in rows]
    finally:
        conn.close()


def broadcast_targeted(
    from_chat_id: int,
    message_id: int,
    target: str = "all",
) -> tuple[int, int, int]:
    """
    Copy a message (from_chat_id / message_id) to every user in the target
    cohort.  Returns (sent, failed, blocked).

    target values:  "all" | "vip" | "lang_BN" | "lang_AR" | ...
    """
    user_ids = _get_targeted_user_ids(target)
    logger.info(
        "advanced_features: targeted broadcast target=%s recipients=%s",
        target, len(user_ids),
    )
    wait_for_broadcast_slot = _g("wait_for_broadcast_slot")
    send_broadcast_request  = _g("send_broadcast_request")

    sent = failed = blocked = 0
    for uid in user_ids:
        try:
            wait_for_broadcast_slot()
            result = send_broadcast_request(
                "copyMessage",
                {
                    "chat_id":      uid,
                    "from_chat_id": from_chat_id,
                    "message_id":   message_id,
                },
            )
            ok = isinstance(result, dict) and result.get("ok")
            if ok:
                sent += 1
            else:
                desc = (result or {}).get("description", "")
                if "blocked" in desc.lower() or "bot was kicked" in desc.lower():
                    blocked += 1
                else:
                    failed += 1
        except Exception:
            failed += 1
    logger.info(
        "advanced_features: broadcast done sent=%s failed=%s blocked=%s",
        sent, failed, blocked,
    )
    return sent, failed, blocked


def build_broadcast_target_keyboard() -> dict:
    """Inline keyboard for choosing broadcast target in admin panel."""
    lang_buttons = [
        {"text": label, "callback_data": f"adv_bcast_lang_{code}"}
        for code, label in LANG_DISPLAY.items()
    ]
    return {
        "inline_keyboard": [
            [
                {"text": "📢 All Users",  "callback_data": "adv_bcast_all",
                 "style": "primary"},
                {"text": "⭐ VIP Only",  "callback_data": "adv_bcast_vip",
                 "style": "primary"},
            ],
            lang_buttons[:3],          # up to 3 language buttons per row
            lang_buttons[3:],
            [
                {"text": "🔙 Back",      "callback_data": "admin",
                 "style": "danger"},
            ],
        ]
    }


# ══════════════════════════════════════════════════════════════════════════════
#  FEATURE 5 — REFERRAL DISPLAY CARD
#  Call format_referral_card(user_id) to get the ready-to-send HTML string.
#  Wire it to /ref command — see Patch #3.
# ══════════════════════════════════════════════════════════════════════════════

def format_referral_card(user_id) -> str:
    """
    Build the referral info card for user_id.
    Returns an HTML string ready to pass to send_message().
    """
    uid = str(user_id)
    BOT_USERNAME = _g("BOT_USERNAME")
    bot_settings = _g("bot_settings")
    data_lock    = _g("data_lock")

    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"

    with data_lock:
        reward = round(float(bot_settings.get("refer_reward", 0.2)), 2)

    # Fetch totals from SQLite
    db_file = _g("DB_FILE")
    conn = sqlite3.connect(db_file, timeout=5)
    conn.execute("PRAGMA busy_timeout=5000")
    total_refers = 0
    balance = 0.0
    try:
        row = conn.execute(
            "SELECT total_refers, balance FROM users WHERE user_id = ?", (uid,)
        ).fetchone()
        if row:
            total_refers = int(row[0] or 0)
            balance      = round(float(row[1] or 0), 2)
    finally:
        conn.close()

    ANIMATED_BANNER = _g("ANIMATED_BANNER")
    gt = lambda key, **kw: get_text(user_id, key, **kw)   # noqa: E731

    lines = [
        f"🎁 <b>{gt('refer_header')}</b>",
        ANIMATED_BANNER,
        f"{gt('refer_link_label')}",
        f"<code>{ref_link}</code>",
        ANIMATED_BANNER,
        f"{gt('refer_total',  total=total_refers)}",
        f"{gt('refer_reward', reward=reward)}",
        f"{gt('balance',      balance=balance)}",
        ANIMATED_BANNER,
        "📲 <i>Share your link and earn every time a new user joins!</i>",
    ]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  FEATURE 6 — CALLBACK HANDLER (language picker + targeted broadcast flow)
#  Wire into handle_callback() — see Patch #4.
# ══════════════════════════════════════════════════════════════════════════════

# Temporary store for pending targeted broadcast: { admin_id: target_str }
_pending_bcast_target: dict[str, str] = {}
_pending_bcast_lock = threading.RLock()


def handle_advanced_callback(
    chat_id,
    message_id,
    callback_id,
    data: str,
    answer_callback_fn,
    is_admin_fn,
    safe_set_user_state_fn,
) -> bool:
    """
    Handles advanced_features callbacks.  Returns True if consumed.

    Callback data prefixes handled:
      setlang_XX          — user language change
      adv_bcast_all       — targeted broadcast: all
      adv_bcast_vip       — targeted broadcast: VIP
      adv_bcast_lang_XX   — targeted broadcast: language cohort
      adv_bcast_menu      — show targeted broadcast target picker
    """
    render_body_text = _g("render_body_text")
    send_message     = _g("send_message")
    edit_message     = _g("edit_message")
    ANIMATED_BANNER  = _g("ANIMATED_BANNER")

    # ── Language picker ──────────────────────────────────────────────────────
    if data.startswith("setlang_"):
        lang_code = data[8:].upper()
        if lang_code not in TRANSLATIONS:
            answer_callback_fn(callback_id, "❌ Unsupported language.", True)
            return True
        set_user_language(chat_id, lang_code)
        answer_callback_fn(callback_id)
        confirmation = get_text(chat_id, "lang_set")
        edit_message(chat_id, message_id, render_body_text(confirmation))
        return True

    # ── Admin: show targeted broadcast menu ──────────────────────────────────
    if data == "adv_bcast_menu":
        if not is_admin_fn(chat_id):
            answer_callback_fn(callback_id, "❌ No access.", True)
            return True
        answer_callback_fn(callback_id)
        prompt = get_text(chat_id, "choose_target")
        edit_message(
            chat_id, message_id,
            render_body_text(f"📤 <b>Targeted Broadcast</b>\n{ANIMATED_BANNER}\n{prompt}"),
            reply_markup=build_broadcast_target_keyboard(),
        )
        return True

    # ── Admin: set broadcast target and enter wait-for-message state ─────────
    if data in ("adv_bcast_all", "adv_bcast_vip") or data.startswith("adv_bcast_lang_"):
        if not is_admin_fn(chat_id):
            answer_callback_fn(callback_id, "❌ No access.", True)
            return True
        answer_callback_fn(callback_id)
        if data == "adv_bcast_all":
            target = "all"
            label  = "All Users"
        elif data == "adv_bcast_vip":
            target = "vip"
            label  = f"VIP (balance ≥ {_BROADCAST_VIP_MIN_BALANCE} TK)"
        else:
            lang   = data[len("adv_bcast_lang_"):]
            target = f"lang_{lang}"
            label  = LANG_DISPLAY.get(lang.upper(), lang.upper())

        with _pending_bcast_lock:
            _pending_bcast_target[str(chat_id)] = target

        safe_set_user_state_fn(chat_id, "adv_wait_for_bcast")
        edit_message(
            chat_id, message_id,
            render_body_text(
                f"📤 <b>Targeted Broadcast</b>\n{ANIMATED_BANNER}\n"
                f"Target: <b>{html.escape(label)}</b>\n\n"
                "Send the message you want to broadcast now.\n"
                "(Text, Photo, Video, File, etc.)"
            ),
            reply_markup={"inline_keyboard": [[
                {"text": "Cancel", "callback_data": "back_to_admin", "style": "danger"}
            ]]},
        )
        return True

    return False


def process_targeted_broadcast_message(chat_id, message_id_to_send) -> None:
    """
    Called from the message handler when state == 'adv_wait_for_bcast'.
    Runs the targeted broadcast in a background thread.
    """
    uid = str(chat_id)
    with _pending_bcast_lock:
        target = _pending_bcast_target.pop(uid, "all")

    send_message = _g("send_message")
    render_body_text = _g("render_body_text")

    def _run():
        sent, failed, blocked = broadcast_targeted(chat_id, message_id_to_send, target)
        result_text = get_text(
            chat_id, "broadcast_done",
            sent=sent, failed=failed,
        ) + f"\n🚫 Blocked: {blocked}"
        send_message(chat_id, render_body_text(result_text))

    threading.Thread(target=_run, name="adv-targeted-bcast", daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP HOOK
#  Call start_advanced_threads() from main() — see Patch #5.
# ══════════════════════════════════════════════════════════════════════════════

def start_advanced_threads() -> None:
    """Start the cloud backup worker thread."""
    if BACKUP_CHANNEL_ID:
        import threading as _t
        _t.Thread(
            target=cloud_backup_worker,
            name="adv-cloud-backup",
            daemon=True,
        ).start()
        logger.info(
            "advanced_features: cloud backup thread started → channel %s",
            BACKUP_CHANNEL_ID,
        )
    else:
        logger.info(
            "advanced_features: BACKUP_CHANNEL_ID not set; cloud backup disabled. "
            "Set it in Replit Secrets to enable."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SURGICAL PATCHES FOR bot-3.py
#  Apply these changes after adding the import block at the top of bot-3.py.
# ══════════════════════════════════════════════════════════════════════════════
"""
──────────────────────────────────────────────────────────────────────────────
IMPORT BLOCK  —  add at the top of bot-3.py (after analytics_addon imports)
──────────────────────────────────────────────────────────────────────────────
from advanced_features import (
    check_rate_limit,
    get_text, set_user_language, get_user_language,
    init_advanced_tables,
    send_backup_to_channel,
    broadcast_targeted,
    format_referral_card,
    handle_advanced_callback,
    process_targeted_broadcast_message,
    start_advanced_threads,
)

──────────────────────────────────────────────────────────────────────────────
PATCH #1  —  init_db()   (create user_language column)
Location : inside init_db(), right after the init_analytics_tables() call
──────────────────────────────────────────────────────────────────────────────
ADD:
        # advanced_features: add user_language column
        init_advanced_tables()

──────────────────────────────────────────────────────────────────────────────
PATCH #2  —  Rate limiter: top of _handle_message_impl() and handle_callback()
Location : very first lines of each function, before any state checks
──────────────────────────────────────────────────────────────────────────────
ADD at the top of _handle_message_impl(update):
    chat_id = update.get("message", {}).get("from", {}).get("id")
    if chat_id and not check_rate_limit(chat_id):
        return   # user is flooding — drop silently

ADD at the top of handle_callback(update):
    chat_id = update.get("callback_query", {}).get("from", {}).get("id")
    if chat_id and not check_rate_limit(chat_id):
        return

──────────────────────────────────────────────────────────────────────────────
PATCH #3  —  /ref command handler
Location : inside _handle_message_impl(), in the command-dispatch block
           (near the existing /start / /help handlers)
──────────────────────────────────────────────────────────────────────────────
ADD:
        elif text_lower == "/ref" or text_lower.startswith("/ref@"):
            card = format_referral_card(chat_id)
            send_message(chat_id, render_body_text(card))
            return

        elif text_lower in ("/setlang", "/language"):
            prompt = get_text(chat_id, "select_language")
            send_message(
                chat_id,
                render_body_text(f"🌐 <b>{prompt}</b>"),
                reply_markup=build_language_keyboard(),   # imported from advanced_features
            )
            return

──────────────────────────────────────────────────────────────────────────────
PATCH #4  —  Targeted broadcast message state handler
Location : inside _handle_message_impl(), in the state dispatch block
           (near the existing  elif state == "wait_for_broadcast":  block)
──────────────────────────────────────────────────────────────────────────────
ADD right before or after the wait_for_broadcast block:
        elif state == "adv_wait_for_bcast":
            safe_set_user_state(chat_id, None)
            process_targeted_broadcast_message(chat_id, update["message"]["message_id"])
            return

──────────────────────────────────────────────────────────────────────────────
PATCH #5  —  Wire advanced callbacks into handle_callback()
Location : inside handle_callback(), near the top of the admin if/elif chain
           (right after the handle_analytics_callback() block)
──────────────────────────────────────────────────────────────────────────────
ADD:
    # ── advanced_features callbacks ──────────────────────────────────────
    if handle_advanced_callback(
        chat_id, message_id, callback_id, data,
        answer_callback, is_admin, safe_set_user_state
    ):
        return

──────────────────────────────────────────────────────────────────────────────
PATCH #6  —  Admin Panel keyboard: add Targeted Broadcast button
Location : wherever the admin panel inline keyboard is built
           (look for  "broadcast_msg"  callback_data)
──────────────────────────────────────────────────────────────────────────────
ADD alongside or after the existing Broadcast button:
    {"text": "🎯 Targeted Blast", "callback_data": "adv_bcast_menu", "style": "primary"}

──────────────────────────────────────────────────────────────────────────────
PATCH #7  —  Cloud backup: hook into database_backup_worker
Location : inside database_backup_worker(), right after the
           _send_backup_to_owner(backup_path) call
──────────────────────────────────────────────────────────────────────────────
ADD:
            # advanced_features: also post to backup channel if configured
            send_backup_to_channel(backup_path)

──────────────────────────────────────────────────────────────────────────────
PATCH #8  —  main(): start cloud backup thread
Location : inside main(), right after the analytics_reporter thread.start()
──────────────────────────────────────────────────────────────────────────────
ADD:
    # ── advanced_features: cloud backup + i18n ready ─────────────────
    start_advanced_threads()
    logger.info("🚀 Advanced features (rate limiter, i18n, cloud backup, targeted broadcast) active.")

──────────────────────────────────────────────────────────────────────────────
ENVIRONMENT VARIABLES (add to Replit Secrets)
──────────────────────────────────────────────────────────────────────────────
BACKUP_CHANNEL_ID        — Telegram channel/group ID for cloud backups
                           (bot must be admin in that channel)
RATE_BUCKET_CAPACITY     — token bucket size per user (default: 10)
RATE_REFILL_RATE         — tokens refilled per second (default: 1.0)
BROADCAST_VIP_MIN_BALANCE — min balance to qualify as VIP for targeted blasts
                           (default: 10.0)
──────────────────────────────────────────────────────────────────────────────
"""
