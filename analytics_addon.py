# -*- coding: utf-8 -*-
"""
analytics_addon.py
==================
Drop this file next to bot-3.py and add ONE line at the top of bot-3.py:

    from analytics_addon import *

That is the only change required to the base file EXCEPT for the six
surgical patches documented at the bottom of this file.

Features
--------
  1. Daily analytics counters (total_otps, new_users, withdrawals) stored
     in SQLite — survives restarts, accumulates forever.
  2. Per-panel OTP performance tracker.
  3. Auto daily report sent to OWNER at midnight (configurable).
  4. Number stock alert — pings OWNER when stock drops below threshold.
  5. User growth chart (new user count per day, last 30 days).
  6. Panel logs export — which number came from which panel.
  7. INSTANT emergency backup on bot crash / SIGTERM / SIGINT / atexit
     — separate from the 6-hour scheduled backup.

Usage
-----
After `from analytics_addon import *` in bot-3.py, apply the six patches
listed at the very bottom of this file.  No other changes needed.
"""

import glob
import html
import json
import logging
import os
import shutil
import signal
import sqlite3
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Patch guard: these names must exist in the importing namespace ─────────
# (They come from bot-3.py globals; accessed after the module finishes loading)
_REQUIRED_GLOBALS = [
    "DB_FILE", "OWNER_ID", "INSTANCE_ID", "BASE_URL",
    "bot_settings", "data_lock", "sessions_lock",
    "number_batches", "user_active_sessions",
    "ANIMATED_BANNER", "render_body_text", "send_message",
    "send_document", "edit_message",
    "get_db", "create_database_backup",
]


def _bot_global(name):
    """Lazy resolver: pull a name from the importing module's global scope."""
    import sys
    for mod in sys.modules.values():
        if hasattr(mod, name) and hasattr(mod, "BOT_USERNAME"):
            return getattr(mod, name)
    raise RuntimeError(f"analytics_addon: could not resolve '{name}' from bot globals")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — DB MIGRATION
#  Call init_analytics_tables() once inside init_db() — see Patch #1 below.
# ══════════════════════════════════════════════════════════════════════════════

def init_analytics_tables():
    """Create analytics tables if they don't exist.  Safe to call every startup."""
    db_file = _bot_global("DB_FILE")
    conn = sqlite3.connect(db_file, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics_daily (
                date   TEXT NOT NULL,
                metric TEXT NOT NULL,
                value  REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (date, metric)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics_panel (
                date       TEXT NOT NULL,
                panel_name TEXT NOT NULL,
                otp_count  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (date, panel_name)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics_panel_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         REAL    NOT NULL,
                panel_name TEXT    NOT NULL,
                number     TEXT    NOT NULL,
                service    TEXT,
                iso        TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_anal_daily_date "
            "ON analytics_daily(date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_anal_panel_date "
            "ON analytics_panel(date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_anal_panlog_ts "
            "ON analytics_panel_log(ts)"
        )
        conn.commit()
        logger.info("Analytics tables initialised.")
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — COUNTERS
# ══════════════════════════════════════════════════════════════════════════════

_analytics_lock = threading.RLock()


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def analytics_increment(metric: str, amount: float = 1.0):
    """Atomically bump a daily metric.  Call from any thread."""
    date = _today()
    try:
        db_file = _bot_global("DB_FILE")
        conn = sqlite3.connect(db_file, timeout=10)
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            conn.execute(
                """
                INSERT INTO analytics_daily (date, metric, value) VALUES (?, ?, ?)
                ON CONFLICT(date, metric) DO UPDATE SET value = value + excluded.value
                """,
                (date, metric, amount),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("analytics_increment failed metric=%s", metric)


def analytics_increment_panel(panel_name: str, number: str = "",
                               service: str = "", iso: str = ""):
    """Track per-panel OTP delivery: daily aggregate + detailed log row."""
    date = _today()
    now = time.time()
    try:
        db_file = _bot_global("DB_FILE")
        conn = sqlite3.connect(db_file, timeout=10)
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            # Aggregate counter
            conn.execute(
                """
                INSERT INTO analytics_panel (date, panel_name, otp_count) VALUES (?, ?, 1)
                ON CONFLICT(date, panel_name) DO UPDATE SET otp_count = otp_count + 1
                """,
                (date, panel_name),
            )
            # Detailed log row (keep last 10 000 rows)
            conn.execute(
                "INSERT INTO analytics_panel_log (ts, panel_name, number, service, iso) "
                "VALUES (?, ?, ?, ?, ?)",
                (now, panel_name, number, service, iso),
            )
            conn.execute(
                "DELETE FROM analytics_panel_log WHERE id NOT IN "
                "(SELECT id FROM analytics_panel_log ORDER BY id DESC LIMIT 10000)"
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("analytics_increment_panel failed panel=%s", panel_name)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — REPORT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def get_analytics_report(days: int = 7) -> dict:
    """
    Returns:
      daily            : { "YYYY-MM-DD": {"total_otps": int, "new_users": int, ...} }
      panel_performance: { "PanelName": int_otp_count }
      totals           : { metric: cumulative_float }
      number_stock     : int  (live)
      active_sessions  : int  (live)
    """
    db_file = _bot_global("DB_FILE")
    number_batches = _bot_global("number_batches")
    user_active_sessions = _bot_global("user_active_sessions")
    data_lock = _bot_global("data_lock")
    sessions_lock = _bot_global("sessions_lock")

    conn = sqlite3.connect(db_file, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        rows = conn.execute(
            "SELECT date, metric, value FROM analytics_daily "
            "WHERE date >= date('now', ?) ORDER BY date DESC",
            (f"-{days} days",),
        ).fetchall()

        daily: dict = {}
        totals: dict = {}
        for row in rows:
            d, m, v = row["date"], row["metric"], row["value"]
            daily.setdefault(d, {})[m] = float(v)
            totals[m] = totals.get(m, 0.0) + float(v)

        panel_rows = conn.execute(
            "SELECT panel_name, SUM(otp_count) AS total FROM analytics_panel "
            "WHERE date >= date('now', ?) GROUP BY panel_name ORDER BY total DESC",
            (f"-{days} days",),
        ).fetchall()
        panel_perf = {r["panel_name"]: int(r["total"]) for r in panel_rows}
    finally:
        conn.close()

    # Live stock
    total_stock = 0
    with data_lock:
        for b in number_batches.values():
            total_stock += len(b.get("numbers", []))

    with sessions_lock:
        active_sess = len(user_active_sessions)

    return {
        "daily": daily,
        "panel_performance": panel_perf,
        "totals": totals,
        "number_stock": total_stock,
        "active_sessions": active_sess,
    }


def format_analytics_message(report: dict, period_label: str = "Last 7 Days") -> str:
    """Render the analytics report as Telegram HTML."""
    try:
        ANIMATED_BANNER = _bot_global("ANIMATED_BANNER")
    except Exception:
        ANIMATED_BANNER = "─" * 8

    totals = report.get("totals", {})
    panel_perf = report.get("panel_performance", {})
    daily = report.get("daily", {})

    daily_new = [
        f"  📅 {d}: <b>{int(v.get('new_users', 0))}</b>"
        for d, v in sorted(daily.items(), reverse=True)[:14]
        if v.get("new_users", 0) > 0
    ]

    panel_lines = []
    for i, (pname, cnt) in enumerate(
        sorted(panel_perf.items(), key=lambda x: -x[1])
    ):
        medal = ["🥇", "🥈", "🥉"][i] if i < 3 else "   •"
        panel_lines.append(
            f"  {medal} <b>{html.escape(str(pname))}</b>: {int(cnt)} OTPs"
        )

    lines = [
        f"📊 <b>ADMIN ANALYTICS</b> — {html.escape(period_label)}",
        ANIMATED_BANNER,
        f"📨 Total OTPs:        <b>{int(totals.get('total_otps', 0))}</b>",
        f"👤 New Users:         <b>{int(totals.get('new_users', 0))}</b>",
        f"💸 Withdrawals:       <b>{int(totals.get('withdrawals', 0))}</b>",
        f"🔐 Active Sessions:   <b>{report.get('active_sessions', 0)}</b>",
        f"📦 Number Stock:      <b>{report.get('number_stock', 0)}</b>",
        ANIMATED_BANNER,
    ]

    if panel_lines:
        lines.append("🏆 <b>Panel Performance:</b>")
        lines.extend(panel_lines)
        lines.append(ANIMATED_BANNER)

    if daily_new:
        lines.append("📈 <b>New Users / Day:</b>")
        lines.extend(daily_new)
        lines.append(ANIMATED_BANNER)

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — STOCK ALERT
# ══════════════════════════════════════════════════════════════════════════════

def get_stock_alert_threshold() -> int:
    bot_settings = _bot_global("bot_settings")
    data_lock = _bot_global("data_lock")
    with data_lock:
        return int(bot_settings.get("stock_alert_threshold", 50))


_last_stock_alert_time = 0.0
_stock_alert_cooldown = 3600.0   # ping owner at most once per hour


def check_and_alert_low_stock():
    global _last_stock_alert_time
    now = time.time()
    if now - _last_stock_alert_time < _stock_alert_cooldown:
        return
    threshold = get_stock_alert_threshold()
    number_batches = _bot_global("number_batches")
    data_lock = _bot_global("data_lock")
    total_stock = 0
    with data_lock:
        for b in number_batches.values():
            total_stock += len(b.get("numbers", []))
    if total_stock < threshold:
        _last_stock_alert_time = now
        try:
            render_body_text = _bot_global("render_body_text")
            send_message = _bot_global("send_message")
            OWNER_ID = _bot_global("OWNER_ID")
            send_message(
                OWNER_ID,
                render_body_text(
                    f"⚠️ <b>LOW NUMBER STOCK ALERT</b>\n"
                    f"📦 Current Stock: <b>{total_stock}</b>\n"
                    f"🔔 Alert Below: <b>{threshold}</b>\n"
                    f"➡️ Please upload more numbers!"
                ),
            )
        except Exception:
            logger.exception("Could not send low-stock alert")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — AUTO-REPORT WORKER THREAD
# ══════════════════════════════════════════════════════════════════════════════

_ANALYTICS_INTERVAL = 24 * 60 * 60   # 24 hours
_analytics_report_lock = threading.RLock()
_analytics_last_sent = 0.0


def _send_analytics_report_now(period: str = "Last 7 Days", days: int = 7):
    global _analytics_last_sent
    with _analytics_report_lock:
        now = time.time()
        if now - _analytics_last_sent < _ANALYTICS_INTERVAL - 30:
            return
        _analytics_last_sent = now
    try:
        report = get_analytics_report(days=days)
        msg = format_analytics_message(report, period_label=period)
        render_body_text = _bot_global("render_body_text")
        send_message = _bot_global("send_message")
        OWNER_ID = _bot_global("OWNER_ID")
        send_message(OWNER_ID, render_body_text(msg))
        logger.info("Analytics daily report sent to owner.")
    except Exception:
        logger.exception("Could not send analytics report")


def analytics_report_worker():
    """Background thread: wait 90 s after startup, then send daily."""
    time.sleep(90)
    _send_analytics_report_now(period="Last 7 Days", days=7)
    while True:
        time.sleep(_ANALYTICS_INTERVAL)
        _send_analytics_report_now(period="Last 7 Days", days=7)
        check_and_alert_low_stock()


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — ADMIN CALLBACK HANDLERS
#  These are called from handle_callback() in bot-3.py — see Patch #4 below.
# ══════════════════════════════════════════════════════════════════════════════

def handle_analytics_callback(chat_id, message_id, callback_id, data,
                               answer_callback_fn, is_admin_fn):
    """
    Handles:  adm_analytics | analytics_7 | analytics_30 | analytics_export
    Returns True if the callback was consumed, False otherwise.
    """
    if data not in ("adm_analytics", "analytics_7", "analytics_30", "analytics_export"):
        return False

    if not is_admin_fn(chat_id):
        answer_callback_fn(callback_id, "❌ No access.", True)
        return True

    render_body_text = _bot_global("render_body_text")
    send_message      = _bot_global("send_message")
    edit_message      = _bot_global("edit_message")
    send_document     = _bot_global("send_document")
    ANIMATED_BANNER   = _bot_global("ANIMATED_BANNER")
    db_file           = _bot_global("DB_FILE")

    nav_kb = {"inline_keyboard": [
        [{"text": "📅 Last 7 Days",   "callback_data": "analytics_7",
          "style": "primary"},
         {"text": "📆 Last 30 Days",  "callback_data": "analytics_30",
          "style": "primary"}],
        [{"text": "📤 Export Logs",   "callback_data": "analytics_export",
          "style": "danger"},
         {"text": "🔙 Back",          "callback_data": "admin",
          "style": "danger"}],
    ]}

    if data in ("adm_analytics", "analytics_7", "analytics_30"):
        answer_callback_fn(callback_id)
        days  = 30 if data == "analytics_30" else 7
        label = f"Last {days} Days"
        try:
            report   = get_analytics_report(days=days)
            msg_text = format_analytics_message(report, label)
            edit_message(chat_id, message_id,
                         render_body_text(msg_text), reply_markup=nav_kb)
        except Exception as exc:
            send_message(chat_id,
                         render_body_text(f"❌ Analytics error: {html.escape(str(exc))}"))
        return True

    if data == "analytics_export":
        answer_callback_fn(callback_id, "📤 Generating…")
        try:
            conn = sqlite3.connect(db_file, timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                daily_rows = conn.execute(
                    "SELECT date, metric, value FROM analytics_daily "
                    "ORDER BY date DESC"
                ).fetchall()
                panel_rows = conn.execute(
                    "SELECT date, panel_name, otp_count FROM analytics_panel "
                    "ORDER BY date DESC"
                ).fetchall()
                log_rows = conn.execute(
                    "SELECT ts, panel_name, number, service, iso "
                    "FROM analytics_panel_log ORDER BY ts DESC LIMIT 500"
                ).fetchall()
            finally:
                conn.close()

            lines = ["=== DAILY METRICS ==="]
            for r in daily_rows:
                lines.append(f"{r['date']} | {r['metric']} | {r['value']}")
            lines.append("\n=== PANEL PERFORMANCE ===")
            for r in panel_rows:
                lines.append(f"{r['date']} | {r['panel_name']} | {r['otp_count']} OTPs")
            lines.append("\n=== PANEL LOG (last 500) ===")
            for r in log_rows:
                stamp = datetime.fromtimestamp(r["ts"]).strftime("%Y-%m-%d %H:%M:%S")
                lines.append(
                    f"{stamp} | {r['panel_name']} | {r['number']} | "
                    f"{r['service']} | {r['iso']}"
                )

            content = "\n".join(lines).encode("utf-8")
            fname = f"analytics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            send_document(chat_id, fname, content)
        except Exception as exc:
            send_message(chat_id,
                         render_body_text(f"❌ Export error: {html.escape(str(exc))}"))
        return True

    return False


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — EMERGENCY BACKUP ON CRASH / SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

_shutdown_backup_sent = False
_shutdown_backup_lock = threading.Lock()


def _emergency_backup_and_notify(reason: str = "shutdown"):
    """
    Fires once per process, as early as possible after any termination signal
    or unhandled exception.  Builds a fresh SQLite snapshot and pushes it to
    the owner's Telegram inbox so no data is lost when the VPS/RDP shuts down.
    """
    global _shutdown_backup_sent
    with _shutdown_backup_lock:
        if _shutdown_backup_sent:
            return
        _shutdown_backup_sent = True

    logger.warning(
        "Emergency backup triggered reason=%s pid=%s", reason, os.getpid()
    )

    try:
        create_database_backup = _bot_global("create_database_backup")
        backup_path = create_database_backup()
        # Let any in-flight writes settle
        time.sleep(0.5)
    except Exception:
        logger.exception("Emergency backup: could not create DB snapshot")
        return

    try:
        BASE_URL  = _bot_global("BASE_URL")
        OWNER_ID  = _bot_global("OWNER_ID")
        INSTANCE_ID = _bot_global("INSTANCE_ID")

        with open(backup_path, "rb") as fh:
            content = fh.read()

        stamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fname    = (
            f"EMERGENCY_{reason}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        )
        emoji_map = {
            "shutdown": "🔴", "sigterm": "⚡",
            "sigint":   "🛑", "crash":   "💥",
        }
        caption = (
            f"{emoji_map.get(reason, '⚠️')} <b>EMERGENCY BACKUP</b>\n"
            f"📌 Reason: <b>{reason.upper()}</b>\n"
            f"📅 Time: {stamp}\n"
            f"📦 Size: {len(content) // 1024} KB\n"
            f"🤖 Instance: <code>{INSTANCE_ID}</code>\n"
            f"⚠️ <b>Bot may be offline — check your server!</b>"
        )
        import requests as _req
        _req.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": OWNER_ID, "caption": caption, "parse_mode": "HTML"},
            files={"document": (fname, content, "application/octet-stream")},
            timeout=30,
        )
        logger.info(
            "Emergency backup sent to owner (%s KB) reason=%s",
            len(content) // 1024, reason,
        )
    except Exception:
        logger.exception("Emergency backup: could not send file to owner")


def setup_emergency_backup_hooks():
    """
    Wire _emergency_backup_and_notify into every possible exit path.
    Call once from main() — see Patch #5 below.
    """
    import atexit

    # 1. Clean exit
    atexit.register(lambda: _emergency_backup_and_notify("shutdown"))

    # 2. SIGTERM (Docker / systemd / VPS stop)
    def _sigterm(signum, frame):
        _emergency_backup_and_notify("sigterm")
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGTERM)

    try:
        signal.signal(signal.SIGTERM, _sigterm)
    except (OSError, ValueError):
        pass

    # 3. SIGINT (Ctrl-C)
    def _sigint(signum, frame):
        _emergency_backup_and_notify("sigint")
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGINT, _sigint)
    except (OSError, ValueError):
        pass

    # 4. Unhandled main-thread crash (chain onto existing hook)
    _prev_excepthook = __import__("sys").excepthook

    def _crash_excepthook(exc_type, exc_value, tb):
        _emergency_backup_and_notify("crash")
        _prev_excepthook(exc_type, exc_value, tb)

    __import__("sys").excepthook = _crash_excepthook

    # 5. Unhandled background thread crash
    import threading as _threading
    _prev_thread_hook = _threading.excepthook

    def _thread_crash(args):
        _emergency_backup_and_notify("crash")
        _prev_thread_hook(args)

    _threading.excepthook = _thread_crash

    logger.info(
        "Emergency backup hooks installed "
        "(atexit + SIGTERM + SIGINT + excepthook + threading.excepthook)"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  SURGICAL PATCHES FOR bot-3.py
#  Apply these 6 changes manually after adding `from analytics_addon import *`
#  at the very top of bot-3.py (just below the existing imports block).
# ══════════════════════════════════════════════════════════════════════════════
"""
──────────────────────────────────────────────────────────────────────────────
PATCH #1  —  init_db()  (add analytics tables)
Location : inside init_db(), just before the final  conn.commit()  call
           (~line 1697 in the original file)
──────────────────────────────────────────────────────────────────────────────
ADD these two lines right before `conn.commit()` at the end of init_db():

        # Analytics tables (added by analytics_addon)
        init_analytics_tables()

──────────────────────────────────────────────────────────────────────────────
PATCH #2  —  OTP delivery  (count OTPs + per-panel log)
Location : inside process_panel_otp_item(), after the block that updates
           `cursor.execute('UPDATE users SET total_otps = total_otps + 1 …')`
           (~line 7070-7074)
──────────────────────────────────────────────────────────────────────────────
ADD after the `_firebase_increment_user(owner_id, total_otps=1)` line:

            # ── analytics ──
            analytics_increment("total_otps")
            panel_name_for_log = p.get("name", "Unknown") if isinstance(p, dict) else "Unknown"
            analytics_increment_panel(
                panel_name_for_log,
                number=str(num),
                service=str(app_full_name),
                iso=str(iso),
            )

Note: `p` is the panel dict passed into process_panel_otp_item(p, item).
If the function signature is process_panel_otp_item(panel, item), replace `p`
with `panel`.

──────────────────────────────────────────────────────────────────────────────
PATCH #3  —  New user registration  (count new users)
Location : inside _handle_message_impl(), right after:
               `is_new_user = register_user_local(chat_id)`
           (~line 9097)
──────────────────────────────────────────────────────────────────────────────
ADD immediately after that line:

    if is_new_user:
        analytics_increment("new_users")

──────────────────────────────────────────────────────────────────────────────
PATCH #4  —  Withdrawal completion  (count withdrawals)
Location : find the block where a withdrawal request is saved to SQLite and
           the withdrawal group is notified.  Look for `req_id = f"W_"` then
           the `conn.execute("INSERT INTO withdrawals …")` call.
           Right after `conn.commit()` in that block add:

    analytics_increment("withdrawals")

──────────────────────────────────────────────────────────────────────────────
PATCH #5  —  Admin callback router  (wire the analytics UI)
Location : inside handle_callback() (or wherever admin callbacks are routed),
           find the big if/elif chain that checks `data ==`.
           ADD this block near the top of the admin section:

    # ── Analytics UI (analytics_addon) ──────────────────────────────
    if handle_analytics_callback(
        chat_id, message_id, callback_id, data,
        answer_callback, is_admin
    ):
        return

──────────────────────────────────────────────────────────────────────────────
PATCH #6  —  main()  (start threads + install crash hooks)
Location : inside main(), right after the lines that start
           `database_backup_worker` and `panel_monitor_thread`.
──────────────────────────────────────────────────────────────────────────────
ADD:

    # ── Emergency backup hooks (analytics_addon) ─────────────────────
    setup_emergency_backup_hooks()

    # ── Daily analytics report thread (analytics_addon) ──────────────
    threading.Thread(
        target=analytics_report_worker,
        name="analytics-reporter",
        daemon=True,
    ).start()
    logger.info("📊 Analytics reporter + emergency backup hooks active.")

──────────────────────────────────────────────────────────────────────────────
PATCH #7  —  Admin Panel keyboard  (add Analytics button)
Location : find the function that builds the admin panel inline keyboard
           (look for `"adm_panels"`, `"adm_system"`, `"adm_users"` etc.).
           Add one more button in an appropriate row:

    {"text": "📊 Analytics", "callback_data": "adm_analytics", "style": "primary"}

──────────────────────────────────────────────────────────────────────────────
"""
