# -*- coding: utf-8 -*-
# ── analytics_addon: drop analytics_addon.py next to this file ──
from analytics_addon import (
    init_analytics_tables,
    analytics_increment,
    analytics_increment_panel,
    handle_analytics_callback,
    setup_emergency_backup_hooks,
    analytics_report_worker,
)
from advanced_features import (
    check_rate_limit,
    get_text,
    set_user_language,
    get_user_language,
    init_advanced_tables,
    send_backup_to_channel,
    broadcast_targeted,
    format_referral_card,
    handle_advanced_callback,
    process_targeted_broadcast_message,
    start_advanced_threads,
    build_language_keyboard,
    build_broadcast_target_keyboard,
    get_all_menu_texts,
)
import requests
import time
import json
import os
import sys
import sqlite3
import heapq
import itertools
import queue
try:
    import fcntl  # POSIX only (Linux/Replit); absent on Windows
except ImportError:
    fcntl = None
import uuid
import threading
import random
import re
import html
import unicodedata
import pyotp
import logging
import math
import copy
import atexit
import glob
import hashlib
import shutil
from logging.handlers import RotatingFileHandler
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    firebase_admin = None
    credentials = None
    firestore = None
from collections import Counter, OrderedDict
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import quote, urljoin, urlsplit

# ==========================================
# Logging Setup
# ==========================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# Firebase Setup
# ==========================================
db = None
if firebase_admin is None:
    logger.warning("firebase-admin is not installed; Firebase sync is disabled.")
else:
    try:
        firebase_credential = None
        firebase_service_account = os.environ.get(
            "FIREBASE_SERVICE_ACCOUNT_JSON", ""
        ).strip()
        if firebase_service_account:
            firebase_credentials = json.loads(firebase_service_account)
            firebase_credential = credentials.Certificate(firebase_credentials)
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            firebase_file_candidates = [
                os.path.join(script_dir, "firebase_account.json"),
                os.path.join(os.getcwd(), "firebase_account.json"),
            ]
            firebase_file = next(
                (
                    path for path in dict.fromkeys(firebase_file_candidates)
                    if os.path.isfile(path)
                ),
                None,
            )
            if firebase_file:
                with open(firebase_file, "r", encoding="utf-8") as account_file:
                    firebase_credentials = json.load(account_file)
                firebase_credential = credentials.Certificate(firebase_credentials)
                logger.info("Firebase credentials loaded from firebase_account.json.")
            else:
                logger.info(
                    "firebase_account.json and FIREBASE_SERVICE_ACCOUNT_JSON "
                    "are unavailable; Firebase sync is disabled and SQLite remains active."
                )

        if firebase_credential is not None:
            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app(firebase_credential)
            db = firestore.client()
            logger.info("Firebase connected; Firestore sync enabled.")
    except Exception as e:
        logger.error(
            "Firebase initialization failed; continuing with SQLite only (%s).",
            type(e).__name__,
        )

# ==========================================
# Configuration (Environment Variables Recommended)
# ==========================================
TOKEN = os.environ.get("BOT_TOKEN", "8532458339:AAEtZ4XlfUWx3sQ1R6GQqy5Th-AiQWdB_ro").strip()
if not TOKEN:
    raise RuntimeError(
        "BOT_TOKEN environment variable is required. "
        "Add BOT_TOKEN to Replit Secrets before starting the bot."
    )
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
FILE_URL = f"https://api.telegram.org/file/bot{TOKEN}/"

try:
    owner_id_value = os.environ.get("OWNER_ID", "7525798243").strip()
    if not owner_id_value:
        raise ValueError("missing")
    OWNER_ID = int(owner_id_value)
except (TypeError, ValueError) as exc:
    raise RuntimeError(
        "OWNER_ID is required and must be a numeric Telegram user ID. "
        "Add OWNER_ID to Replit Secrets before starting the bot."
    ) from exc
BOT_USERNAME = "A2pPremiumBazar1_bot"


def _redact_secret(value):
    text = str(value)
    return text.replace(TOKEN, "[REDACTED]") if TOKEN else text


def _redact_log_value(value):
    """Redact strings in log arguments without changing numeric types."""
    if isinstance(value, str):
        return _redact_secret(value)
    if isinstance(value, tuple):
        return tuple(_redact_log_value(item) for item in value)
    if isinstance(value, list):
        return [_redact_log_value(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _redact_log_value(item)
            for key, item in value.items()
        }
    return value


class _SecretRedactionFilter(logging.Filter):
    def filter(self, record):
        if TOKEN:
            if isinstance(record.msg, str):
                record.msg = _redact_secret(record.msg)
            record.args = _redact_log_value(record.args)
        return True


logger.addFilter(_SecretRedactionFilter())
DB_FILE = os.environ.get(
    "BOT_DB_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_data.db"),
)

_log_directory = os.path.dirname(os.path.abspath(DB_FILE))
os.makedirs(_log_directory, exist_ok=True)
_file_handler = RotatingFileHandler(
    os.path.join(_log_directory, "bot.log"),
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
))
logger.addHandler(_file_handler)


def _restore_database_backup_if_missing():
    """Restore the newest local SQLite backup before init_db creates an empty DB."""
    if os.path.exists(DB_FILE):
        return False
    backup_directory = DB_FILE + ".backups"
    backups = sorted(
        glob.glob(os.path.join(backup_directory, "bot_backup_*.db")),
        key=os.path.getmtime,
        reverse=True,
    )
    for backup_path in backups:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(DB_FILE)), exist_ok=True)
            shutil.copy2(backup_path, DB_FILE)
            logger.warning(
                "Primary database was missing; restored the newest available SQLite backup: %s",
                backup_path,
            )
            return True
        except OSError:
            logger.exception("Could not restore SQLite backup %s", backup_path)
    return False


def _warn_if_database_path_is_ephemeral():
    """Warn when SQLite is configured in a known temporary filesystem."""
    db_path = os.path.abspath(DB_FILE)
    ephemeral_roots = (
        "/tmp", "/var/tmp", "/dev/shm", "/run", "/run/user",
        "/home/runner/.cache",
    )
    for root in ephemeral_roots:
        try:
            if os.path.commonpath((db_path, root)) == root:
                logger.warning(
                    "BOT_DB_FILE points to non-persistent storage (%s); "
                    "number stock and user data may be lost on redeploy. "
                    "Set BOT_DB_FILE to persistent storage.",
                    DB_FILE,
                )
                return
        except ValueError:
            continue


_restore_database_backup_if_missing()
_warn_if_database_path_is_ephemeral()

# ==========================================
# Panel credential encryption (Fernet) + log redaction
# ==========================================
try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover - depends on the environment
    Fernet = None
    InvalidToken = Exception

_ENC_PREFIX = "enc:v1:"
_PANEL_SECRET_KEY_RE = re.compile(
    r"(?i)(pass(word|wd)?|token|secret|api_?key|cookie|session|auth)")
_PANEL_URL_KEYS = {"api_url", "full_api_url", "login_url", "msg_link", "url"}
_URL_SECRET_QS_RE = re.compile(
    r"(?i)[?&](token|key|api_?key|apikey|secret|password|passwd|pass|auth)=[^&\s]+")
_KNOWN_SECRET_VALUES = set()
_known_secret_lock = threading.Lock()
_DATA_KEY_FILE = os.path.join(_log_directory, ".data_key")
_LOCAL_DATA_KEY_WAS_GENERATED = False
_ENCRYPTED_DATA_KEY_WAS_REGENERATED = False
_DATA_ENCRYPTION_KEY_ALERT_SENT = False
_LEGACY_ENCRYPTED_DATA_ALERT_SENT = False


def _contains_encrypted_marker(value):
    if isinstance(value, str):
        return _ENC_PREFIX in value
    if isinstance(value, dict):
        return any(_contains_encrypted_marker(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_encrypted_marker(item) for item in value)
    return False


def _database_has_encrypted_panel_credentials():
    """Check SQLite panel and pending-interaction data without creating a database."""
    if not os.path.isfile(DB_FILE):
        return False
    database_uri = "file:" + quote(os.path.abspath(DB_FILE), safe="/") + "?mode=ro"
    try:
        connection = sqlite3.connect(database_uri, uri=True, timeout=5)
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            if "settings" in tables:
                row = connection.execute(
                    "SELECT value FROM settings WHERE key = 'panels' LIMIT 1"
                ).fetchone()
                if row and _ENC_PREFIX in str(row[0]):
                    return True
            if "pending_admin_interactions" in tables:
                row = connection.execute(
                    "SELECT 1 FROM pending_admin_interactions "
                    "WHERE data LIKE ? LIMIT 1",
                    (f"%{_ENC_PREFIX}%",),
                ).fetchone()
                if row:
                    return True
        finally:
            connection.close()
    except sqlite3.Error as exc:
        logger.warning(
            "Could not inspect the existing database for encrypted panel credentials (%s).",
            type(exc).__name__,
        )
    return False


def _read_local_fernet_key():
    try:
        with open(_DATA_KEY_FILE, "rb") as key_file:
            raw_key = key_file.read().strip()
        if not raw_key:
            raise ValueError("empty key file")
        return Fernet(raw_key)
    except Exception as exc:
        logger.error(
            "Could not load the local data-encryption key (%s).",
            type(exc).__name__,
        )
        return None


def _load_fernet():
    global _LOCAL_DATA_KEY_WAS_GENERATED, _ENCRYPTED_DATA_KEY_WAS_REGENERATED
    if Fernet is None:
        logger.error(
            "The 'cryptography' package is missing; panel credentials cannot be protected."
        )
        return None

    raw = os.environ.get("DATA_ENCRYPTION_KEY", "").strip()
    if raw:
        try:
            return Fernet(raw.encode("ascii"))
        except (UnicodeEncodeError, ValueError):
            logger.error(
                "DATA_ENCRYPTION_KEY is invalid; trying the local data-key file."
            )

    if os.path.exists(_DATA_KEY_FILE):
        return _read_local_fernet_key()

    old_encrypted_data_found = _database_has_encrypted_panel_credentials()
    generated_key = Fernet.generate_key()
    try:
        descriptor = os.open(
            _DATA_KEY_FILE,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as key_file:
            os.fchmod(key_file.fileno(), 0o600)
            key_file.write(generated_key)
            key_file.flush()
            os.fsync(key_file.fileno())
    except FileExistsError:
        # Another process may have created the persistent key concurrently.
        return _read_local_fernet_key()
    except OSError as exc:
        logger.error(
            "Could not create the local data-encryption key (%s).",
            type(exc).__name__,
        )
        try:
            os.remove(_DATA_KEY_FILE)
        except OSError:
            pass
        return None

    _LOCAL_DATA_KEY_WAS_GENERATED = True
    if old_encrypted_data_found:
        _ENCRYPTED_DATA_KEY_WAS_REGENERATED = True
        logger.warning(
            "A new data-encryption key was generated while previously encrypted "
            "panel credentials were found; saved panel passwords can no longer "
            "be read and must be entered again."
        )
    else:
        logger.info("A persistent local data-encryption key was generated.")
    return Fernet(generated_key)


_FERNET = _load_fernet()


def _is_secret_field(key, value):
    if not isinstance(value, str) or not value:
        return False
    key = str(key)
    if value.startswith(_ENC_PREFIX):
        return _PANEL_SECRET_KEY_RE.search(key) is not None or key.lower() == "key" or key in _PANEL_URL_KEYS
    if _PANEL_SECRET_KEY_RE.search(key) or key.lower() == "key":
        return True
    return key in _PANEL_URL_KEYS and bool(_URL_SECRET_QS_RE.search(value))


def _register_secret_value(value):
    if isinstance(value, str) and len(value) >= 4 and not value.startswith(_ENC_PREFIX):
        with _known_secret_lock:
            _KNOWN_SECRET_VALUES.add(value)


def encrypt_secret_value(value):
    if not isinstance(value, str) or not value or value.startswith(_ENC_PREFIX):
        return value
    if _FERNET is None:
        return value
    return _ENC_PREFIX + _FERNET.encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_secret_value(value):
    if not isinstance(value, str) or not value.startswith(_ENC_PREFIX):
        return value
    if _FERNET is None:
        logger.error("Encrypted panel credential found but DATA_ENCRYPTION_KEY is unavailable.")
        return value
    try:
        return _FERNET.decrypt(value[len(_ENC_PREFIX):].encode("ascii")).decode("utf-8")
    except (InvalidToken, ValueError):
        logger.error("A stored credential could not be decrypted (wrong DATA_ENCRYPTION_KEY?).")
        return value


def encrypt_secret_fields(record):
    """Return a copy of *record* with secret-looking string fields encrypted."""
    if not isinstance(record, dict):
        return record
    out = dict(record)
    for key, value in record.items():
        if _is_secret_field(key, value):
            _register_secret_value(value)
            out[key] = encrypt_secret_value(value)
    return out


def decrypt_secret_fields(record):
    """Decrypt in place. Returns True when plaintext secrets were found (migration needed)."""
    plaintext_found = False
    if not isinstance(record, dict):
        return False
    for key, value in list(record.items()):
        if not _is_secret_field(key, value):
            continue
        if value.startswith(_ENC_PREFIX):
            record[key] = decrypt_secret_value(value)
        else:
            plaintext_found = True
        _register_secret_value(record[key])
    return plaintext_found


def encrypt_panels_for_storage(panels):
    if not isinstance(panels, list):
        return panels
    return [encrypt_secret_fields(p) for p in panels]


def decrypt_panels_in_place(panels):
    found = False
    if isinstance(panels, list):
        for p in panels:
            found = decrypt_secret_fields(p) or found
    return found


def encrypt_interaction_data(data):
    """Encrypt secrets in a pending admin-interaction payload (incl. p_data)."""
    if not isinstance(data, dict):
        return data
    out = encrypt_secret_fields(data)
    if isinstance(out.get("p_data"), dict):
        out["p_data"] = encrypt_secret_fields(out["p_data"])
    return out


def decrypt_interaction_data(data):
    if isinstance(data, dict):
        decrypt_secret_fields(data)
        if isinstance(data.get("p_data"), dict):
            decrypt_secret_fields(data["p_data"])
    return data


def scrub_sqlite_secrets(path, force_vacuum=False):
    """Encrypt panel/interaction secrets inside a SQLite file (live DB or old backup)."""
    if _FERNET is None or not os.path.exists(path):
        return False
    changed = False
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            try:
                row = conn.execute("SELECT value FROM settings WHERE key = 'panels'").fetchone()
                if row and row[0]:
                    panels = json.loads(row[0])
                    if isinstance(panels, list):
                        new_panels = encrypt_panels_for_storage(panels)
                        if new_panels != panels:
                            conn.execute(
                                "UPDATE settings SET value = ? WHERE key = 'panels'",
                                (json.dumps(new_panels),),
                            )
                            changed = True
            except (sqlite3.Error, ValueError):
                logger.debug("Ignored exception in scrub_sqlite_secrets (line 317)", exc_info=True)
            try:
                for user_id, raw in conn.execute(
                    "SELECT user_id, data FROM pending_admin_interactions"
                ).fetchall():
                    try:
                        data = json.loads(raw)
                    except (TypeError, ValueError):
                        logger.debug("Ignored exception in scrub_sqlite_secrets (line 325)", exc_info=True)
                        continue
                    new_data = encrypt_interaction_data(data)
                    if new_data != data:
                        conn.execute(
                            "UPDATE pending_admin_interactions SET data = ? WHERE user_id = ?",
                            (json.dumps(new_data, ensure_ascii=False), user_id),
                        )
                        changed = True
            except sqlite3.Error:
                logger.debug("Ignored exception in scrub_sqlite_secrets (line 334)", exc_info=True)
            conn.commit()
            if changed or force_vacuum:
                conn.execute("VACUUM")  # drop plaintext left in free pages
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    logger.debug("Ignored exception in scrub_sqlite_secrets (line 341)", exc_info=True)
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not scrub credentials in %s", os.path.basename(path))
    return changed


def scrub_old_backups():
    backup_directory = DB_FILE + ".backups"
    for path in glob.glob(os.path.join(backup_directory, "bot_backup_*.db")):
        scrub_sqlite_secrets(path, force_vacuum=True)


_SECRET_QS_LOG_RE = re.compile(
    r"(?i)((?<![A-Za-z])(?:token|api_?key|apikey|secret|password|passwd|pass|auth|key)=)([^&\s\"'<>]+)")
_SECRET_JSON_LOG_RE = re.compile(
    r"(?i)([\"']?(?:password|passwd|token|api_?key|secret)[\"']?\s*[:=]\s*[\"'])([^\"']+)([\"'])")


def redact_for_log(text):
    text = str(text)
    with _known_secret_lock:
        known = sorted(_KNOWN_SECRET_VALUES, key=len, reverse=True)
    if TOKEN:
        known.append(TOKEN)
    for secret in known:
        if secret and secret in text:
            text = text.replace(secret, "[REDACTED]")
    text = _SECRET_QS_LOG_RE.sub(lambda m: m.group(1) + "[REDACTED]", text)
    text = _SECRET_JSON_LOG_RE.sub(lambda m: m.group(1) + "[REDACTED]" + m.group(3), text)
    return text


class _RedactingFormatter(logging.Formatter):
    def format(self, record):
        return redact_for_log(super().format(record))


class _StructuredRedactingFormatter(logging.Formatter):
    """Emit one redacted JSON object per log event for easier filtering."""
    def format(self, record):
        event = {
            "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "process": record.process,
            "thread": record.threadName,
            "message": record.getMessage(),
        }
        if record.exc_info:
            event["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            event["stack"] = self.formatStack(record.stack_info)
        return redact_for_log(json.dumps(event, ensure_ascii=False, default=str))


for _handler in list(logging.getLogger().handlers) + list(logger.handlers):
    _handler.setFormatter(_StructuredRedactingFormatter())


INSTANCE_ID = uuid.uuid4().hex[:12]
PROCESS_STARTED_AT = time.time()
_instance_lock_handle = None
_instance_lock_guard = threading.RLock()
_owner_error_alert_lock = threading.RLock()
_last_owner_error_alert = 0.0
OWNER_ERROR_ALERT_INTERVAL = 300
_polling_conflict_alert_sent = False
_polling_conflict_lock = threading.RLock()
last_polling_error = "none"
last_poll_at = 0.0
last_get_number_click = {}
get_number_debounce_lock = threading.RLock()
GET_NUMBER_DEBOUNCE_SECONDS = 1.0
backup_lock = threading.RLock()
LAST_BACKUP_TIME = 0.0


def acquire_single_instance_lock():
    """Acquire an exclusive process lock next to the SQLite database."""
    global _instance_lock_handle
    with _instance_lock_guard:
        if _instance_lock_handle is not None:
            return True
        if fcntl is None:
            logger.warning(
                "fcntl is unavailable on this platform; the single-instance lock is "
                "disabled. Make sure only one bot process runs per database."
            )
            return True
        lock_path = DB_FILE + ".lock"
        os.makedirs(os.path.dirname(os.path.abspath(lock_path)), exist_ok=True)
        handle = open(lock_path, "a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            logger.error(
                "Single-instance lock unavailable for database %s; another bot "
                "process is already polling (pid=%s).",
                DB_FILE,
                os.getpid(),
            )
            raise SystemExit(
                "Another bot instance already holds the SQLite polling lock."
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} instance_id={INSTANCE_ID}\n")
        handle.flush()
        _instance_lock_handle = handle
        logger.info(
            "Acquired single-instance lock pid=%s instance_id=%s db=%s",
            os.getpid(),
            INSTANCE_ID,
            DB_FILE,
        )
        return True


def release_single_instance_lock():
    global _instance_lock_handle
    with _instance_lock_guard:
        if _instance_lock_handle is None:
            return
        try:
            fcntl.flock(_instance_lock_handle.fileno(), fcntl.LOCK_UN)
            _instance_lock_handle.close()
        except OSError:
            logger.exception("Could not release the polling instance lock")
        finally:
            _instance_lock_handle = None


atexit.register(release_single_instance_lock)


_owner_error_alert_keys = {}


def notify_owner_error(summary, key=None):
    """Rate-limit concise owner notifications for unexpected handler errors.

    Without ``key`` all alerts share one global cooldown.  With ``key`` the
    cooldown is tracked per key, so unrelated problems (for example two
    different forward groups) are not hidden behind each other.
    """
    global _last_owner_error_alert
    now = time.monotonic()
    with _owner_error_alert_lock:
        if key is None:
            if now - _last_owner_error_alert < OWNER_ERROR_ALERT_INTERVAL:
                return
            _last_owner_error_alert = now
        else:
            last_alert = _owner_error_alert_keys.get(key)
            if last_alert is not None and now - last_alert < OWNER_ERROR_ALERT_INTERVAL:
                return
            _owner_error_alert_keys[key] = now
    safe_summary = html.escape(str(summary or "Unexpected bot error")[:300])
    try:
        send_message(
            OWNER_ID,
            f"⚠️ <b>Bot error</b>\n<code>{safe_summary}</code>\n"
            f"Instance: <code>{INSTANCE_ID}</code>",
        )
    except Exception:
        logger.exception("Could not send the owner error alert")


def _thread_exception_logger(args):
    logger.critical(
        "Unhandled background thread exception in %s",
        getattr(args.thread, "name", "unnamed"),
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )
    try:
        notify_owner_error("Unhandled background thread exception")
    except Exception:
        logger.exception("Could not notify owner about a thread exception")


def _process_exception_logger(exc_type, exc_value, traceback):
    logger.critical(
        "Unhandled main-thread exception",
        exc_info=(exc_type, exc_value, traceback),
    )


threading.excepthook = _thread_exception_logger
sys.excepthook = _process_exception_logger


# One condition-driven scheduler replaces unbounded per-callback Timer threads.
_scheduler_condition = threading.Condition()
_scheduled_tasks = []
_scheduled_sequence = itertools.count()
_scheduler_thread = None


def _scheduler_loop():
    while True:
        with _scheduler_condition:
            while not _scheduled_tasks:
                _scheduler_condition.wait()
            due_at, _, callback, args = _scheduled_tasks[0]
            wait_for = due_at - time.monotonic()
            if wait_for > 0:
                _scheduler_condition.wait(wait_for)
                continue
            heapq.heappop(_scheduled_tasks)
        try:
            callback(*args)
        except Exception:
            logger.exception("Scheduled task failed")


def schedule_once(delay_seconds, callback, *args):
    """Schedule bounded one-shot work on the shared scheduler thread."""
    global _scheduler_thread
    with _scheduler_condition:
        if _scheduler_thread is None or not _scheduler_thread.is_alive():
            _scheduler_thread = threading.Thread(
                target=_scheduler_loop,
                name="bot-scheduler",
                daemon=True,
            )
            _scheduler_thread.start()
        heapq.heappush(
            _scheduled_tasks,
            (time.monotonic() + max(0.0, float(delay_seconds)),
             next(_scheduled_sequence), callback, args),
        )
        _scheduler_condition.notify()


# ==========================================
# Premium Emoji Database
# ==========================================
PEM = {
    "ok": '<tg-emoji emoji-id="5352694861990501856">✅</tg-emoji>',
    "no": '<tg-emoji emoji-id="5420130255174145507">❌</tg-emoji>',
    "warn": '<tg-emoji emoji-id="5336944168944047463">⚠️</tg-emoji>',
    "admin": '<tg-emoji emoji-id="5353032893096567467">📊</tg-emoji>',
    "user": '<tg-emoji emoji-id="5352861489541714456">👤</tg-emoji>',
    "file": '<tg-emoji emoji-id="5352721946054268944">📁</tg-emoji>',
    "rocket": '<tg-emoji emoji-id="5352597830089347330">🚀</tg-emoji>',
    "graph": '<tg-emoji emoji-id="5352877703043258544">📊</tg-emoji>',
    "money": '<tg-emoji emoji-id="5348469219761626211">💸</tg-emoji>',
    "gift": '<tg-emoji emoji-id="5420396762189831222">🎁</tg-emoji>',
    "msg": '<tg-emoji emoji-id="5337302974806922068">💬</tg-emoji>',
    "gear": '<tg-emoji emoji-id="5420155432272438703">⚙️</tg-emoji>',
    "link": '<tg-emoji emoji-id="5420517437885943844">🔗</tg-emoji>',
    "trash": '<tg-emoji emoji-id="5422557736330106570">🗑</tg-emoji>',
    "upload": '<tg-emoji emoji-id="5353001161878182134">📤</tg-emoji>',
    "world": '<tg-emoji emoji-id="5336972142066047577">🌐</tg-emoji>',
    "lock": '<tg-emoji emoji-id="5353022963132174959">🔐</tg-emoji>',
    "phone": '<tg-emoji emoji-id="5337132498965010628">📱</tg-emoji>',
    "num": '<tg-emoji emoji-id="5352862640592949843">🔢</tg-emoji>',
    "pin": '<tg-emoji emoji-id="5352922460897452503">📍</tg-emoji>',
    "star": '<tg-emoji emoji-id="5352552689983067014">✨</tg-emoji>',
    "hi": '<tg-emoji emoji-id="5353027129250453493">👋</tg-emoji>',
    "waiting": '<tg-emoji emoji-id="6053193097276298985">⌛</tg-emoji>',
    # Animated premium emoji supplied for number-loading states.
    "loading": '<tg-emoji emoji-id="6071051768861562054">🔄</tg-emoji>',
    # IDs supplied in premium_emoji_full_guide.txt.  Keep these separate from
    # the existing IDs so an existing emoji is never registered twice.
    "premium": '<tg-emoji emoji-id="6314396206306958399">✨</tg-emoji>',
    "crown": '<tg-emoji emoji-id="6311888503751843904">👑</tg-emoji>'
}

CHANGE_NUMBER_BUTTON_EMOJI_ID = "6053387526150823179"
NEW_NUMBER_REQUEST_EMOJI_ID = "6312314362644143742"

GLOBAL_BODY_EMOJIS = {
    "➖": "5870818207383686839", "🚫": "5334807341109908955", "😒": "5334763399299506604",
    "🖥": "5334880948259427772", "🌐": "5334590977837403844", "🌟": "5337102391244263212",
    "🕓": "5336983442125001376", "⌛": "5337172996211648018", "💬": "5337302974806922068",
    "🔐": "5337255927735163754", "🍏": "5337132498965010628", "❔": "5336850036145823599",
    "⚠️": "5336944168944047463", "🔥": "5337267511261960341", "💸": "5348469219761626211",
    "🥚": "5348390922507817684", "👨‍⚖": "5334763399299506604", "🐁": "5348494358205207761",
    "🧻": "5348486915026884464", "⚗": "5346311574221000149", "🛴": "5348075478634766440",
    "📊": "5353032893096567467", "🔢": "5352862640592949843", "👤": "5352861489541714456",
    "📁": "5352721946054268944", "🚀": "5352597830089347330", "💎": "5352838545826420397",
    "📍": "5352922460897452503", "👋": "5353027129250453493", "✅": "5352694861990501856",
    "1️⃣": "5352651766288652742", "2️⃣": "5355186458418257716", "3️⃣": "5352867219028091093",
    "4️⃣": "5352566657216714037", "5️⃣": "5353086880835474989", "6️⃣": "5354859211975071385",
    "7️⃣": "5352859127309707652", "8️⃣": "5352957533600389988", "9️⃣": "5353060913463204207",
    "🔤": "5352727417842606016", "📣": "5352980533150259581", "📤": "5353001161878182134",
    "✨": "5352552689983067014", "🔹": "5352638632278660622", "🎙": "5355102594886833928",
    "💴": "5352985330628730418", "📅": "5352585194295564660", "📴": "5352974971167611327",
    "✏️": "5395444784611480792", "📱": "5337132498965010628", "🔗": "5420517437885943844",
    "❌": "5420130255174145507", "⚙️": "5420155432272438703", "🫂": "5420145051336485498",
    "➕": "5420323438508155202", "🗑": "5422557736330106570", "🎁": "5420396762189831222",
    "➤": "5420618897898381296", "🏢": "5420156334215565595", "💳": "5190899075968441286",
    "📝": "5192739271886282680", "🛡": "5190447043545438788", "🤝": "5192805934073685937",
    "💰": "5190576863226933563", "👀": "5190645917711114179", "🕹": "5193100774988617665",
    "🟢": "5192812028632274956", "🧪": "5190781475468915802", "🎨": "5190751148704833975",
    "📂": "5257969839313526622", "🌍": "5780471598922337683", "📌": "5318986077455795572",
    "📢": "5789428375261023681", "🆔": "5352862640592949843", "📈": "5352877703043258544",
    "🔔": "5352980533150259581", "🏦": "5348469219761626211", "🧾": "5192739271886282680",
    "👨‍⚖️": "5334763399299506604", "🔍": "5463352748751753567",
    "🔑": "5197288647275071607"
}

# `➖` is converted to the premium animated banner emoji by
# `render_body_text`.  Keep this as text here so it also works inside
# configurable/custom messages.
ANIMATED_BANNER = "➖" * 8


def fmt_money(value):
    try:
        v = round(float(value) + 1e-9, 2)
    except (TypeError, ValueError):
        return "0"
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return s or "0"


DEFAULT_CUSTOM_MESSAGES = {
    "start": {"text": f"{PEM['crown']} <b>NUMBER BOT</b> {PEM['crown']}\n{ANIMATED_BANNER}\n🚀 <b>Welcome to Number &amp; OTP Service</b>\n\n✅ Choose an option below to continue.\n{ANIMATED_BANNER}\n{PEM['premium']} <b>Premium OTP Service</b>", "buttons": []},
    "get_number": {"text": f"{PEM['pin']} Select a service:", "buttons": []},
    "select_country": {"text": "📌 Select a country for {service}:", "buttons": []}, 
    "search_number": {"text": f"🔍 <b>SEARCH NUMBER</b>\n{ANIMATED_BANNER}\n✅ Enter 3 to 9 digits to search for a number.\n\n📝 <b>Examples:</b>\n➥ 880\n➥ 9227373\n{ANIMATED_BANNER}\n⚡ Fast Number Lookup", "buttons": []},
    "traffic": {"text": f"{PEM['graph']} <b>Traffic Overview</b>\n\n{PEM['ok']} Available Numbers: {{avail}}\n{PEM['rocket']} Assigned Numbers: {{assigned}}", "buttons": []},
    "refer": {"text": f"➖➖➖➖➖➖➖\n« {PEM['gift']} REFER & EARN »\n➖➖➖➖➖➖➖\n{PEM['link']} YOUR LINK:\n<code>{{ref_link}}</code>\n➖➖➖➖➖➖➖\n{PEM['user']} TOTAL REFERS: <b>{{total_ref}}</b>\n➖➖➖➖➖➖➖\n{PEM['money']} PER REFER: <b>{{ref_reward}} TK</b>\n➖➖➖➖➖➖➖", "buttons": []},
    "withdrawal": {"text": f"💳 <b>WITHDRAWAL</b>\n{ANIMATED_BANNER}\n👋 Total OTP: {{total_otp}}\n🤝 Total Referrals: {{total_ref}}\n💰 Balance: {{bal}} TK\n🔐 Minimum: {{min_w}} TK\n{ANIMATED_BANNER}\nSelect a withdrawal method:", "buttons": []},
    "support": {"text": f"{PEM['msg']} Contact us for any help:", "buttons": []}
}

# ==========================================
# Thread-safe Data Structures
# ==========================================
data_lock = threading.RLock()
user_states_lock = threading.RLock()
temp_data_lock = threading.RLock()
cooldowns_lock = threading.RLock()
sessions_lock = threading.RLock()

# ==========================================
# Global Variables & Settings
# ==========================================
bot_settings = {
    "admins": [OWNER_ID],
    "panels": [], 
    "fw_groups": [], 
    "otp_inline_buttons": [],
    "otp_link": "https://t.me/+W8T473952bA1MDg1",
    "withdraw_on": True,
    "min_withdraw": 30.0,
    "otp_reward": 0.1,
    "refer_reward": 0.2,
    "cooldown": 10,
    "num_req": 3,
    # Number delivery controls.  `num_req` is kept as a legacy alias for
    # older installations, while the System menu exposes these two settings.
    "number_limit": 3,
    "change_number_cooldown": 3,
    "num_share": 1, 
    "support_link": "https://t.me/+SZ4ovBicpPRiNjZl",
    "w_methods": ["bKash", "Nagad"],
    "w_group": "", 
    
    "fj_on": False,
    "fj_channels": [], 
    "disabled_services": [],
    "premium_flags": {
        "US": {"char": "🇺🇸", "iso": "US", "code": "1", "name": "United States", "id": "5913463998522592692"},
        "BD": {"char": "🇧🇩", "iso": "BD", "code": "880", "name": "Bangladesh", "id": "5911365056594973179"},
        "IN": {"char": "🇮🇳", "iso": "IN", "code": "91", "name": "India", "id": "5913754823643107921"},
        "PK": {"char": "🇵🇰", "iso": "PK", "code": "92", "name": "Pakistan", "id": "5913705895375672082"},
        "GB": {"char": "🇬🇧", "iso": "GB", "code": "44", "name": "United Kingdom", "id": "5913443365499703513"}
    },
    "premium_apps": {
        "FACEBOOK": {"char": "🚫", "id": "5334807341109908955", "name": "Facebook"},
        "WHATSAPP": {"char": "🚫", "id": "5334759662677957452", "name": "WhatsApp"}
    },
    "custom_messages": copy.deepcopy(DEFAULT_CUSTOM_MESSAGES),
    # Dynamic OTP reward per "SERVICE::COUNTRY" key (e.g. "WHATSAPP::BD").
    # Falls back to global otp_reward when a key is absent.
    "service_prices": {}
}

FS_KEYS = [
    "admins", "panels", "fw_groups", "otp_link", "withdraw_on", 
    "min_withdraw", "otp_reward", "refer_reward", "cooldown", 
    "num_req", "number_limit", "change_number_cooldown", "num_share", "support_link", "w_methods", "w_group", "disabled_services",
    "fj_on", "fj_channels", "otp_inline_buttons", "service_prices"
]
FORCE_JOIN_JOINED_CACHE_SECONDS = 60
FORCE_JOIN_MISSING_CACHE_SECONDS = 3
PENDING_NUMBER_UPLOAD_STATES = {
    "wait_for_txt",
    "wait_for_service",
    "wait_for_country",
}
PENDING_UPLOAD_TTL_SECONDS = 30 * 60
ADMIN_INTERACTION_TTL_SECONDS = 30 * 60
ADMIN_INTERACTION_STATES = {
    "wait_for_emoji_extract", "wait_for_emoji_details",
    "wait_for_flag_txt", "wait_for_app_txt",
    "wait_for_cpanel_url", "wait_for_cpanel_user", "wait_for_cpanel_pass",
    "wait_for_cpanel_msg_link", "wait_for_cpanel_num_col_name",
    "wait_for_cpanel_num_col_idx", "wait_for_cpanel_msg_col_name",
    "wait_for_cpanel_msg_col_idx", "wait_for_um_bal_uid",
    "wait_for_um_bal_amt", "wait_for_um_ban_uid", "wait_for_um_prof_uid",
    "wait_for_menu_text", "wait_for_menu_btn", "wait_for_test_service",
    "wait_for_test_number", "wait_for_test_otp", "wait_for_test_lang",
    "wait_for_add_wm", "wait_for_add_fj", "wait_for_fj_invite",
    "wait_for_add_adm", "wait_for_add_fw_id", "wait_for_add_fw_btn",
    "wait_for_otp_link", "wait_for_oib_add", "wait_for_oib_edit", "wait_for_panel_name", "wait_for_p_api",
    "wait_for_p_tok", "wait_for_p_fapi", "wait_for_p_rec",
    "set_number_setting", "set_WAO", "set_service_price",
}
PERSISTED_INTERACTION_STATES = (
    ADMIN_INTERACTION_STATES
    | PENDING_NUMBER_UPLOAD_STATES
    | {
        "wait_for_broadcast",
        "wait_for_2fa_key",
        "wait_for_search",
        "wait_for_withdraw_amount",
        "wait_for_withdraw_number",
    }
)
MAX_NUMBER_UPLOAD_BYTES = 10 * 1024 * 1024

number_batches = {}
used_numbers_list = []
known_numbers = set()

# ==========================================
# Bounded in-memory state (idle TTL + max size)
# ==========================================
STATE_TTL_SECONDS = 6 * 60 * 60   # idle entries disappear after 6 hours


class TTLDict(MutableMapping):
    """Thread-safe dict that drops idle entries (TTL) and caps its size (LRU).

    Any read or write of a key refreshes its idle timer.  `can_evict(value)` can
    veto an eviction (e.g. a lock that is currently held) and `on_evict(value)`
    runs cleanup (e.g. closing an HTTP session).
    """

    def __init__(self, name, ttl=STATE_TTL_SECONDS, max_size=10000,
                 can_evict=None, on_evict=None, sweep_interval=60.0):
        self.name = name
        self.ttl = ttl
        self.max_size = max_size
        self._can_evict = can_evict
        self._on_evict = on_evict
        self._sweep_interval = sweep_interval
        self._data = OrderedDict()
        self._touched = {}
        self._lock = threading.RLock()
        self._last_sweep = time.monotonic()

    def _touch(self, key):
        self._touched[key] = time.monotonic()
        self._data.move_to_end(key)

    def _evict(self, key):
        value = self._data.get(key)
        if self._can_evict is not None:
            try:
                if not self._can_evict(value):
                    return False
            except Exception:
                return False
        self._data.pop(key, None)
        self._touched.pop(key, None)
        if self._on_evict is not None:
            try:
                self._on_evict(value)
            except Exception:
                logging.getLogger(__name__).debug("TTLDict(%s) cleanup failed", self.name, exc_info=True)
        return True

    def sweep(self, force=False):
        now = time.monotonic()
        with self._lock:
            if not force and now - self._last_sweep < self._sweep_interval and len(self._data) <= self.max_size:
                return
            self._last_sweep = now
            for key in list(self._data):          # oldest first
                if now - self._touched.get(key, now) <= self.ttl:
                    break
                self._evict(key)
            if len(self._data) > self.max_size:
                for key in list(self._data):
                    if len(self._data) <= self.max_size:
                        break
                    self._evict(key)

    def __getitem__(self, key):
        with self._lock:
            value = self._data[key]
            self._touch(key)
            return value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value
            self._touch(key)
            self.sweep()

    def __delitem__(self, key):
        with self._lock:
            del self._data[key]
            self._touched.pop(key, None)

    def __iter__(self):
        with self._lock:
            return iter(list(self._data))

    def __len__(self):
        with self._lock:
            return len(self._data)

    def __contains__(self, key):
        with self._lock:
            return key in self._data

    def get(self, key, default=None):
        with self._lock:
            if key in self._data:
                self._touch(key)
                return self._data[key]
            return default

    def setdefault(self, key, default=None):
        with self._lock:
            if key in self._data:
                self._touch(key)
                return self._data[key]
            self[key] = default
            return default

    def __repr__(self):
        return f"TTLDict({self.name!r}, size={len(self)})"


def _lock_is_idle(lock):
    """A per-chat lock may only be dropped while nobody holds it."""
    if lock is None:
        return True
    if lock.acquire(blocking=False):
        lock.release()
        return True
    return False


def _close_quietly(resource):
    close = getattr(resource, "close", None)
    if callable(close):
        close()

dirty_number_batches = set()
deleted_number_batches = set()
pending_used_numbers = []
pending_number_usage = []
total_uploaded_stats = 0
total_assigned_stats = 0
# Bounded FIFO of already-forwarded OTP keys (oldest first).
PROCESSED_OTPS_LIMIT = 5000
processed_otps = OrderedDict()


def _phone_number_digits(value):
    return re.sub(r"\D", "", str(value or ""))


def _canonical_otp_key(number, otp):
    number_digits = _phone_number_digits(number)
    otp_code = str(otp or "").strip()
    if not number_digits or not otp_code:
        return None
    return f"{number_digits}|{otp_code}"


def _canonicalize_processed_otp_id(value):
    """Convert current and legacy persisted keys to number-digits + OTP."""
    raw = str(value or "")
    if "|" in raw:
        parts = raw.split("|")
        if len(parts) >= 2:
            return _canonical_otp_key(parts[0], parts[1]) or raw
    if "_" in raw:
        number, otp = raw.rsplit("_", 1)
        return _canonical_otp_key(number, otp) or raw
    return raw


recent_traffic = []
user_banned_cache = {}
user_banned_cache_lock = threading.RLock()
force_join_cache = {}
force_join_cache_lock = threading.RLock()
force_join_warning_lock = threading.RLock()
force_join_warning_keys = set()
force_join_admin_status_cache = {}
force_join_admin_status_cache_lock = threading.RLock()
force_join_invite_creation_cache = {}
force_join_invite_creation_lock = threading.RLock()
force_join_metadata_cache = {}
force_join_metadata_cache_lock = threading.RLock()
force_join_bot_id_cache = {"id": None, "username": None, "time": 0.0}
force_join_bot_id_lock = threading.RLock()
inventory_index_cache = None
inventory_index_lock = threading.RLock()
inventory_cache_version = 0
service_screen_cache = None
country_screen_cache = {}
user_active_sessions = {}
ACTIVE_USER_SESSION_TTL_SECONDS = 30 * 60
CALLBACK_PAYLOAD_TTL_SECONDS = 6 * 60 * 60
CALLBACK_PAYLOAD_MAX_ENTRIES = 50000
_callback_payload_counter = itertools.count()
_callback_payload_lock = threading.RLock()
_callback_payloads = {}
user_states = TTLDict("user_states", max_size=50000)
temp_data = TTLDict("temp_data", max_size=50000)
admin_prompt_messages = TTLDict("admin_prompt_messages", max_size=10000)
admin_interaction_updated_at = TTLDict("admin_interaction_updated_at", max_size=10000)
expired_admin_interaction_chats = []
expired_interaction_notifications = set()
expired_interaction_notifications_lock = threading.RLock()
admin_interaction_context = threading.local()
user_cooldowns = TTLDict("user_cooldowns", max_size=100000)
pending_withdrawals = {}
panel_sessions = TTLDict("panel_sessions", max_size=200, on_evict=_close_quietly)
handler_locks = TTLDict("handler_locks", max_size=50000, can_evict=_lock_is_idle)
handler_locks_guard = threading.RLock()
pending_update_ids = set()
pending_update_ids_lock = threading.RLock()
chat_update_queues = {}
active_chat_workers = set()
chat_update_queue_lock = threading.RLock()
startup_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bot-startup")
startup_tasks_lock = threading.RLock()


def _base36(value):
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    value = max(0, int(value))
    if value == 0:
        return "0"
    out = ""
    while value:
        value, rem = divmod(value, 36)
        out = alphabet[rem] + out
    return out


def _store_callback_payload(kind, *values):
    """Map arbitrary labels to compact callback IDs below Telegram's 64-byte cap."""
    token = "x" + _base36(next(_callback_payload_counter))
    now = time.monotonic()
    with _callback_payload_lock:
        for old_token, (expiry, _kind, _values) in list(_callback_payloads.items()):
            if expiry <= now:
                _callback_payloads.pop(old_token, None)
        while len(_callback_payloads) >= CALLBACK_PAYLOAD_MAX_ENTRIES:
            _callback_payloads.pop(next(iter(_callback_payloads)))
        _callback_payloads[token] = (
            now + CALLBACK_PAYLOAD_TTL_SECONDS,
            str(kind),
            tuple(str(value) if value is not None else None for value in values),
        )
    return token


def _get_callback_payload(token, expected_kind=None):
    now = time.monotonic()
    with _callback_payload_lock:
        item = _callback_payloads.get(str(token))
        if not item:
            return None
        expiry, kind, values = item
        if expiry <= now or (expected_kind and kind != expected_kind):
            _callback_payloads.pop(str(token), None)
            return None
        return values


def _is_short_callback_token(value):
    return bool(re.fullmatch(r"x[0-9a-z]+", str(value or "")))


def _valid_force_join_invite_url(url):
    """Accept only HTTPS Telegram public-channel or invite links."""
    try:
        parsed = urlsplit(str(url or "").strip())
    except ValueError:
        return False
    if parsed.scheme != "https" or parsed.hostname not in {
        "t.me", "www.t.me", "telegram.me", "www.telegram.me"
    }:
        return False
    path = parsed.path.strip("/")
    return bool(
        re.fullmatch(r"\+[A-Za-z0-9_-]{5,}", path)
        or re.fullmatch(r"joinchat/[A-Za-z0-9_-]{5,}", path)
        or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", path)
    )


def normalize_force_join_channel(value):
    """Normalize old string entries and new channel records to one shape."""
    source = value if isinstance(value, dict) else {"id": str(value or "").strip()}
    channel_id = str(source.get("id", source.get("chat_id", "")) or "").strip()
    title = str(source.get("title", "") or "").strip()
    username = str(source.get("username", "") or "").strip().lstrip("@")
    invite_link = str(source.get("invite_link", "") or "").strip()

    if not isinstance(value, dict):
        raw = channel_id
        if raw.startswith(("https://", "http://")):
            try:
                path = urlsplit(raw).path.strip("/")
            except ValueError:
                path = ""
            if _valid_force_join_invite_url(raw):
                if path.startswith("+") or path.startswith("joinchat/"):
                    invite_link = raw
                    channel_id = ""
                else:
                    username = path.split("/", 1)[0]
                    channel_id = "@" + username
            else:
                channel_id = ""
        elif raw.startswith("@"):
            username = raw[1:]
            channel_id = "@" + username
        elif raw and not raw.startswith("-") and not raw.isdigit():
            username = raw
            channel_id = "@" + username
    elif (
        channel_id
        and not username
        and not channel_id.startswith("-")
        and not channel_id.isdigit()
    ):
        username = channel_id.lstrip("@")
        channel_id = "@" + username

    if username:
        username = username.lstrip("@")
        if not channel_id:
            channel_id = "@" + username
    elif channel_id.startswith("@"):
        username = channel_id[1:]
    if not title:
        title = username or channel_id or "Unknown channel"
    if invite_link and not _valid_force_join_invite_url(invite_link):
        invite_link = ""
    return {
        "id": channel_id,
        "title": title,
        "username": username,
        "invite_link": invite_link,
        "bot_is_admin": source.get("bot_is_admin"),
    }


def force_join_channel_key(channel):
    channel = normalize_force_join_channel(channel)
    return (
        channel["id"],
        channel["title"],
        channel["username"],
        channel["invite_link"],
    )


def invalidate_force_join_cache():
    with force_join_cache_lock:
        force_join_cache.clear()
    with force_join_admin_status_cache_lock:
        force_join_admin_status_cache.clear()
    with force_join_metadata_cache_lock:
        force_join_metadata_cache.clear()


def get_handler_lock(chat_id):
    """Serialize updates for one chat without blocking unrelated users."""
    with handler_locks_guard:
        return handler_locks.setdefault(chat_id, threading.RLock())


def is_http_url(value):
    try:
        parsed = urlsplit(str(value or "").strip())
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.hostname)
    except ValueError:
        return False


# ==========================================
# OTP Group inline buttons (global BOT / CHANNEL ...)  -- START
# ==========================================
OTP_COPY_ICON_ID = "6312105464024800161"
# Show the premium copy icon on the full-width group OTP copy button.
GROUP_OTP_COPY_ICON = True
OTP_INLINE_DEFAULT_EMOJI_IDS = {          # button name (case-insensitive) -> premium emoji
    "BOT": "6314599345375158239",
    "CHANNEL": "6312301920123887895",
}


def _otp_inline_url_key(url):
    return str(url or "").strip().lower().rstrip("/")


def normalize_otp_inline_button(raw):
    """Return a clean {"id","text","url","emoji_id","enabled"} dict or None."""
    if not isinstance(raw, dict):
        return None
    text = re.sub(r"\s+", " ", str(raw.get("text") or "")).strip()
    url = str(raw.get("url") or "").strip()
    if not text or len(text) > 64 or not is_http_url(url):
        return None
    emoji_id = str(raw.get("emoji_id") or "").strip()
    if not re.fullmatch(r"\d{5,25}", emoji_id):
        emoji_id = ""
    bid = re.sub(r"[^A-Za-z0-9]", "", str(raw.get("id") or ""))[:16]
    if not bid:
        bid = hashlib.md5(f"{text}|{url}".encode("utf-8")).hexdigest()[:8]
    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        enabled = str(enabled).strip().lower() not in {"false", "0", "no", "off", ""}
    return {"id": bid, "text": text, "url": url, "emoji_id": emoji_id, "enabled": enabled}


def normalize_otp_inline_buttons(raw_list):
    out, seen_ids = [], set()
    for raw in raw_list if isinstance(raw_list, list) else []:
        item = normalize_otp_inline_button(raw)
        if not item:
            continue
        base, n = item["id"], 1
        while item["id"] in seen_ids:
            n += 1
            item["id"] = f"{base}{n}"
        seen_ids.add(item["id"])
        out.append(item)
    return out


def otp_inline_button_emoji_id(btn):
    """Admin-added emoji first; otherwise the built-in one for BOT / CHANNEL; else None."""
    eid = str(btn.get("emoji_id") or "").strip()
    if eid:
        return eid
    return OTP_INLINE_DEFAULT_EMOJI_IDS.get(str(btn.get("text") or "").strip().upper())


def get_enabled_otp_inline_buttons():
    with data_lock:
        items = list(bot_settings.get("otp_inline_buttons") or [])
    return [b for b in (normalize_otp_inline_button(i) for i in items) if b and b["enabled"]]


def _otp_copy_button(otp, with_icon=True, shield=False):
    otp = str(otp)
    label = f"🛡️ {otp}" if shield else otp
    btn = {"text": label, "copy_text": {"text": otp}, "style": "success"}
    if with_icon:
        btn["icon_custom_emoji_id"] = OTP_COPY_ICON_ID
    return btn


def _otp_display_value(otp):
    """Remove separators only from digit-only grouped OTPs used in group buttons."""
    value = str(otp)
    candidate = value.strip()
    if re.fullmatch(r"(?:\d{3}\s*-\s*\d{3}|\d{2}\s*-\s*\d{2}\s*-\s*\d{2})", candidate):
        return re.sub(r"[-\s]", "", candidate)
    return value

def build_group_otp_keyboard(fw, otp):
    """Put OTP copy on its own row, followed by link buttons two per row."""
    extra, seen = [], set()
    for btn in get_enabled_otp_inline_buttons():
        key = _otp_inline_url_key(btn["url"])
        if key in seen:
            continue
        seen.add(key)
        b_obj = {"text": btn["text"], "url": btn["url"], "style": "primary"}
        eid = otp_inline_button_emoji_id(btn)
        if eid:
            b_obj["icon_custom_emoji_id"] = eid
        extra.append(b_obj)
    legacy = []
    for btn in (fw.get("buttons") if isinstance(fw, dict) else None) or []:
        if not isinstance(btn, dict) or not btn.get("text") or not btn.get("url"):
            continue
        key = _otp_inline_url_key(btn["url"])
        if key in seen:
            extra = [e for e in extra if _otp_inline_url_key(e["url"]) != key]   # keep the legacy one
        seen.add(key)
        b_obj = {"text": btn["text"], "url": btn["url"], "style": "primary"}
        if "icon_custom_emoji_id" in btn:
            b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
        legacy.append(b_obj)
    extra += legacy

    copy_btn = _otp_copy_button(_otp_display_value(otp), with_icon=GROUP_OTP_COPY_ICON)
    kb = [[copy_btn]]
    for i in range(0, len(extra), 2):
        kb.append(extra[i:i + 2])
    return {"inline_keyboard": kb}


def build_inbox_otp_keyboard(otp):
    """Assigned-number Inbox: shield + copy icon + OTP; never add group links."""
    return {"inline_keyboard": [[_otp_copy_button(otp, with_icon=True, shield=True)]]}


def split_otp_inline_button_input(text):
    """"NAME - https://url" -> (name, url) or None.

    Splits at the first " - "; if that does not give a valid URL (a name that
    itself contains " - "), splits at the last http/https link instead.
    """
    raw = str(text or "").strip()
    if " - " in raw:
        name, url = raw.split(" - ", 1)
        name, url = name.strip(), url.strip()
        if name and is_http_url(url):
            return name, url
    matches = list(re.finditer(r"https?://\S+", raw, re.I))
    if matches:
        m = matches[-1]
        name = raw[:m.start()].strip().rstrip("-\u2013\u2014:|").strip()
        url = m.group(0).strip()
        if name and is_http_url(url):
            return name, url
    return None


def parse_otp_inline_button_input(text, entities=None):
    """-> (name, url, emoji_id) or None. A custom emoji in the text becomes the button emoji."""
    text = str(text or "")
    emoji_id, emoji_char = "", ""
    for ent in entities or []:
        if ent.get("type") == "custom_emoji":
            emoji_id = str(ent.get("custom_emoji_id") or "")
            off, ln = ent.get("offset", 0), ent.get("length", 0)
            emoji_char = text.encode("utf-16-le")[off * 2:(off + ln) * 2].decode("utf-16-le")
            break
    if emoji_char:
        text = text.replace(emoji_char, "")
    parts = split_otp_inline_button_input(text)
    if not parts:
        return None
    name = re.sub(r"\s+", " ", parts[0]).strip()
    if not name or len(name) > 32:
        return None
    return name, parts[1], emoji_id
# ==========================================
# OTP Group inline buttons  -- END
# ==========================================

# ==========================================
# SQLite Database Setup
# ==========================================
def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    # Apply the lock wait to every connection, not only the migration connection.
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

def init_db():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=10000")
    
        # Settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
    
        # Users table - fixed column names
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                balance REAL DEFAULT 0,
                total_refers INTEGER DEFAULT 0,
                total_otps INTEGER DEFAULT 0,
                banned INTEGER DEFAULT 0,
                verified INTEGER DEFAULT 0,
                referred_by TEXT,
                ref_paid INTEGER DEFAULT 0
            )
        ''')
    
        # Withdrawals table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS withdrawals (
                req_id TEXT PRIMARY KEY,
                user_id TEXT,
                amount REAL,
                method TEXT,
                number TEXT,
                full_name TEXT,
                status TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
        # Number batches table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS number_batches (
                batch_id TEXT PRIMARY KEY,
                filename TEXT,
                service TEXT,
                country TEXT,
                numbers TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_number_uploads (
                user_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                data TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_admin_interactions (
                user_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                data TEXT NOT NULL,
                prompt_message_id INTEGER,
                updated_at REAL NOT NULL
            )
        ''')

        # Persist every interactive state, not only the legacy upload/admin flows.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_user_states (
                user_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                data TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_telegram_updates (
                update_id INTEGER PRIMARY KEY,
                update_json TEXT NOT NULL,
                received_at REAL NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS force_join_verified_users (
                user_id TEXT PRIMARY KEY,
                verified_at REAL NOT NULL
            )
        ''')
        # Remove the legacy permanent Force Join bypass records. Membership is
        # checked through the short-lived cache instead.
        cursor.execute("DELETE FROM force_join_verified_users")
    
        # Used numbers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS used_numbers (
                number TEXT PRIMARY KEY
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS number_usage (
                user_id TEXT NOT NULL,
                service TEXT NOT NULL,
                country TEXT NOT NULL,
                number TEXT NOT NULL,
                used_at REAL NOT NULL,
                released INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, number)
            )
        ''')
        number_usage_columns = {
            row["name"] for row in cursor.execute("PRAGMA table_info(number_usage)")
        }
        if "released" not in number_usage_columns:
            cursor.execute(
                "ALTER TABLE number_usage "
                "ADD COLUMN released INTEGER NOT NULL DEFAULT 0"
            )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_number_usage_user_service_country "
            "ON number_usage(user_id, service, country)"
        )
    
        # Traffic table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS traffic (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service TEXT,
                iso TEXT,
                flag TEXT,
                number TEXT,
                time REAL,
                real_range TEXT
            )
        ''')
    
        # Processed OTPs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_otps (
                otp_id TEXT PRIMARY KEY
            )
        ''')
    
        # Banned cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_cache (
                user_id TEXT PRIMARY KEY,
                banned INTEGER DEFAULT 0,
                time REAL
            )
        ''')
    
        conn.commit()
    
        # Create indexes after tables are created
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_banned ON users(banned)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_users_banned")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_users_referred_by")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id ON withdrawals(user_id)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_withdrawals_user_id")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_withdrawals_status")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_batches_service ON number_batches(service)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_batches_service")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_batches_country ON number_batches(country)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_batches_country")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_traffic_time ON traffic(time)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_traffic_time")
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_traffic_service ON traffic(service)')
        except sqlite3.DatabaseError:
            logger.exception("Could not create idx_traffic_service")
    
        # Analytics tables (analytics_addon)
        init_analytics_tables()

        # Advanced features: user_language column (advanced_features)
        init_advanced_tables()

        conn.commit()
    finally:
        conn.close()
    logger.info("Database initialized successfully")

# Initialize database
acquire_single_instance_lock()
init_db()

def load_settings():
    global used_numbers_list, total_uploaded_stats, total_assigned_stats
    global recent_traffic, processed_otps, pending_withdrawals
    global _ENCRYPTED_DATA_KEY_WAS_REGENERATED
    # Firestore can block; resolve its snapshot before taking the shared data lock.
    firebase_config_ref = None
    firebase_config_doc = None
    firebase_initial_payload = None
    otp_keys_migrated = False
    otp_keys_for_migration = []
    if db:
        try:
            firebase_config_ref = db.collection("settings").document("bot_config")
            firebase_config_doc = firebase_config_ref.get(timeout=3)
        except Exception as e:
            logger.error("Could not load settings from Firestore: %s", e)
    with data_lock:
        conn = get_db()
        try:
            cursor = conn.cursor()
            loaded_setting_keys = set()
            force_join_channels_migrated = False
        
            # Load settings
            cursor.execute('SELECT key, value FROM settings')
            rows = cursor.fetchall()
            for row in rows:
                key = row['key']
                value_str = row['value']
                loaded_setting_keys.add(key)
                # Check if value is None or empty
                if value_str is None or value_str.strip() == '':
                    continue
                try:
                    value = json.loads(value_str)
                    if key in FS_KEYS or key in {
                        "custom_messages", "premium_flags", "premium_apps"
                    }:
                        bot_settings[key] = value
                except json.JSONDecodeError:
                    # If JSON decode fails, skip this setting
                    logger.warning(f"Failed to decode JSON for key: {key}, value: {value_str[:100]}")
                    continue

            # Firestore is authoritative for shared settings; only apply its
            # already-fetched snapshot while holding the local data lock.
            if firebase_config_doc is not None:
                if firebase_config_doc.exists:
                    firebase_settings = firebase_config_doc.to_dict() or {}
                    for key in FS_KEYS:
                        if key in firebase_settings:
                            bot_settings[key] = firebase_settings[key]
                    logger.info("Shared settings loaded from Firestore.")
                else:
                    firebase_initial_payload = {
                        key: (
                            encrypt_panels_for_storage(bot_settings[key])
                            if key == "panels"
                            else copy.deepcopy(bot_settings[key])
                        )
                        for key in FS_KEYS
                        if key in bot_settings
                    }

            if (
                _LOCAL_DATA_KEY_WAS_GENERATED
                and not _ENCRYPTED_DATA_KEY_WAS_REGENERATED
                and _contains_encrypted_marker(bot_settings.get("panels"))
            ):
                _ENCRYPTED_DATA_KEY_WAS_REGENERATED = True
                logger.warning(
                    "A new data-encryption key was generated while previously encrypted "
                    "Firestore panel credentials were found; saved panel passwords "
                    "can no longer be read and must be entered again."
                )

            # Decrypt panel credentials (SQLite and Firestore copies); plaintext
            # found here means a pre-encryption database that must be migrated.
            panel_secrets_migrated = bool(
                decrypt_panels_in_place(bot_settings.get("panels")) and _FERNET is not None
            )

            # Migrate installations created before the dedicated number settings
            # existed.  Preserve a previously configured NUM/REQ value, but use
            # the requested 3-second change default for the new cooldown.
            if "number_limit" not in loaded_setting_keys:
                try:
                    bot_settings["number_limit"] = max(1, int(bot_settings.get("num_req", 3)))
                except (TypeError, ValueError):
                    bot_settings["number_limit"] = 3
            if "change_number_cooldown" not in loaded_setting_keys:
                bot_settings["change_number_cooldown"] = 3

            # Keep old databases from turning malformed JSON settings into
            # runtime TypeErrors/KeyErrors after an upgrade.
            for list_key in (
            "admins", "panels", "fw_groups", "w_methods",
                "fj_channels", "disabled_services", "otp_inline_buttons",
            ):
                if not isinstance(bot_settings.get(list_key), list):
                    bot_settings[list_key] = []
            bot_settings["otp_inline_buttons"] = normalize_otp_inline_buttons(
                bot_settings.get("otp_inline_buttons")
            )
            normalized_fj_channels = [
                normalize_force_join_channel(channel)
                for channel in bot_settings.get("fj_channels", [])
            ]
            if normalized_fj_channels != bot_settings.get("fj_channels", []):
                bot_settings["fj_channels"] = normalized_fj_channels
                force_join_channels_migrated = True
            for dict_key in (
            "premium_flags", "premium_apps",
            ):
                if not isinstance(bot_settings.get(dict_key), dict):
                    bot_settings[dict_key] = {}

            # Backward-compatible migration from calling-code keys (1, 7, 880)
            # to ISO keys. The dial code remains on each record for phone lookup.
            migrated_flags = {}
            premium_flags_migrated = False
            for legacy_key, raw_item in bot_settings.get("premium_flags", {}).items():
                if not isinstance(raw_item, dict):
                    logger.warning("Skipping malformed premium flag record key=%r", legacy_key)
                    premium_flags_migrated = True
                    continue
                item = copy.deepcopy(raw_item)
                iso = str(item.get("iso", "")).strip().upper()
                code = str(item.get("code") or (legacy_key if str(legacy_key).isdigit() else "")).strip()
                new_key = iso if re.fullmatch(r"[A-Z]{2}", iso) else str(legacy_key)
                if new_key in migrated_flags:
                    new_key = f"{new_key}_{code or legacy_key}"
                if code:
                    item["code"] = code
                if iso:
                    item["iso"] = iso
                migrated_flags[new_key] = item
                if new_key != str(legacy_key) or item != raw_item:
                    premium_flags_migrated = True
            if migrated_flags != bot_settings.get("premium_flags", {}):
                bot_settings["premium_flags"] = migrated_flags
                premium_flags_migrated = True

            if not isinstance(bot_settings.get("custom_messages"), dict):
                bot_settings["custom_messages"] = {}
            for menu_key, default_value in DEFAULT_CUSTOM_MESSAGES.items():
                current = bot_settings["custom_messages"].get(menu_key)
                if not isinstance(current, dict):
                    bot_settings["custom_messages"][menu_key] = copy.deepcopy(default_value)
                    continue
                if not isinstance(current.get("text"), str):
                    current["text"] = default_value["text"]
                if not isinstance(current.get("buttons"), list):
                    current["buttons"] = []
        
            # Load number batches
            cursor.execute('SELECT * FROM number_batches')
            rows = cursor.fetchall()
            for row in rows:
                try:
                    raw_numbers = json.loads(row['numbers'])
                    normalized_numbers = []
                    for item in raw_numbers if isinstance(raw_numbers, list) else []:
                        if isinstance(item, dict):
                            raw_num = item.get("num", "")
                            shares = item.get("shares", 0)
                            used_by = item.get("used_by", [])
                        else:
                            raw_num, shares, used_by = item, 0, []
                        raw_num = str(raw_num).strip()
                        if not raw_num:
                            continue
                        try:
                            shares = max(0, int(shares or 0))
                        except (TypeError, ValueError):
                            shares = 0
                        normalized_numbers.append({
                            "num": raw_num,
                            "shares": shares,
                            "used_by": list(used_by) if isinstance(used_by, list) else [],
                        })
                    number_batches[row['batch_id']] = {
                        'filename': row['filename'],
                        'service': row['service'],
                        'country': row['country'],
                        'numbers': normalized_numbers,
                    }
                except (TypeError, ValueError, json.JSONDecodeError):
                    logger.debug("Ignored exception in load_settings (line 1465)", exc_info=True)
                    continue
        
            # Load used numbers
            cursor.execute('SELECT number FROM used_numbers')
            used_numbers_list = [row['number'] for row in cursor.fetchall()]
        
            # Load traffic (last 500 records)
            cursor.execute('SELECT service, iso, flag, number, time, real_range FROM traffic ORDER BY time DESC LIMIT 500')
            recent_traffic = []
            for row in cursor.fetchall():
                recent_traffic.append({
                    'service': row['service'],
                    'iso': row['iso'],
                    'flag': row['flag'],
                    'number': row['number'],
                    'time': row['time'],
                    'real_range': row['real_range']
                })
        
            # Load the most recent processed OTPs (newest first from SQLite,
            # stored oldest->newest so FIFO eviction drops the oldest keys).
            cursor.execute(
                'SELECT otp_id FROM processed_otps ORDER BY rowid DESC LIMIT ?',
                (PROCESSED_OTPS_LIMIT,),
            )
            _loaded_otp_ids = [row['otp_id'] for row in cursor.fetchall()]
            processed_otps = OrderedDict()
            for stored_id in reversed(_loaded_otp_ids):
                otp_id = _canonicalize_processed_otp_id(stored_id)
                otp_keys_migrated = otp_keys_migrated or otp_id != stored_id
                processed_otps[otp_id] = None
                processed_otps.move_to_end(otp_id)
            while len(processed_otps) > PROCESSED_OTPS_LIMIT:
                processed_otps.popitem(last=False)
            if otp_keys_migrated:
                otp_keys_for_migration = list(processed_otps)

            cursor.execute(
                "SELECT req_id, user_id, amount, method, number, full_name "
                "FROM withdrawals WHERE status = 'pending'"
            )
            pending_withdrawals = {}
            for row in cursor.fetchall():
                try:
                    pending_withdrawals[row["req_id"]] = {
                        "user_id": int(row["user_id"]),
                        "amount": float(row["amount"]),
                        "method": row["method"],
                        "number": row["number"],
                        "full_name": row["full_name"],
                    }
                except (TypeError, ValueError):
                    logger.warning("Skipping malformed pending withdrawal %r", dict(row))

            # Resume only the number-upload workflow after a process restart.
            # Other interactive states may contain credentials or unrelated data.
            cursor.execute(
                "SELECT user_id, state, data, updated_at FROM pending_number_uploads"
            )
            now = time.time()
            for row in cursor.fetchall():
                try:
                    chat_id = int(row["user_id"])
                    state = row["state"]
                    data = json.loads(row["data"])
                    updated_at = float(row["updated_at"])
                    if (
                        state not in PENDING_NUMBER_UPLOAD_STATES
                        or not isinstance(data, dict)
                        or now - updated_at > PENDING_UPLOAD_TTL_SECONDS
                    ):
                        cursor.execute(
                            "DELETE FROM pending_number_uploads WHERE user_id = ?",
                            (str(row["user_id"]),),
                        )
                        if now - updated_at > PENDING_UPLOAD_TTL_SECONDS:
                            expired_admin_interaction_chats.append(chat_id)
                        continue
                    user_states[chat_id] = state
                    temp_data[chat_id] = data
                    admin_interaction_updated_at[chat_id] = updated_at
                    if data.get("msg_id"):
                        admin_prompt_messages[chat_id] = int(data["msg_id"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    cursor.execute(
                        "DELETE FROM pending_number_uploads WHERE user_id = ?",
                        (str(row["user_id"]),),
                    )
        
            cursor.execute(
                "SELECT user_id, state, data, prompt_message_id, updated_at "
                "FROM pending_admin_interactions"
            )
            for row in cursor.fetchall():
                user_key = str(row["user_id"])
                try:
                    chat_id = int(user_key)
                    state = str(row["state"])
                    data = decrypt_interaction_data(json.loads(row["data"]))
                    updated_at = float(row["updated_at"])
                    if (
                        state not in ADMIN_INTERACTION_STATES
                        or not isinstance(data, dict)
                        or now - updated_at > ADMIN_INTERACTION_TTL_SECONDS
                    ):
                        cursor.execute(
                            "DELETE FROM pending_admin_interactions WHERE user_id = ?",
                            (user_key,),
                        )
                        if now - updated_at > ADMIN_INTERACTION_TTL_SECONDS:
                            expired_admin_interaction_chats.append(chat_id)
                        continue
                    user_states[chat_id] = state
                    temp_data[chat_id] = data
                    admin_interaction_updated_at[chat_id] = updated_at
                    prompt_id = row["prompt_message_id"] or data.get("msg_id")
                    if prompt_id:
                        admin_prompt_messages[chat_id] = int(prompt_id)
                except (TypeError, ValueError, json.JSONDecodeError):
                    logger.warning("Deleting malformed admin interaction for user %s", user_key)
                    cursor.execute(
                        "DELETE FROM pending_admin_interactions WHERE user_id = ?",
                        (user_key,),
                    )

            # Restore every in-flight prompt from the unified state journal.
            known_admins = {str(OWNER_ID)}
            known_admins.update(
                str(value) for value in bot_settings.get("admins", [])
            )
            cursor.execute(
                "SELECT user_id, state, data, updated_at FROM pending_user_states"
            )
            for row in cursor.fetchall():
                user_key = str(row["user_id"])
                try:
                    chat_id = int(user_key)
                    state = str(row["state"])
                    updated_at = float(row["updated_at"])
                    data = decrypt_interaction_data(json.loads(row["data"]))
                    expired = now - updated_at > ADMIN_INTERACTION_TTL_SECONDS
                    admin_only = state in ADMIN_INTERACTION_STATES or state == "wait_for_broadcast"
                    if (
                        state not in PERSISTED_INTERACTION_STATES
                        or not isinstance(data, dict)
                        or expired
                        or (admin_only and user_key not in known_admins)
                    ):
                        cursor.execute(
                            "DELETE FROM pending_user_states WHERE user_id = ?",
                            (user_key,),
                        )
                        if expired:
                            expired_admin_interaction_chats.append(chat_id)
                        continue
                    user_states[chat_id] = state
                    temp_data[chat_id] = data
                    admin_interaction_updated_at[chat_id] = updated_at
                    if data.get("msg_id"):
                        admin_prompt_messages[chat_id] = int(data["msg_id"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    logger.warning(
                        "Deleting malformed saved interaction for user %s",
                        user_key,
                    )
                    cursor.execute(
                        "DELETE FROM pending_user_states WHERE user_id = ?",
                        (user_key,),
                    )

            # Load stats
            cursor.execute('SELECT value FROM settings WHERE key = "total_uploaded_stats"')
            row = cursor.fetchone()
            if row and row['value']:
                try:
                    total_uploaded_stats = json.loads(row['value'])
                except (TypeError, ValueError, json.JSONDecodeError):
                    total_uploaded_stats = 0
        
            cursor.execute('SELECT value FROM settings WHERE key = "total_assigned_stats"')
            row = cursor.fetchone()
            if row and row['value']:
                try:
                    total_assigned_stats = json.loads(row['value'])
                except (TypeError, ValueError, json.JSONDecodeError):
                    total_assigned_stats = 0
        
        finally:
            conn.close()
        if panel_secrets_migrated:
            save_settings()
            if db:
                try:
                    startup_executor.submit(_sync_firebase_settings)
                except RuntimeError:
                    logger.warning("Panel credential encryption sync to Firestore was skipped.")
            scrub_sqlite_secrets(DB_FILE, force_vacuum=True)
            scrub_old_backups()
            logger.info("Panel credentials encrypted and old plaintext copies scrubbed.")
        if force_join_channels_migrated:
            save_settings()
            if db:
                try:
                    startup_executor.submit(_sync_firebase_settings)
                except RuntimeError:
                    logger.warning("Force Join settings migration sync was skipped.")
        if premium_flags_migrated:
            save_settings()
            logger.info("Premium flag keys migrated to ISO country codes.")
        dirty_number_batches.clear()
        deleted_number_batches.clear()
        pending_used_numbers.clear()
        pending_number_usage.clear()
        logger.info("Settings loaded successfully")
    if otp_keys_migrated:
        save_processed_otps(otp_keys_for_migration, prune=True)
    # Initialize a missing Firestore settings document only after releasing data_lock.
    if firebase_initial_payload is not None and firebase_config_ref is not None:
        try:
            firebase_config_ref.set(firebase_initial_payload, timeout=3)
            logger.info("Firestore settings document initialized.")
        except Exception as e:
            logger.error("Could not initialize Firestore settings: %s", e)

def save_local_db(clear_pending_user=None):
    """Persist only changed stock rows and counters, not whole tables."""
    with data_lock:
        conn = get_db()
        try:
            cursor = conn.cursor()
            for batch_id in tuple(dirty_number_batches):
                batch_data = number_batches.get(batch_id)
                if batch_data is None:
                    continue
                cursor.execute(
                    'INSERT OR REPLACE INTO number_batches '
                    '(batch_id, filename, service, country, numbers) VALUES (?, ?, ?, ?, ?)',
                    (
                        batch_id,
                        batch_data.get('filename', ''),
                        batch_data.get('service', ''),
                        batch_data.get('country', ''),
                        json.dumps(batch_data.get('numbers', [])),
                    ),
                )
            if deleted_number_batches:
                cursor.executemany(
                    'DELETE FROM number_batches WHERE batch_id = ?',
                    [(batch_id,) for batch_id in deleted_number_batches],
                )
            if pending_used_numbers:
                cursor.executemany(
                    'INSERT OR IGNORE INTO used_numbers (number) VALUES (?)',
                    [(number,) for number in pending_used_numbers],
                )
            if pending_number_usage:
                cursor.executemany(
                    "INSERT INTO number_usage "
                    "(user_id, service, country, number, used_at, released) "
                    "VALUES (?, ?, ?, ?, ?, 0) "
                    "ON CONFLICT(user_id, number) DO UPDATE SET "
                    "service = excluded.service, country = excluded.country, "
                    "used_at = excluded.used_at, released = 0",
                    pending_number_usage,
                )
            if clear_pending_user is not None:
                cursor.execute(
                    "DELETE FROM pending_number_uploads WHERE user_id = ?",
                    (str(clear_pending_user),),
                )

            # Save stats
            cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('total_uploaded_stats', json.dumps(total_uploaded_stats)))
            cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('total_assigned_stats', json.dumps(total_assigned_stats)))
        
            conn.commit()
        finally:
            conn.close()
        dirty_number_batches.clear()
        deleted_number_batches.clear()
        pending_used_numbers.clear()
        pending_number_usage.clear()

def save_processed_otps(otp_ids, prune=False):
    """Persist processed OTP keys; optionally keep only the newest N rows."""
    otp_ids = [str(i) for i in otp_ids]
    if not otp_ids and not prune:
        return
    with data_lock:
        conn = get_db()
        try:
            cursor = conn.cursor()
            if otp_ids:
                cursor.executemany(
                    'INSERT OR IGNORE INTO processed_otps (otp_id) VALUES (?)',
                    [(i,) for i in otp_ids],
                )
            if prune:
                cursor.execute(
                    'DELETE FROM processed_otps WHERE rowid NOT IN '
                    '(SELECT rowid FROM processed_otps ORDER BY rowid DESC LIMIT ?)',
                    (PROCESSED_OTPS_LIMIT,),
                )
            conn.commit()
        finally:
            conn.close()

def _read_setting_raw(key):
    conn = get_db()
    try:
        row = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    finally:
        conn.close()
    if not row or row['value'] is None:
        return None
    try:
        return json.loads(row['value'])
    except (TypeError, ValueError):
        return None

def _write_setting_raw(key, value):
    conn = get_db()
    try:
        conn.execute(
            'INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
            (key, json.dumps(value)),
        )
        conn.commit()
    finally:
        conn.close()

def save_traffic_record(traffic_item):
    """Persist one traffic row without rewriting stock or history."""
    with data_lock:
        conn = get_db()
        try:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO traffic (service, iso, flag, number, time, real_range) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (
                    traffic_item.get('service'),
                    traffic_item.get('iso'),
                    traffic_item.get('flag'),
                    traffic_item.get('number'),
                    traffic_item.get('time'),
                    traffic_item.get('real_range'),
                ),
            )
            cursor.execute(
                'DELETE FROM traffic WHERE id NOT IN '
                '(SELECT id FROM traffic ORDER BY id DESC LIMIT 500)'
            )
            conn.commit()
        finally:
            conn.close()

def save_settings():
    """Persist every configurable setting, including custom UI and emoji data."""
    with data_lock:
        conn = get_db()
        try:
            cursor = conn.cursor()
            for key, value in bot_settings.items():
                if key == "panels":
                    value = encrypt_panels_for_storage(value)
                cursor.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (key, json.dumps(value))
                )
            conn.commit()
        finally:
            conn.close()

db_writer_condition = threading.Condition()
db_writer_pending = False
db_writer_include_local = False
db_writer_settings_changed = False
db_writer_clear_pending_users = set()
db_writer_deadline = 0.0
db_writer_active = False
db_writer_stopping = False
db_writer_thread = None
DB_WRITE_DEBOUNCE_SECONDS = 0.15


def invalidate_inventory_cache():
    """Discard cached service/country indexes after inventory or setting changes."""
    global inventory_index_cache, inventory_cache_version
    global service_screen_cache, country_screen_cache
    with inventory_index_lock:
        inventory_index_cache = None
        service_screen_cache = None
        country_screen_cache = {}
        inventory_cache_version += 1


def _db_writer_loop():
    global db_writer_pending, db_writer_include_local
    global db_writer_settings_changed
    global db_writer_active
    while True:
        with db_writer_condition:
            while not db_writer_pending and not db_writer_stopping:
                db_writer_condition.wait()
            if db_writer_stopping and not db_writer_pending:
                return
            while db_writer_pending and not db_writer_stopping:
                wait_for = db_writer_deadline - time.monotonic()
                if wait_for <= 0:
                    break
                db_writer_condition.wait(timeout=wait_for)
            include_local = db_writer_include_local
            settings_changed = db_writer_settings_changed
            clear_users = set(db_writer_clear_pending_users)
            db_writer_pending = False
            db_writer_include_local = False
            db_writer_settings_changed = False
            db_writer_clear_pending_users.clear()
            db_writer_active = True

        try:
            if include_local:
                save_local_db()
                for user_id in clear_users:
                    conn = get_db()
                    try:
                        conn.execute(
                            "DELETE FROM pending_number_uploads WHERE user_id = ?",
                            (str(user_id),),
                        )
                        conn.commit()
                    finally:
                        conn.close()
            if settings_changed:
                save_settings()
            if settings_changed and db:
                try:
                    startup_executor.submit(_sync_firebase_settings)
                except RuntimeError:
                    logger.warning("Firebase settings sync was skipped during shutdown.")
        except Exception:
            logger.exception("Background database write failed.")
        finally:
            with db_writer_condition:
                db_writer_active = False
                db_writer_condition.notify_all()


def _ensure_db_writer():
    global db_writer_thread
    with db_writer_condition:
        if db_writer_thread is None or not db_writer_thread.is_alive():
            db_writer_thread = threading.Thread(
                target=_db_writer_loop,
                name="bot-db-writer",
                daemon=True,
            )
            db_writer_thread.start()


def save_db(include_local=False, clear_pending_user=None):
    """Debounce database persistence off the update-handling thread."""
    global db_writer_pending, db_writer_include_local
    global db_writer_settings_changed, db_writer_deadline
    invalidate_inventory_cache()
    _ensure_db_writer()
    with db_writer_condition:
        db_writer_pending = True
        db_writer_include_local = db_writer_include_local or include_local
        db_writer_settings_changed = db_writer_settings_changed or not include_local
        if clear_pending_user is not None:
            db_writer_clear_pending_users.add(str(clear_pending_user))
        db_writer_deadline = time.monotonic() + DB_WRITE_DEBOUNCE_SECONDS
        db_writer_condition.notify_all()


def flush_db_writer(timeout=10.0):
    """Wait for queued writes; used during orderly shutdown."""
    global db_writer_deadline
    with db_writer_condition:
        if db_writer_pending:
            db_writer_deadline = time.monotonic()
            db_writer_condition.notify_all()
        deadline = time.monotonic() + timeout
        while db_writer_pending or db_writer_active:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning("Timed out waiting for the database writer to flush.")
                return False
            db_writer_condition.wait(timeout=remaining)
    return True


def shutdown_db_writer():
    global db_writer_stopping
    flush_db_writer()
    with db_writer_condition:
        db_writer_stopping = True
        db_writer_condition.notify_all()

atexit.register(shutdown_db_writer)

def _sync_firebase_settings():
    if not db:
        return
    try:
        fs_data = {key: (encrypt_panels_for_storage(bot_settings[key]) if key == "panels" else bot_settings[key]) for key in FS_KEYS if key in bot_settings}
        db.collection("settings").document("bot_config").set(fs_data, timeout=3)
    except Exception as e:
        logger.error("Could not save settings to Firestore: %s", e)

# Load settings after all definitions
load_settings()

# Migrate the bundled box-style screens once.  They are difficult to read
# across Telegram clients with different font metrics, while the new layout
# keeps the same welcome/service information.
_ui_migrated = False
for _menu_key in ("start", "search_number", "withdrawal"):
    _current = bot_settings.get("custom_messages", {}).get(_menu_key, {})
    _current_text = _current.get("text", "") if isinstance(_current, dict) else ""
    if any(mark in _current_text for mark in ("╔", "╚", "║", "《", "》")):
        bot_settings.setdefault("custom_messages", {})[_menu_key] = DEFAULT_CUSTOM_MESSAGES[_menu_key].copy()
        _ui_migrated = True
if _ui_migrated:
    save_db()

# ==========================================
# SQLite User Management Functions
# ==========================================
user_cache = TTLDict("user_cache", max_size=50000)
user_cache_lock = threading.RLock()
_user_load_locks = {}
_user_load_lock_refs = {}
_user_load_locks_guard = threading.RLock()
_USER_SYNC_FIELDS = (
    "balance", "total_refers", "total_otps", "banned",
    "verified", "referred_by", "ref_paid",
)
_user_first_access_state = {}
_user_first_access_state_lock = threading.RLock()
_user_firestore_retry_needed = set()
_user_firestore_retry_inflight = set()

class _UserLoadLockContext:
    def __init__(self, user_key, lock):
        self.user_key = user_key
        self.lock = lock

    def __enter__(self):
        self.lock.acquire()
        return self.lock

    def __exit__(self, exc_type, exc_value, traceback):
        self.lock.release()
        with _user_load_locks_guard:
            remaining = _user_load_lock_refs.get(self.user_key, 1) - 1
            if remaining <= 0:
                _user_load_lock_refs.pop(self.user_key, None)
                if _user_load_locks.get(self.user_key) is self.lock:
                    _user_load_locks.pop(self.user_key, None)
            else:
                _user_load_lock_refs[self.user_key] = remaining
        return False


def _user_load_lock(user_id):
    """Serialize one user's load/mutations without blocking other users."""
    user_key = str(user_id)
    with _user_load_locks_guard:
        lock = _user_load_locks.setdefault(user_key, threading.RLock())
        _user_load_lock_refs[user_key] = _user_load_lock_refs.get(user_key, 0) + 1
    return _UserLoadLockContext(user_key, lock)

def _submit_firebase_task(task, *args):
    if not db:
        return False
    try:
        startup_executor.submit(task, *args)
        return True
    except RuntimeError:
        logger.warning("Firestore task skipped because the background pool is closing.")
        return False


def _firebase_set_user_fields_sync(user_id, fields):
    if not db:
        return
    try:
        try:
            firestore_user_id = int(user_id)
        except (TypeError, ValueError):
            firestore_user_id = str(user_id)
        payload = {"user_id": firestore_user_id}
        payload.update(fields)
        db.collection("users").document(str(user_id)).set(
            payload, merge=True, timeout=3
        )
    except Exception as e:
        logger.warning("Could not sync user %s to Firestore: %s", user_id, e)


def _firebase_set_user_fields(user_id, fields):
    if db:
        _submit_firebase_task(
            _firebase_set_user_fields_sync,
            str(user_id),
            copy.deepcopy(fields),
        )


def _firebase_increment_user_sync(user_id, increments):
    if not db:
        return
    try:
        try:
            firestore_user_id = int(user_id)
        except (TypeError, ValueError):
            firestore_user_id = str(user_id)
        payload = {"user_id": firestore_user_id}
        payload.update({
            key: firestore.Increment(value)
            for key, value in increments.items()
        })
        db.collection("users").document(str(user_id)).set(
            payload, merge=True, timeout=3
        )
    except Exception as e:
        logger.warning("Could not update Firestore counters for user %s: %s", user_id, e)


def _firebase_increment_user(user_id, **increments):
    if db:
        _submit_firebase_task(
            _firebase_increment_user_sync,
            str(user_id),
            dict(increments),
        )


def migrate_rounded_balances():
    """Round legacy SQLite balances once and mirror changed values to Firestore."""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        changed_rows = cursor.execute(
            "SELECT user_id, ROUND(balance, 4) AS rounded_balance "
            "FROM users WHERE balance IS NOT ROUND(balance, 4)"
        ).fetchall()
        if not changed_rows:
            conn.commit()
            return 0

        cursor.execute("UPDATE users SET balance = ROUND(balance, 4)")
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Legacy balance rounding migration failed.")
        raise
    finally:
        conn.close()

    for row in changed_rows:
        user_id = str(row["user_id"])
        rounded_balance = float(row["rounded_balance"] or 0)
        with user_cache_lock:
            cached_user = user_cache.get(user_id)
            if cached_user is not None:
                cached_user["balance"] = rounded_balance

    logger.info(
        "Rounded %s existing SQLite balance(s) to 4 decimals; Firestore is left authoritative.",
        len(changed_rows),
    )
    return len(changed_rows)


def _read_local_user(user_id):
    """Read a user row without holding the global cache/data locks."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    try:
        resolved_user_id = int(row["user_id"])
    except (TypeError, ValueError):
        resolved_user_id = row["user_id"]
    return {
        "user_id": resolved_user_id,
        "balance": row["balance"],
        "total_refers": row["total_refers"],
        "total_otps": row["total_otps"],
        "banned": bool(row["banned"]),
        "verified": bool(row["verified"]),
        "referred_by": row["referred_by"],
        "ref_paid": bool(row["ref_paid"]) if row["ref_paid"] is not None else False,
    }


def _fetch_firebase_user(user_id):
    """Return (read_succeeded, remote_exists, remote_fields), with no shared locks."""
    if not db:
        return False, False, {}
    try:
        doc = db.collection("users").document(str(user_id)).get(timeout=3)
        if not doc.exists:
            return True, False, {}
        payload = doc.to_dict() or {}
        return True, True, {
            field: payload[field]
            for field in _USER_SYNC_FIELDS
            if field in payload
        }
    except Exception as exc:
        logger.warning("Could not load user %s from Firestore: %s", user_id, exc)
        return False, False, {}


def _merge_remote_user(local_data, remote_fields):
    """Firestore wins fields it contains; missing referral fields stay local."""
    merged = copy.deepcopy(local_data)
    for field, value in remote_fields.items():
        if field in _USER_SYNC_FIELDS:
            merged[field] = value
    merged["balance"] = float(merged.get("balance", 0) or 0)
    merged["total_refers"] = int(merged.get("total_refers", 0) or 0)
    merged["total_otps"] = int(merged.get("total_otps", 0) or 0)
    merged["banned"] = bool(merged.get("banned", False))
    merged["verified"] = bool(merged.get("verified", False))
    merged["ref_paid"] = bool(merged.get("ref_paid", False))
    return merged


def _store_user_local(user_id, data):
    """Insert/update the full merged user record in SQLite."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users "
            "(user_id, balance, total_refers, total_otps, banned, verified, referred_by, ref_paid) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "balance=excluded.balance, total_refers=excluded.total_refers, "
            "total_otps=excluded.total_otps, banned=excluded.banned, "
            "verified=excluded.verified, referred_by=excluded.referred_by, "
            "ref_paid=excluded.ref_paid",
            (
                str(user_id),
                float(data.get("balance", 0) or 0),
                int(data.get("total_refers", 0) or 0),
                int(data.get("total_otps", 0) or 0),
                int(bool(data.get("banned", False))),
                int(bool(data.get("verified", False))),
                data.get("referred_by"),
                int(bool(data.get("ref_paid", False))),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _user_fields_for_firebase(data):
    fields = {
        field: data.get(field)
        for field in (
            "balance", "total_refers", "total_otps", "banned", "verified",
        )
    }
    if data.get("referred_by") not in (None, ""):
        fields["referred_by"] = data.get("referred_by")
        fields["ref_paid"] = bool(data.get("ref_paid", False))
    return fields


def _refresh_user_from_firebase(user_id, local_snapshot=None):
    """Retry a failed cold read in the background and merge a successful result."""
    user_key = str(user_id)
    try:
        succeeded, exists, remote_fields = _fetch_firebase_user(user_id)
        if not succeeded:
            return
        with user_cache_lock:
            current = user_cache.get(user_key)
            baseline = copy.deepcopy(local_snapshot or current)
            if current is None:
                current = copy.deepcopy(local_snapshot or {
                    "user_id": user_id, "balance": 0.0, "total_refers": 0,
                    "total_otps": 0, "banned": False, "verified": False,
                    "referred_by": None, "ref_paid": False,
                })
            refreshed = _merge_remote_user(current, remote_fields) if exists else current
            if exists and baseline:
                # Preserve local mutations made after the original cold read
                # while the retry was in flight.
                for field in _USER_SYNC_FIELDS:
                    if current.get(field) != baseline.get(field):
                        refreshed[field] = current.get(field)
            user_cache[user_key] = refreshed
        if exists:
            _store_user_local(user_id, refreshed)
            with _user_first_access_state_lock:
                if _user_first_access_state.get(user_key) in {
                    "unknown", "unknown_new"
                }:
                    _user_first_access_state[user_key] = "existing"
                _user_firestore_retry_needed.discard(user_key)
        else:
            with _user_first_access_state_lock:
                was_new_candidate = (
                    _user_first_access_state.get(user_key) == "unknown_new"
                )
                if was_new_candidate:
                    _user_first_access_state[user_key] = "new"
                elif _user_first_access_state.get(user_key) == "unknown":
                    _user_first_access_state[user_key] = "existing"
                _user_firestore_retry_needed.discard(user_key)
            _firebase_set_user_fields(
                user_id,
                _user_fields_for_firebase(refreshed),
            )
    except Exception:
        logger.exception("Background Firestore user refresh failed for %s", user_key)
    finally:
        with _user_first_access_state_lock:
            _user_firestore_retry_inflight.discard(user_key)


def _queue_user_firestore_retry(user_id, local_snapshot):
    user_key = str(user_id)
    with _user_first_access_state_lock:
        if user_key in _user_firestore_retry_inflight:
            return
        _user_firestore_retry_inflight.add(user_key)
    submitted = _submit_firebase_task(
        _refresh_user_from_firebase,
        user_id,
        copy.deepcopy(local_snapshot),
    )
    if not submitted:
        with _user_first_access_state_lock:
            _user_firestore_retry_inflight.discard(user_key)

def get_user(user_id):
    user_id_str = str(user_id)
    with user_cache_lock:
        cached = user_cache.get(user_id_str)
        if cached is not None:
            with _user_first_access_state_lock:
                should_retry = user_id_str in _user_firestore_retry_needed
            if should_retry and db:
                _queue_user_firestore_retry(user_id, cached)
            return cached

    # Serialize initialization only for this user; remote Firestore I/O must
    # never hold the global cache lock and stall unrelated chats.
    with _user_load_lock(user_id_str):
        with user_cache_lock:
            cached = user_cache.get(user_id_str)
            if cached is not None:
                return cached

        local_data = _read_local_user(user_id_str)
        local_exists = local_data is not None
        if local_data is None:
            local_data = {
                "user_id": user_id, "balance": 0.0, "total_refers": 0,
                "total_otps": 0, "banned": False, "verified": False,
                "referred_by": None, "ref_paid": False,
            }
        remote_succeeded = not bool(db)
        remote_exists = False
        remote_fields = {}
        if db:
            # This is deliberately before the SQLite insert and before /start
            # decides whether referral attribution is allowed.
            remote_succeeded, remote_exists, remote_fields = _fetch_firebase_user(
                user_id
            )
        data = _merge_remote_user(local_data, remote_fields) if remote_exists else local_data
        _store_user_local(user_id_str, data)
        if remote_succeeded:
            with _user_first_access_state_lock:
                _user_first_access_state[user_id_str] = (
                    "existing" if (local_exists or remote_exists) else "new"
                )
                _user_firestore_retry_needed.discard(user_id_str)
            if db and not remote_exists:
                _firebase_set_user_fields(
                    user_id,
                    _user_fields_for_firebase(data),
                )
        else:
            with _user_first_access_state_lock:
                _user_first_access_state[user_id_str] = (
                    "existing" if local_exists else "unknown_new"
                )
                if db:
                    _user_firestore_retry_needed.add(user_id_str)
        with user_cache_lock:
            user_cache[user_id_str] = data
        if db and not remote_succeeded:
            _queue_user_firestore_retry(user_id, data)
        return data

def update_balance(user_id, amount):
    try:
        amount = round(float(amount), 4)
    except (TypeError, ValueError):
        logger.warning("update_balance ignored invalid amount %r for user %s", amount, user_id)
        return False
    if amount < 0:
        # Debits must go through the atomic, sufficient-balance check.
        return deduct_balance(user_id, -amount)
    with _user_load_lock(user_id):
        user_id_str = str(user_id)
        with user_cache_lock:
            if user_id_str in user_cache:
                user_cache[user_id_str]['balance'] = round(
                    user_cache[user_id_str].get('balance', 0.0) + amount,
                    4,
                )

        conn = get_db()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET balance = ROUND(balance + ?, 4) WHERE user_id = ?",
                (amount, user_id_str),
            )
            conn.commit()
        finally:
            conn.close()
        _firebase_increment_user(user_id, balance=amount)
    return True


def deduct_balance(user_id, amount):
    """Atomically deduct amount only when the current balance is sufficient."""
    try:
        amount = round(float(amount), 4)
    except (TypeError, ValueError):
        return False
    if amount <= 0:
        return False

    user_id_str = str(user_id)
    with _user_load_lock(user_id):
        conn = get_db()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET balance = ROUND(balance - ?, 4) "
                "WHERE user_id = ? AND balance + 0.000001 >= ?",
                (amount, user_id_str, amount),
            )
            changed = cursor.rowcount == 1
            if changed:
                conn.commit()
                with user_cache_lock:
                    if user_id_str in user_cache:
                        user_cache[user_id_str]["balance"] = max(
                            0.0,
                            round(
                                float(user_cache[user_id_str].get("balance", 0.0))
                                - amount,
                                4,
                            ),
                        )
            else:
                conn.rollback()
        finally:
            conn.close()
        if changed:
            _firebase_increment_user(user_id, balance=-amount)
        return changed


def _attach_start_referral(inviter_id, new_user_id, is_new_local_user):
    """Remember who invited a brand-new user (SQLite is the source of truth).

    The local write is done first and does not depend on Firestore, so a slow
    or failing Firestore can no longer lose the referral.  Only a user that is
    new to the bot (is_new_local_user) can ever be attributed to an inviter.
    """
    if not is_new_local_user:
        return False
    try:
        inviter_id = int(inviter_id)
        new_user_id = int(new_user_id)
    except (TypeError, ValueError):
        return False
    if inviter_id == new_user_id:
        return False

    attached = False
    try:
        conn = get_db()
        try:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO users "
                "(user_id, balance, total_refers, total_otps, banned, verified) "
                "VALUES (?, 0, 0, 0, 0, 0)",
                (str(new_user_id),),
            )
            cursor = conn.execute(
                "UPDATE users SET referred_by = ?, ref_paid = 0 "
                "WHERE user_id = ? AND referred_by IS NULL "
                "AND (ref_paid = 0 OR ref_paid IS NULL)",
                (str(inviter_id), str(new_user_id)),
            )
            attached = cursor.rowcount > 0
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.error("Error saving local referral for user %s: %s", new_user_id, e)
        return False

    with user_cache_lock:
        cached = user_cache.get(str(new_user_id))
        if cached is not None and attached:
            cached["referred_by"] = str(inviter_id)
            cached["ref_paid"] = False

    if attached and db:
        # Best-effort mirror to Firestore; never blocks or breaks the referral.
        _firebase_set_user_fields(
            new_user_id,
            {"referred_by": int(inviter_id), "ref_paid": False},
        )
    return attached

def _process_pending_referral(user_id):
    """Pay the inviter once, right after the new user got the welcome message."""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT referred_by, ref_paid FROM users WHERE user_id = ?",
            (str(user_id),),
        )
        row = cursor.fetchone()
        referred_by = row["referred_by"] if row else None
        ref_paid = bool(row["ref_paid"]) if row and row["ref_paid"] is not None else False

        if not referred_by or ref_paid:
            return False

        try:
            inviter_id = int(referred_by)
        except (TypeError, ValueError):
            return False
        inviter_key = str(inviter_id)
        if inviter_key == str(user_id):
            return False
        reward = round(float(bot_settings.get("refer_reward", 0.2)), 4)

        # Atomic claim + payout in ONE transaction: only the thread whose
        # UPDATE flips ref_paid 0 -> 1 (rowcount == 1) may pay the inviter.
        try:
            cursor.execute(
                "UPDATE users SET ref_paid = 1 "
                "WHERE user_id = ? AND (ref_paid = 0 OR ref_paid IS NULL)",
                (str(user_id),),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return False
            cursor.execute(
                "INSERT OR IGNORE INTO users "
                "(user_id, balance, total_refers, total_otps, banned, verified) "
                "VALUES (?, 0, 0, 0, 0, 0)",
                (inviter_key,),
            )
            cursor.execute(
                "UPDATE users SET balance = ROUND(balance + ?, 4), "
                "total_refers = total_refers + 1 "
                "WHERE user_id = ?",
                (reward, inviter_key),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        with user_cache_lock:
            if str(user_id) in user_cache:
                user_cache[str(user_id)]["ref_paid"] = True
            if inviter_key in user_cache:
                user_cache[inviter_key]["balance"] = round(
                    user_cache[inviter_key].get("balance", 0.0) + reward,
                    4,
                )
                user_cache[inviter_key]["total_refers"] = user_cache[inviter_key].get("total_refers", 0) + 1
        if db:
            _firebase_set_user_fields(user_id, {"ref_paid": True})
            _firebase_increment_user(inviter_id, balance=reward, total_refers=1)

        ref_msg = (
            f"{PEM['gift']} <b>New Referral !</b>\n"
            f"{ANIMATED_BANNER}\n"
            f"🔥 <b>You Received {fmt_money(reward)} TK</b>\n"
            f"{ANIMATED_BANNER}\n"
            f"{PEM['user']} <b>From User ID:</b> <code>{user_id}</code>"
        )
        rendered_ref_msg = render_body_text(ref_msg)
        send_result = send_message(inviter_id, rendered_ref_msg)
        send_result = send_result if isinstance(send_result, dict) else {}
        logger.info(
            "Referral notification result inviter_id=%s ok=%s error_code=%s "
            "description=%s rendered_text_prefix=%r",
            inviter_id,
            send_result.get("ok"),
            send_result.get("error_code"),
            send_result.get("description"),
            rendered_ref_msg[:300],
        )
        return True
    except Exception as e:
        logger.error("Error processing pending referral for user %s: %s", user_id, e)
        return False
    finally:
        conn.close()

def sync_users_list():
    global all_known_users
    try:
        conn = get_db()
        try:
            cursor = conn.cursor()
        
            # Check if users table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            table_exists = cursor.fetchone()
        
            if table_exists:
                # Check if user_id column exists
                cursor.execute("PRAGMA table_info(users)")
                columns = [col[1] for col in cursor.fetchall()]
            
                if 'user_id' in columns:
                    cursor.execute('SELECT user_id FROM users')
                    with user_cache_lock:
                        all_known_users = set(row['user_id'] for row in cursor.fetchall())
                else:
                    logger.error(
                        "Users table is missing required user_id column; "
                        "leaving its data untouched."
                    )
                    return
            else:
                # Create users table if not exists
                cursor.execute('''
                    CREATE TABLE users (
                        user_id TEXT PRIMARY KEY,
                        balance REAL DEFAULT 0,
                        total_refers INTEGER DEFAULT 0,
                        total_otps INTEGER DEFAULT 0,
                        banned INTEGER DEFAULT 0,
                        verified INTEGER DEFAULT 0,
                        referred_by TEXT,
                        ref_paid INTEGER DEFAULT 0
                    )
                ''')
                conn.commit()
                all_known_users = set()
                logger.info("Users table created successfully")
            
        finally:
            conn.close()

        # Match store.py: when the local user list is empty, rebuild it from
        # Firestore so broadcasts can still reach previously registered users.
        if not all_known_users and db:
            try:
                firebase_user_ids = [
                    str(doc.id) for doc in db.collection("users").select([]).stream(timeout=5)
                ]
                if firebase_user_ids:
                    conn = get_db()
                    try:
                        conn.executemany(
                            "INSERT OR IGNORE INTO users "
                            "(user_id, balance, total_refers, total_otps, banned, verified) "
                            "VALUES (?, 0.0, 0, 0, 0, 0)",
                            [(uid,) for uid in firebase_user_ids],
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    with user_cache_lock:
                        all_known_users.update(firebase_user_ids)
            except Exception as e:
                logger.warning("Could not rebuild the local user list from Firestore: %s", e)

        logger.info(f"Synced {len(all_known_users)} users")
    except Exception as e:
        logger.error(f"Error syncing users: {e}")
        all_known_users = set()

def register_user_local(uid):
    uid_str = str(uid)
    # get_user() runs first in /start and records whether this ID existed in
    # SQLite or Firestore before creating a local placeholder row.
    with _user_first_access_state_lock:
        first_access_state = _user_first_access_state.pop(uid_str, None)
        is_new_user = first_access_state == "new"
        # A missing/uncertain classification must never trigger a referral
        # reward; consuming the state makes eligibility one-shot.
    with user_cache_lock:
        all_known_users.add(uid_str)
    return is_new_user

all_known_users = set()
threading.Thread(target=sync_users_list, daemon=True).start()

def periodic_user_sync():
    while True:
        time.sleep(300)  # Sync every 5 minutes
        sync_users_list()

threading.Thread(target=periodic_user_sync, daemon=True).start()
# ==========================================
# Telegram API & Helpers
# ==========================================
tg_session = requests.Session()


class _KeepAliveAdapter(requests.adapters.HTTPAdapter):
    """HTTPAdapter that enables TCP keep-alive on every pooled socket.

    Idle pooled connections were silently dropped by NAT/firewalls, so the
    first request after a quiet period hung until the read timeout
    ("Read timed out. (read timeout=8)") and buttons looked dead.  TCP
    keep-alive keeps the sockets alive / detects dead ones early.
    """

    def _socket_options(self):
        import socket
        opts = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
        for name, value in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 4)):
            if hasattr(socket, name):
                opts.append((socket.IPPROTO_TCP, getattr(socket, name), value))
        return opts

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["socket_options"] = self._socket_options()
        super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)


tg_adapter = _KeepAliveAdapter(
    pool_connections=32,
    pool_maxsize=64,
    max_retries=0,
)
tg_session.mount("https://", tg_adapter)
tg_session.mount("http://", tg_adapter)
rate_limit_lock = threading.RLock()
last_request_time = 0


def _wait_for_telegram_rate_slot():
    global last_request_time
    with rate_limit_lock:
        now = time.monotonic()
        delay = 0.033 - (now - last_request_time)
        if delay > 0:
            time.sleep(delay)
        last_request_time = time.monotonic()


def api_call(method, payload=None):
    global last_polling_error, _polling_conflict_alert_sent
    # Do not hold the rate-limit lock while Telegram is processing a request.
    # In particular, getUpdates is a long poll; holding the lock here would
    # block every button and message response until the poll timed out.
    url = f"{BASE_URL}/{method}"
    # (connect, read) timeouts: a dead connection fails fast on connect, while
    # the read timeout still allows Telegram to answer slow requests.
    if method == "getUpdates":
        timeout = (5, 65)
    elif method == "getChatMember":
        timeout = (3.05, 5)
    else:
        timeout = (3.05, 8)
    # Safe to repeat: re-sending them cannot duplicate anything for the user.
    retryable_reads = {
        "getMe", "getUpdates", "getChatMember", "getFile",
        "editMessageText", "editMessageReplyMarkup", "answerCallbackQuery",
        "deleteMessage", "sendChatAction",
    }
    for attempt in range(3):
        _wait_for_telegram_rate_slot()
        try:
            res = tg_session.post(url, json=payload, timeout=timeout)
            try:
                result = res.json()
            except ValueError:
                result = {}

            error_code = result.get("error_code", res.status_code)
            description = str(result.get("description", ""))
            if method == "getUpdates" and error_code == 409:
                last_polling_error = "409 Conflict: another getUpdates poller is active"
                logger.error(
                    "Telegram getUpdates returned 409 Conflict; another bot "
                    "instance is polling. pid=%s instance_id=%s",
                    os.getpid(),
                    INSTANCE_ID,
                )
                with _polling_conflict_lock:
                    should_alert = not _polling_conflict_alert_sent
                    _polling_conflict_alert_sent = True
                if should_alert:
                    try:
                        send_message(
                            OWNER_ID,
                            "🚨 <b>একাধিক বট ইনস্ট্যান্স চলছে</b>\n"
                            "Telegram getUpdates returned 409 Conflict. "
                            "Workspace Run বন্ধ রাখুন; শুধু একটিমাত্র Reserved VM/"
                            "Background Worker চালু রাখুন।",
                        )
                    except Exception:
                        logger.exception("Could not send the 409 owner warning")
            if error_code == 429 or res.status_code == 429:
                if attempt < 2:
                    retry_after = (
                        (result.get("parameters") or {}).get("retry_after")
                        or res.headers.get("Retry-After")
                        or 1
                    )
                    try:
                        time.sleep(max(0.0, float(retry_after)))
                    except (TypeError, ValueError):
                        time.sleep(1.0)
                    continue

            if (
                method in {"editMessageText", "editMessageReplyMarkup"}
                and "message is not modified" in description.lower()
            ):
                return {"ok": True, "result": True, "ignored": True}

            if not result.get("ok", False):
                if method == "getUpdates":
                    last_polling_error = (
                        f"Telegram API {error_code}: "
                        f"{description or 'unknown polling error'}"
                    )
                logger.error(
                    "Telegram API rejected %s: %s",
                    method,
                    _redact_secret(description or "unknown Telegram API error"),
                )
            return result
        except requests.RequestException as exc:
            if method == "getUpdates":
                last_polling_error = f"{type(exc).__name__}: {exc}"
            # A connect failure means the request never reached Telegram, so
            # it is safe to retry any method (including sendMessage).
            never_sent = isinstance(exc, requests.exceptions.ConnectTimeout) or (
                isinstance(exc, requests.exceptions.ConnectionError)
                and not isinstance(exc, requests.exceptions.ReadTimeout)
                and "RemoteDisconnected" not in str(exc)
                and "Connection aborted" not in str(exc)
            )
            if (method in retryable_reads or never_sent) and attempt < 2:
                time.sleep(0.15 * (2 ** attempt))
                continue
            logger.error("API call error (%s): %s", method, _redact_secret(exc))
            return {}
        except Exception as exc:
            if method == "getUpdates":
                last_polling_error = f"{type(exc).__name__}: {exc}"
            logger.error("API call error (%s): %s", method, _redact_secret(exc))
            return {}
    return {}


def download_telegram_file(file_id, max_bytes=MAX_NUMBER_UPLOAD_BYTES):
    """Download a Telegram file, retrying getFile and file transfer up to 3 times."""
    last_error = "Telegram file download failed"
    for attempt in range(3):
        try:
            file_info = api_call("getFile", {"file_id": file_id})
            if not file_info.get("ok"):
                last_error = str(
                    file_info.get("description", "Telegram getFile failed")
                )
                raise RuntimeError(last_error)
            file_path = (file_info.get("result") or {}).get("file_path")
            if not file_path:
                last_error = "Telegram did not return a file path"
                raise RuntimeError(last_error)
            response = tg_session.get(f"{FILE_URL}{file_path}", timeout=20)
            response.raise_for_status()
            content = response.content
            if max_bytes is not None and len(content) > max_bytes:
                raise ValueError(
                    f"Downloaded file exceeds the {max_bytes // (1024 * 1024)} MB limit"
                )
            return content
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            last_error = str(exc)
            if attempt < 2:
                delay = 0.5 * (2 ** attempt)
                logger.warning(
                    "Telegram file download retry %s/3 after %s: %s",
                    attempt + 1,
                    delay,
                    type(exc).__name__,
                )
                time.sleep(delay)
    raise RuntimeError(f"Could not download file after 3 attempts: {last_error}")

def send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id, 
        "text": text, 
        "parse_mode": parse_mode, 
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    result = api_call("sendMessage", payload)

    # Track every cancelable admin prompt, including prompts sent from
    # multi-step input handlers.
    try:
        if (
            chat_id in user_states
            and is_admin(chat_id)
            and isinstance(reply_markup, dict)
            and "cancel" in repr(reply_markup).lower()
            and result.get("result", {}).get("message_id")
        ):
            admin_prompt_messages[chat_id] = result["result"]["message_id"]
            if safe_get_user_state(chat_id) in ADMIN_INTERACTION_STATES:
                _queue_interaction_state(chat_id, safe_get_user_state(chat_id))
    except Exception as e:
        logger.warning("Could not track admin prompt for chat %s: %s", chat_id, e)
    return result


def alert_owner_if_data_encryption_key_missing():
    """Alert the owner once when no usable encryption key is available."""
    global _DATA_ENCRYPTION_KEY_ALERT_SENT
    if _FERNET is not None:
        return False
    if _DATA_ENCRYPTION_KEY_ALERT_SENT:
        return False
    _DATA_ENCRYPTION_KEY_ALERT_SENT = True
    try:
        result = send_message(
            OWNER_ID,
            "⚠️ <b>Security warning:</b> No usable panel-encryption key could "
            "be loaded or created. Panel credentials may be stored unencrypted.",
        )
        if not isinstance(result, dict) or not result.get("ok"):
            logger.error(
                "Could not deliver the DATA_ENCRYPTION_KEY startup alert to the owner."
            )
            return False
        logger.warning(
            "Owner was alerted that DATA_ENCRYPTION_KEY is missing."
        )
        return True
    except Exception:
        logger.exception(
            "Could not send the DATA_ENCRYPTION_KEY startup alert to the owner."
        )
        return False


def alert_owner_if_previous_encrypted_data_key_was_lost():
    """Tell the owner once when old encrypted credentials cannot be recovered."""
    global _LEGACY_ENCRYPTED_DATA_ALERT_SENT
    if not _ENCRYPTED_DATA_KEY_WAS_REGENERATED:
        return False
    if _LEGACY_ENCRYPTED_DATA_ALERT_SENT:
        return False
    _LEGACY_ENCRYPTED_DATA_ALERT_SENT = True
    try:
        result = send_message(
            OWNER_ID,
            "⚠️ সতর্কতা: ডেটাবেসে আগে এনক্রিপ্ট করা প্যানেল পাসওয়ার্ড আছে, "
            "কিন্তু পুরনো কী ফাইল পাওয়া যায়নি। নতুন কী তৈরি হয়েছে; পুরনো "
            "সেভ করা প্যানেল পাসওয়ার্ড আর পড়া যাবে না। অনুগ্রহ করে পাসওয়ার্ড "
            "আবার দিন।",
        )
        if not isinstance(result, dict) or not result.get("ok"):
            logger.error(
                "Could not deliver the old encrypted panel-credential warning to the owner."
            )
            return False
        logger.warning(
            "Owner was alerted that previously encrypted panel passwords must be entered again."
        )
        return True
    except Exception:
        logger.exception(
            "Could not send the old encrypted panel-credential warning to the owner."
        )
        return False


def send_photo(chat_id, photo_url_or_file_id, caption="", reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id, 
        "photo": photo_url_or_file_id, 
        "caption": caption, 
        "parse_mode": parse_mode
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return api_call("sendPhoto", payload)

def edit_message(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    result = api_call("editMessageText", payload)
    if is_admin(chat_id) and (
        chat_id in user_states
        or admin_prompt_messages.get(chat_id) == message_id
    ):
        try:
            if (
                isinstance(reply_markup, dict)
                and "cancel" in repr(reply_markup).lower()
                and chat_id in user_states
            ):
                admin_prompt_messages[chat_id] = message_id
                if safe_get_user_state(chat_id) in ADMIN_INTERACTION_STATES:
                    _queue_interaction_state(chat_id, safe_get_user_state(chat_id))
            elif admin_prompt_messages.get(chat_id) == message_id:
                # The prompt was reused to display a result or admin menu.
                admin_prompt_messages.pop(chat_id, None)
        except Exception as e:
            logger.warning("Could not update admin prompt tracking for chat %s: %s", chat_id, e)
    return result

def delete_message(chat_id, message_id):
    try:
        result = api_call(
            "deleteMessage",
            {"chat_id": chat_id, "message_id": message_id},
        )
        if (
            result.get("ok")
            and getattr(admin_interaction_context, "input_key", None)
            == (chat_id, message_id)
        ):
            admin_interaction_context.input_deleted = True
        if not result.get("ok"):
            logger.warning(
                "Could not delete Telegram message %s in chat %s: %s",
                message_id,
                chat_id,
                result.get("description", "unknown Telegram API error"),
            )
        return result
    except Exception as exc:
        logger.warning(
            "Could not delete Telegram message %s in chat %s: %s",
            message_id,
            chat_id,
            _redact_secret(exc),
        )
        return {}

callback_answer_lock = threading.RLock()
callback_answer_states = {}
CALLBACK_ACK_TIMEOUT_SECONDS = 0.5
CALLBACK_ACK_STATE_TTL_SECONDS = 15 * 60
callback_ack_executor = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="callback-ack",
)

def _prune_callback_answer_states(now=None):
    now = time.monotonic() if now is None else now
    expired = [
        callback_id
        for callback_id, state in callback_answer_states.items()
        if now - state["created_at"] > CALLBACK_ACK_STATE_TTL_SECONDS
    ]
    for callback_id in expired:
        callback_answer_states.pop(callback_id, None)
    if len(callback_answer_states) > 4096:
        oldest = sorted(
            callback_answer_states.items(),
            key=lambda item: item[1]["created_at"],
        )
        for callback_id, _ in oldest[:len(callback_answer_states) - 4096]:
            callback_answer_states.pop(callback_id, None)


def _cleanup_runtime_state():
    """Bound all long-lived caches from one shared periodic cleanup task."""
    now = time.monotonic()
    with callback_answer_lock:
        _prune_callback_answer_states(now)
    with _callback_payload_lock:
        for token, (expiry, _kind, _values) in list(_callback_payloads.items()):
            if expiry <= now:
                _callback_payloads.pop(token, None)
    expired_sessions = []
    wall_now = time.time()
    with sessions_lock:
        for user_id, session in list(user_active_sessions.items()):
            expiry = session.get("expires_at")
            if expiry is None:
                expiry = float(session.get("created_at", wall_now)) + ACTIVE_USER_SESSION_TTL_SECONDS
            if expiry <= wall_now:
                expired_sessions.append(
                    (user_id, user_active_sessions.pop(user_id))
                )
    for user_id, session in expired_sessions:
        mark_number_usage_released(user_id, session.get("nums", []))
    for bounded in (
        user_states, temp_data, admin_prompt_messages,
        admin_interaction_updated_at, user_cooldowns, panel_sessions,
    ):
        bounded.sweep(force=True)
    # Expire active prompts after 30 minutes, retaining a one-shot notice so
    # the user's next message is never silently swallowed.
    for chat_id in list(admin_interaction_updated_at):
        updated_at = admin_interaction_updated_at.get(chat_id)
        if (
            updated_at is None
            or time.time() - updated_at <= ADMIN_INTERACTION_TTL_SECONDS
        ):
            continue
        chat_lock = get_handler_lock(chat_id)
        if not chat_lock.acquire(blocking=False):
            continue
        try:
            if safe_get_user_state(chat_id) in PERSISTED_INTERACTION_STATES:
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                with expired_interaction_notifications_lock:
                    expired_interaction_notifications.add(chat_id)
        finally:
            chat_lock.release()
    with force_join_cache_lock:
        for key, item in list(force_join_cache.items()):
            ttl = (
                FORCE_JOIN_MISSING_CACHE_SECONDS
                if item.get("missing") or item.get("has_errors")
                else FORCE_JOIN_JOINED_CACHE_SECONDS
            )
            if now - item.get("time", 0) > ttl:
                force_join_cache.pop(key, None)
    with user_banned_cache_lock:
        for key, item in list(user_banned_cache.items()):
            if time.time() - item.get("time", 0) > 120:
                user_banned_cache.pop(key, None)
    with force_join_admin_status_cache_lock:
        for key, item in list(force_join_admin_status_cache.items()):
            if now - item.get("time", 0) > 120:
                force_join_admin_status_cache.pop(key, None)
    with force_join_invite_creation_lock:
        for key, item in list(force_join_invite_creation_cache.items()):
            if now - item.get("time", 0) > 120:
                force_join_invite_creation_cache.pop(key, None)
    with force_join_metadata_cache_lock:
        for key, item in list(force_join_metadata_cache.items()):
            if now - item.get("time", 0) > 3600:
                force_join_metadata_cache.pop(key, None)
    with force_join_warning_lock:
        if len(force_join_warning_keys) > 5000:
            force_join_warning_keys.clear()
    with _owner_error_alert_lock:
        if len(_owner_error_alert_keys) > 2000:
            _owner_error_alert_keys.clear()
    schedule_once(60, _cleanup_runtime_state)


def _auto_answer_callback(callback_id):
    try:
        callback_ack_executor.submit(_auto_answer_callback_now, callback_id)
    except RuntimeError:
        logger.warning("Callback acknowledgement worker is shutting down")


def _auto_answer_callback_now(callback_id):
    with callback_answer_lock:
        state = callback_answer_states.get(callback_id)
        if not state or state["answered"]:
            return
        state["answered"] = True
        state["auto_acked"] = True
    _answer_callback_request(callback_id, "", False)

def begin_callback_ack(callback):
    callback_id = callback.get("id")
    if not callback_id or callback_id == "internal":
        return
    message = callback.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    now = time.monotonic()
    with callback_answer_lock:
        _prune_callback_answer_states(now)
        state = callback_answer_states.get(callback_id)
        if state is None:
            callback_answer_states[callback_id] = {
                "chat_id": chat_id,
                "created_at": now,
                "answered": False,
                "auto_acked": False,
            }
    schedule_once(CALLBACK_ACK_TIMEOUT_SECONDS, _auto_answer_callback, callback_id)

def answer_callback(callback_id, text="", show_alert=False):
    if not callback_id or callback_id == "internal":
        return {}

    fallback_chat_id = None
    with callback_answer_lock:
        _prune_callback_answer_states()
        state = callback_answer_states.get(callback_id)
        if state and state["answered"]:
            if state["auto_acked"] and text and state.get("chat_id"):
                fallback_chat_id = state["chat_id"]
            else:
                return {"ok": True, "result": True}
        elif state:
            state["answered"] = True

    if fallback_chat_id is not None:
        return send_message(
            fallback_chat_id,
            html.escape(str(text)),
            parse_mode="HTML",
        )
    return _answer_callback_request(callback_id, text, show_alert)

def _answer_callback_request(callback_id, text="", show_alert=False):
    """answerCallbackQuery with one retry; a lost ack leaves Loading spinning."""
    payload = {
        "callback_query_id": callback_id,
        "text": text,
        "show_alert": bool(show_alert),
    }
    result = api_call("answerCallbackQuery", payload)
    if isinstance(result, dict) and result.get("ok"):
        return result
    description = str((result or {}).get("description", "")).lower() if isinstance(result, dict) else ""
    # An expired/invalid query can never be answered; retrying is pointless.
    if "too old" in description or "invalid" in description or "timeout expired" in description:
        return result
    time.sleep(0.2)
    return api_call("answerCallbackQuery", payload)

def send_document(chat_id, filename, content):
    url = f"{BASE_URL}/sendDocument"
    files = {'document': (filename, content)}
    data = {'chat_id': chat_id}
    try:
        result = tg_session.post(url, data=data, files=files, timeout=15)
        if not result.ok:
            logger.error("Send document HTTP error: %s", result.status_code)
        return result.json()
    except Exception as e:
        logger.error(f"Send document error: {e}")
        return {}

_broadcast_rate_lock = threading.RLock()
_last_broadcast_request = 0.0


def wait_for_broadcast_slot():
    """Limit broadcasts globally to about 15 Telegram sends per second."""
    global _last_broadcast_request
    with _broadcast_rate_lock:
        delay = (1 / 15) - (time.monotonic() - _last_broadcast_request)
        if delay > 0:
            time.sleep(delay)
        _last_broadcast_request = time.monotonic()


def send_broadcast_request(method, payload):
    """Retry a broadcast request when Telegram returns 429."""
    result = {}
    for attempt in range(3):
        wait_for_broadcast_slot()
        result = api_call(method, payload)
        if result.get("ok"):
            return result
        error_code = result.get("error_code")
        if error_code != 429 or attempt == 2:
            return result
        retry_after = (result.get("parameters") or {}).get("retry_after", 1)
        try:
            delay = max(0.0, float(retry_after))
        except (TypeError, ValueError):
            delay = 1.0
        logger.warning(
            "Broadcast hit Telegram 429; retrying after %.2fs",
            delay,
        )
        time.sleep(delay)
    return result


def _broadcast_to_users(users, method, payload_for_user):
    sent = 0
    failed = 0
    blocked = 0
    for user_id in users:
        try:
            result = send_broadcast_request(method, payload_for_user(user_id))
            if result.get("ok"):
                sent += 1
            elif result.get("error_code") == 403:
                blocked += 1
                logger.info("Broadcast blocked by user=%s", user_id)
            else:
                failed += 1
                logger.warning(
                    "Broadcast failed user=%s method=%s code=%s reason=%s",
                    user_id,
                    method,
                    result.get("error_code", "unknown"),
                    result.get("description", "unknown"),
                )
        except Exception:
            failed += 1
            logger.exception(
                "Broadcast exception user=%s method=%s",
                user_id,
                method,
            )
    return sent, failed, blocked


def broadcast_copymessage(from_chat_id, msg_id):
    with user_cache_lock:
        users = list(all_known_users)
    sent, failed, blocked = _broadcast_to_users(
        users,
        "copyMessage",
        lambda user_id: {
            "chat_id": user_id,
            "from_chat_id": from_chat_id,
            "message_id": msg_id,
        },
    )
    # Keep the admin panel clean after the source message has been copied.
    delete_message(from_chat_id, msg_id)
    send_message(from_chat_id, render_body_text(
        f"📢 <b>Broadcast Completed!</b>\n✅ Sent: {sent}\n"
        f"❌ Failed: {failed}\n🚫 Blocked: {blocked}\n"
        f"👥 Total: {len(users)}"
    ))


def broadcast_text_message(admin_id, text):
    with user_cache_lock:
        users = list(all_known_users)
    sent, failed, blocked = _broadcast_to_users(
        users,
        "sendMessage",
        lambda user_id: {
            "chat_id": user_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )
    send_message(
        admin_id,
        render_body_text(
            f"📢 <b>Stock announcement completed.</b>\n"
            f"✅ Sent: {sent}\n❌ Failed: {failed}\n"
            f"🚫 Blocked: {blocked}\n👥 Total: {len(users)}"
        ),
    )

def render_body_text(text):
    if not text:
        return str(text)
    # Replace long static box-drawing separators with the same premium
    # animated banner used by the referral screen.  This also covers saved
    # custom menu text and broadcast templates created before this update.
    text = re.sub(r"━{4,}", ANIMATED_BANNER, str(text))
    parts = re.split(r'(<tg-emoji.*?</tg-emoji>)', text)
    for i in range(len(parts)):
        if not parts[i].startswith('<tg-emoji'):
            for normal_emj, prem_id in GLOBAL_BODY_EMOJIS.items():
                if normal_emj in parts[i]:
                    parts[i] = parts[i].replace(normal_emj, f'<tg-emoji emoji-id="{prem_id}">{normal_emj}</tg-emoji>')
    return "".join(parts)

def extract_premium_html(msg):
    text = msg.get("text", msg.get("caption", ""))
    entities = msg.get("entities", msg.get("caption_entities", []))
    if not entities:
        return text
    try:
        b_text = text.encode('utf-16-le')
        c_entities = [e for e in entities if e.get("type") == "custom_emoji"]
        c_entities.sort(key=lambda x: x["offset"], reverse=True)
        for ent in c_entities:
            offset = ent["offset"] * 2
            length = ent["length"] * 2
            eid = ent["custom_emoji_id"]
            emoji_char = b_text[offset:offset+length].decode('utf-16-le')
            html_tag = f'<tg-emoji emoji-id="{eid}">{emoji_char}</tg-emoji>'
            replacement = html_tag.encode('utf-16-le')
            b_text = b_text[:offset] + replacement + b_text[offset+length:]
        return b_text.decode('utf-16-le')
    except Exception as e:
        logger.error(f"Extract premium HTML error: {e}")
        return text 

def get_flag_info_from_num(num):
    clean = re.sub(r"\D", "", str(num or ""))
    candidates = []
    for key, data in bot_settings.get("premium_flags", {}).items():
        if not isinstance(data, dict):
            continue
        code = str(data.get("code") or (key if str(key).isdigit() else ""))
        if code.isdigit():
            candidates.append((code, data))
    for code, data in sorted(candidates, key=lambda item: len(item[0]), reverse=True):
        if clean.startswith(code):
            return data.get("char", "🌍"), data.get("iso", "XX"), data.get("id")
    return "🌍", "XX", None

def get_flag_and_code(num):
    char, iso, _ = get_flag_info_from_num(num)
    return char, iso

def get_flag_info_html(num_or_iso, return_full_name=False):
    if len(str(num_or_iso)) == 2:
        for code, data in bot_settings.get("premium_flags", {}).items():
            if str(data.get("iso", "")).upper() == str(num_or_iso).upper():
                eid = data.get("id")
                char = data.get("char")
                name = data.get("name", num_or_iso)
                if return_full_name:
                    return name
                if eid:
                    return f'<tg-emoji emoji-id="{eid}">{char}</tg-emoji>'
                return char
        if return_full_name:
            return num_or_iso
        return "🌍"
        
    char, _, eid = get_flag_info_from_num(num_or_iso)
    if return_full_name:
        clean = re.sub(r"\D", "", str(num_or_iso or ""))
        for key, data in bot_settings.get("premium_flags", {}).items():
            code = str(data.get("code") or (key if str(key).isdigit() else ""))
            if code and clean.startswith(code):
                return data.get("name", num_or_iso)
        return num_or_iso
        
    if eid:
        return f'<tg-emoji emoji-id="{eid}">{char}</tg-emoji>'
    return char

def mask_number(num):
    clean = num.replace("+", "").replace(" ", "")
    if len(clean) > 6:
        return f"{clean[:3]}WAO{clean[-3:]}"
    elif len(clean) > 2:
        return f"{clean[:1]}WAO{clean[-1:]}"
    return clean

LANG_MAP = {
    "#EN": "English", "#BN": "Bengali", "#AR": "Arabic", "#HI": "Hindi", 
    "#PA": "Punjabi", "#GU": "Gujarati", "#OR": "Odia", "#TA": "Tamil", 
    "#TE": "Telugu", "#KN": "Kannada", "#ML": "Malayalam", "#SI": "Sinhala", 
    "#TH": "Thai", "#LO": "Lao", "#BO": "Tibetan", "#MY": "Burmese", 
    "#AM": "Amharic", "#KM": "Khmer", "#KA": "Georgian", "#HY": "Armenian", 
    "#HE": "Hebrew", "#EL": "Greek", "#RU": "Russian", "#ZH": "Chinese", 
    "#JA": "Japanese", "#KO": "Korean", "#ID": "Indonesian", "#MS": "Malay", 
    "#VN": "Vietnamese", "#TL": "Filipino", "#ES": "Spanish", "#PT": "Portuguese", 
    "#FR": "French", "#DE": "German", "#IT": "Italian", "#PL": "Polish", 
    "#TR": "Turkish", "#NL": "Dutch", "#SV": "Swedish", "#DA": "Danish", 
    "#NO": "Norwegian", "#FI": "Finnish", "#CS": "Czech", "#SK": "Slovak", 
    "#HU": "Hungarian", "#RO": "Romanian", "#HR": "Croatian", "#BG": "Bulgarian", 
    "#UK": "Ukrainian", "#SW": "Swahili", "#AF": "Afrikaans"
}

SERVICE_SMS_KEYWORDS = {
    "whatsapp": ["whatsapp", "whatsa", "whatsap", "whats", "whatsapp business", "whatsapp me", "whatsapp code", "whatsap", "واتساب", "واتساپ", "واٹس ایپ", "व्हाट्सएप", "वाट्सएप", "वॉट्सऐप", "व्हाट्सप्प", "হোয়াটসঅ্যাপ", "হোটসঅ্যাপ", "ватсап", "уотсап", "вотсап", "ватс апп", "వాట్సాప్", "വാട്‌സ്ആപ്പ്", "வாட்ஸ்அப்", "ವಾಟ್ಸಾಪ್", "વોટ્સએપ", "ਵਟਸਐਪ", "ହ୍ଵାଟସ୍ ଆପ୍", "වට්ස්ඇප්", "วอตส์แอปป์", "วอทส์แอพ", "ဝက်စ်အက်ပ်", "វ៉តសាប់", "ວອດແອັບ", "ワッツアップ", "왓츠앱", "whatsapp的", "whatsapp验证码", "וואטסאפ", "γουάτσαπ", "ዋትስአፕ", "ვოთსაფი", "վոթսափ"],
    "facebook": ["facebook", "fb", "meta", "fbook", "fb code", "facebook code", "فيسبوك", "فيس بوك"],
    "instagram": ["instagram", "insta", "ig", "ig code", "instagram code", "انستغرام", "انستقرام"],
    "telegram": ["telegram", "tg", "tele", "telegram code", "tg code", "t.me", "تيليجرام", "تليجرام"],
    "tiktok": ["tiktok", "tik tok", "tikvideo", "tiktok code", "tik code", "تيك توك"],
    "snapchat": ["snapchat", "snap", "snap code", "سناب شات"],
    "twitter": ["twitter", "x.com", "x code", "twitter code", "تويتر"],
    "discord": ["discord", "discord code", "ديسكورد"],
    "viber": ["viber", "viber code", "فايبر"],
    "line": ["line", "line code", "line verification", "لاين"],
    "wechat": ["wechat", "we chat", "wechat code", "وي تشات"],
    "signal": ["signal", "signal code", "سيجنال"],
    "linkedin": ["linkedin", "linked in", "لينكد إن"],
    "imo": ["imo", "imo code", "imo verification", "ايمو"],
    "kakaotalk": ["kakao", "kakaotalk", "كاكاو"],
    "qq": ["qq", "tencent qq"],
    "vk": ["vk", "vkontakte"],
    "google": ["google", "gmail", "youtube", "g-", "google voice", "جوجل", "غوغل"],
    "microsoft": ["microsoft", "ms", "outlook", "live.com", "hotmail"],
    "apple": ["apple", "icloud", "itunes", "apple id"],
    "yahoo": ["yahoo", "yahoo code", "ymail"],
    "protonmail": ["proton", "protonmail"],
    "binance": ["binance", "bnb", "binances"],
    "coinbase": ["coinbase"],
    "okx": ["okx", "okex"],
    "kucoin": ["kucoin"],
    "bybit": ["bybit"],
    "huobi": ["huobi", "htx"],
    "mexc": ["mexc"],
    "trustwallet": ["trust wallet", "trustwallet"],
    "bkash": ["bkash", "b-kash", "bkash code"],
    "nagad": ["nagad", "nagad code"],
    "rocket": ["rocket", "dutch bangla"],
    "upay": ["upay", "upay code"],
    "paypal": ["paypal", "pay pal"],
    "paytm": ["paytm"],
    "cashapp": ["cash app", "cashapp"],
    "wise": ["wise", "transferwise"],
    "amazon": ["amazon", "amzn", "amazon code"],
    "ebay": ["ebay"],
    "aliexpress": ["aliexpress", "ali express"],
    "alibaba": ["alibaba"],
    "daraz": ["daraz", "daraz code"],
    "foodpanda": ["foodpanda", "food panda"],
    "uber": ["uber", "uber code", "uber verification", "uber eats"],
    "pathao": ["pathao", "pathao ride"],
    "netflix": ["netflix", "netflix code"],
    "spotify": ["spotify", "spotify code"],
    "steam": ["steam", "steam guard"],
    "epicgames": ["epic games", "epicgames"],
    "roblox": ["roblox", "roblox code"],
    "riotgames": ["riot", "riot games", "valorant", "league of legends"],
    "garena": ["garena", "free fire", "freefire"],
    "playstation": ["playstation", "psn"],
    "1xbet": ["1xbet", "1x bet"],
    "melbet": ["melbet", "melbet code"],
    "linebet": ["linebet"],
    "bet365": ["bet365"],
    "megapari": ["megapari"],
    "tinder": ["tinder", "tinder code"],
    "bumble": ["bumble"],
    "badoo": ["badoo"]
}

# ===== SERVICE DETECTION v2 START (word-boundary / hint-first / default emoji) =====
from functools import lru_cache

# Admin-uploaded premium_apps এ ম্যাচ না পেলে (বা id না থাকলে) এই Default Premium Emoji ব্যবহার হবে।
# কোনো সার্ভিসের (Apple সহ) ID এখানে বানানো হয় না।
DEFAULT_SERVICE_EMOJI_ID = "6111799373434197692"
DEFAULT_SERVICE_EMOJI_CHAR = "📱"

# প্যানেল JSON/টেবিলের যে ফিল্ডগুলো সার্ভিস/সেন্ডার নির্দেশ করে (অগ্রাধিকার ক্রমে)
SERVICE_HINT_FIELDS = (
    "sender", "senderid", "sender_id", "sender_name",
    "cli", "service", "service_name", "app", "app_name", "application", "originator",
)
SERVICE_HINT_HEADER_RE = re.compile(r"\b(?:sender|cli|service|app|originator)\b", re.I)

# canonical SERVICE KEY -> alias নাম (premium_apps ডাটাবেসে খোঁজার জন্য)
SERVICE_ALIASES = {
    "WHATSAPP": ["WhatsApp", "WhatsApp Business", "WhatsApp Businesses", "WA"],
    "FACEBOOK": ["Facebook", "FB", "Meta", "Facebook Lite"],
    "INSTAGRAM": ["Instagram", "IG", "Insta"],
    "TELEGRAM": ["Telegram", "TG"],
    "TIKTOK": ["TikTok", "Tik Tok"],
    "SNAPCHAT": ["Snapchat", "Snap"],
    "TWITTER": ["Twitter", "X"],
    "GOOGLE": ["Google", "Gmail", "YouTube", "Google Voice"],
    "MICROSOFT": ["Microsoft", "Outlook", "Hotmail"],
    "APPLE": ["Apple", "Apple ID", "iCloud"],
    "KAKAOTALK": ["KakaoTalk", "Kakao"],
    "WECHAT": ["WeChat", "We Chat"],
    "LINKEDIN": ["LinkedIn"],
    "PROTONMAIL": ["ProtonMail", "Proton"],
    "TRUSTWALLET": ["Trust Wallet"],
    "CASHAPP": ["Cash App"],
    "WISE": ["Wise", "TransferWise"],
    "RIOTGAMES": ["Riot Games", "Riot", "Valorant"],
    "GARENA": ["Garena", "Free Fire"],
    "EPICGAMES": ["Epic Games"],
    "PLAYSTATION": ["PlayStation", "PSN"],
    "HUOBI": ["Huobi", "HTX"],
    "BKASH": ["bKash"],
    "ROCKET": ["Rocket", "Dutch Bangla"],
    "ALIEXPRESS": ["AliExpress"],
    "FOODPANDA": ["Foodpanda"],
}

# ছোট/অস্পষ্ট কীওয়ার্ড: মেসেজে প্রসঙ্গ (context) ছাড়া ম্যাচ হবে না। সেন্ডার/hint ফিল্ডে শুধু word-boundary লাগে।
_AMBIGUOUS_SERVICE_KEYWORDS = frozenset({
    "fb", "ig", "tg", "ms", "line", "snap", "signal", "meta", "wise", "rocket",
    "tele", "imo", "whats", "whatsa",
})

_CTX_WORDS = (
    r"(?:app\s+)?(?:code|otp|pin|passcode|password|verification|verify|security|login|"
    r"log-in|log\s+in|sign-in|signin|authentication|confirmation|account|one-time|"
    r"is\s+your|will\s+never)"
)


def _build_ctx_regex(core):
    b = rf"(?<![a-z0-9]){core}(?![a-z0-9])"
    parts = [
        rf"[\[\(<]\s*{core}\s*[\]\)>]",                              # [LINE] (Signal) <Meta>
        rf"^[\W_]*{core}(?![a-z0-9])\s*[:\-\u2013\u2014|>]",         # "LINE: 123456"
        rf"{b}\s*{_CTX_WORDS}\b",                                    # "Line code", "Signal account"
        rf"\b(?:code|otp|pin|verification|login|account|password)\s+(?:[\w\-]+\s+){{0,2}}(?:for|on|from|to|at)\s+(?:your\s+)?{b}",
        rf"\b(?:welcome\s+to|(?:log|sign)\s*in\s+to|login\s+to)\s+{b}",
    ]
    return re.compile("|".join(parts), re.M)


def _build_service_patterns():
    patterns = []
    for service_key, keywords in SERVICE_SMS_KEYWORDS.items():
        for kw in keywords:
            k = str(kw).strip().lower()
            if not k:
                continue
            ctx = None
            if k == "g-":
                # Google কোড ফরম্যাট: "G-123456"
                plain = re.compile(r"(?<![a-z0-9])g-\d{4,8}(?!\d)")
            elif k.isascii():
                core = re.escape(k).replace(r"\ ", r"[\s_\-]*")
                plain = re.compile(rf"(?<![a-z0-9]){core}(?![a-z0-9])")
                if k in _AMBIGUOUS_SERVICE_KEYWORDS:
                    ctx = _build_ctx_regex(core)
            else:
                plain = re.compile(re.escape(k))   # আরবি/বাংলা ইত্যাদি: সাবস্ট্রিং
            patterns.append((service_key, k, plain, ctx))
    return patterns


_SERVICE_PATTERNS = _build_service_patterns()


def detect_service_match(text, hint_mode=False):
    """(SERVICE_KEY, matched_keyword) অথবা None।

    * ASCII কীওয়ার্ডে word-boundary ("online"→LINE নয়, "meta data"→FACEBOOK নয়)।
    * ছোট/অস্পষ্ট কীওয়ার্ড মেসেজে প্রসঙ্গসহ হলেই ম্যাচ; hint_mode=True (সেন্ডার ফিল্ড) এ boundary যথেষ্ট।
    * একাধিক ম্যাচ হলে সবচেয়ে দীর্ঘ/নির্দিষ্ট কীওয়ার্ড, টাই হলে আগে আসা।
    """
    if not text:
        return None
    t = str(text).lower()
    best = None
    for service_key, kw, plain, ctx in _SERVICE_PATTERNS:
        rx = plain if (hint_mode or ctx is None) else ctx
        m = rx.search(t)
        if not m:
            continue
        score = (len(kw), -m.start())
        if best is None or score > best[0]:
            best = (score, service_key.upper(), kw)
    return (best[1], best[2]) if best else None


def detect_service(text, hint_mode=False):
    match = detect_service_match(text, hint_mode=hint_mode)
    return match[0] if match else None


# ---------- তালিকায় না থাকা সার্ভিসের নাম মেসেজ থেকে বের করা ----------
_NAME_GENERIC = frozenset({
    "your", "the", "a", "an", "new", "secret", "temporary", "one-time", "onetime", "one", "time",
    "otp", "code", "pin", "passcode", "password", "verification", "verify", "security", "login",
    "log-in", "sign-in", "signin", "confirmation", "authentication", "activation", "validation",
    "registration", "reset", "access", "account", "sms", "auth", "dynamic", "single-use", "single",
    "use", "digit", "transaction", "message", "mobile", "phone", "number", "text", "unique",
})
_NAME_REJECT_WORDS = frozenset({
    "is", "are", "was", "has", "have", "to", "for", "of", "and", "or", "you", "it", "this", "that",
    "not", "do", "please", "dear", "hi", "hello", "enter", "will", "be", "never", "share", "with",
    "anyone", "expires", "valid", "minutes", "min", "sent", "received", "notice", "alert", "info",
    "important", "urgent", "ad", "promo",
})
_NAME_PAT = r"[A-Za-z][\w.\-]{1,24}(?:[ ][A-Za-z][\w.\-]{1,24}){0,2}"
_NAME_RX_YOUR = re.compile(r"\byour\s+(.{1,40}?)\s+(?:code|otp|pin|passcode|password)\b", re.I)
_NAME_RX_COLON = re.compile(rf"^[\W_]*({_NAME_PAT})\s*:\s*\d{{3,8}}\b")
_NAME_RX_START = re.compile(
    rf"^[\W_]*({_NAME_PAT})\s+(?:(?:verification|security|login|confirmation|one-time)\s+)?"
    rf"(?:code|otp|pin)\b|^[\W_]*({_NAME_PAT})\s+is\s+your\b", re.I)
_NAME_RX_BRACKET = re.compile(r"\[([^\[\]\n]{2,25})\]")


def _clean_service_name(raw):
    if not raw:
        return None
    s = re.sub(r"\s+", " ", str(raw)).strip(" \t.:-_[]()<>#*")
    toks = [t for t in s.split(" ") if t]

    def _generic(tok):
        tl = tok.lower()
        return tl in _NAME_GENERIC or bool(re.fullmatch(r"\d+-?digits?", tl))

    while toks and _generic(toks[0]):
        toks.pop(0)
    while toks and _generic(toks[-1]):
        toks.pop()
    if not toks or len(toks) > 3:
        return None
    name = " ".join(toks)
    if len(name) < 2 or len(name) > 28 or not re.search(r"[A-Za-z]", name):
        return None
    if re.search(r"\d{4,}", name) or re.search(r"https?|www\.|@", name, re.I):
        return None
    if not re.fullmatch(r"[\w.\-+ ]+", name):
        return None
    if any(t.lower() in _NAME_REJECT_WORDS for t in toks):
        return None
    return name if any(c.isupper() for c in name) else name.title()


def extract_service_name_from_message(msg_text):
    """"Your X code is", "X: 123456", "[X]" ধরনের মেসেজ থেকে সার্ভিসের নাম। না পেলে None।"""
    if not msg_text:
        return None
    text = re.sub(r"^\s*<#>\s*", "", str(msg_text)).strip()
    m = _NAME_RX_YOUR.search(text)
    if m:
        name = _clean_service_name(m.group(1))
        if name:
            return name
    for rx in (_NAME_RX_COLON, _NAME_RX_START):
        m = rx.search(text)
        if m:
            name = _clean_service_name(next((g for g in m.groups() if g), ""))
            if name:
                return name
    for m in _NAME_RX_BRACKET.finditer(text):
        name = _clean_service_name(m.group(1))
        if name:
            return name
    return None


# ---------- premium_apps (অ্যাডমিন-আপলোড) ডাটাবেসে খোঁজা ----------
def _clean_service_hint(value):
    if value is None or isinstance(value, (dict, list, tuple, set, bool)):
        return ""
    s = re.sub(r"\s+", " ", str(value)).strip()
    if not s or len(s) > 40 or not re.search(r"[A-Za-z\u0080-\uffff]", s):
        return ""
    return s


@lru_cache(maxsize=2048)
def _db_name_service(app_name):
    """premium_apps এর নামটি নিজে কোন পরিচিত সার্ভিস (না থাকলে None)।"""
    return detect_service(app_name, hint_mode=True)


def _premium_apps_snapshot():
    apps = bot_settings.get("premium_apps", {}) or {}
    snap = []
    for name, data in list(apps.items()):
        key = _service_lookup_key(name)
        if key and isinstance(data, dict):
            snap.append((name, data, key))
    return snap


def _lookup_premium_candidates(candidates, expect_key=None):
    """candidates ক্রমানুসারে: সম্পূর্ণ-মিল > দীর্ঘতম-অন্তর্ভুক্তি।

    অন্তর্ভুক্তি-ম্যাচে দুই পক্ষই কমপক্ষে ৪ অক্ষর হতে হবে। expect_key জানা থাকলে
    অন্য পরিচিত সার্ভিসের রেকর্ড (যেমন LINEBET বনাম LINE) বাদ যায়।
    """
    snap = _premium_apps_snapshot()
    if not snap:
        return None
    for cand in candidates:
        ck = _service_lookup_key(cand)
        if not ck:
            continue
        best, best_score = None, -1
        for name, data, ak in snap:
            if ck == ak:
                score = 100000 + len(ak)
            elif len(ck) >= 4 and len(ak) >= 4 and (ck in ak or ak in ck):
                if expect_key:
                    other = _db_name_service(name)
                    if other and other != expect_key:
                        continue
                score = len(ak)
            else:
                continue
            if score > best_score:
                best, best_score = data, score
        if best is not None:
            return best
    return None


@lru_cache(maxsize=2048)
def _name_boundary_regex(app_name):
    toks = [t for t in re.split(r"[^a-z0-9]+", str(app_name).lower()) if t]
    if not toks:
        return None
    return re.compile(r"(?<![a-z0-9])" + r"[\s_\-]*".join(map(re.escape, toks)) + r"(?![a-z0-9])")


def _find_premium_app_in_text(msg_text):
    """মেসেজে সরাসরি উল্লেখ থাকা অ্যাডমিন-আপলোড অ্যাপ (word-boundary, দীর্ঘতম জয়ী)।"""
    t = str(msg_text).lower()
    best, best_len = None, 0
    for name, data, key in _premium_apps_snapshot():
        if len(key) < 4 or _db_name_service(name):
            continue          # পরিচিত সার্ভিস detect_service-ই সামলায়
        rx = _name_boundary_regex(name)
        if rx and rx.search(t) and len(key) > best_len:
            best, best_len = data, len(key)
    return best


def _display_service_name(service_key):
    aliases = SERVICE_ALIASES.get(service_key)
    return aliases[0] if aliases else str(service_key).title()


def resolve_service_info(service_text="", msg_text="", service_hint=None, panel_mode=False):
    """সার্ভিস শনাক্ত + emoji নির্বাচনের একমাত্র কেন্দ্র (Test Connection ও গ্রুপ মেসেজ দুটোই এটা ব্যবহার করে)।

    ক্রম: (১) service_hint (২) মেসেজ থেকে detect_service (৩) মেসেজ থেকে নাম/ডাটাবেস (৪) প্যানেলের নাম।
    panel_mode=True হলে service_text প্যানেলের নাম: শুধু ডাটাবেস-লুকআপে লাগে, সার্ভিস হিসেবে দেখানো হয় না।
    """
    hint = _clean_service_hint(service_hint)
    msg_text = msg_text or ""
    data, name, source, identified, matched_kw = None, None, None, None, None

    if hint:
        match = detect_service_match(hint, hint_mode=True)
        if match:
            identified, matched_kw = match
            source = "service_hint"
    if not identified and msg_text:
        match = detect_service_match(msg_text)
        if match:
            identified, matched_kw = match
            source = "message_detect"

    if identified:
        cands = [matched_kw, identified] + SERVICE_ALIASES.get(identified, [])
        data = _lookup_premium_candidates(cands, expect_key=identified)
        name = (data or {}).get("name") or _display_service_name(identified)
    else:
        if hint:
            data = _lookup_premium_candidates([hint])
            if data:
                source = "service_hint_db"
        if not data and msg_text:
            data = _find_premium_app_in_text(msg_text)
            if data:
                source = "message_db_name"
        if not data and msg_text:
            extracted = extract_service_name_from_message(msg_text)
            if extracted:
                data = _lookup_premium_candidates([extracted])
                name = (data or {}).get("name") or extracted
                source = "message_extracted_db" if data else "message_extracted"
        if not data and not name and service_text:
            data = _lookup_premium_candidates([service_text])
            if data:
                source = "panel_name_db"
            elif not panel_mode:
                st = re.sub(r"\s+", " ", str(service_text)).strip()
                if st and len(st) <= 30:
                    name, source = st.title(), "service_text"
        if data and not name:
            name = data.get("name")

    if not name:
        name = "Unknown"
    eid = str((data or {}).get("id") or "").strip()
    if re.fullmatch(r"\d{5,25}", eid):
        emoji_src = "premium_apps"
    else:
        eid, emoji_src = DEFAULT_SERVICE_EMOJI_ID, "default"
    char = (data or {}).get("char") or DEFAULT_SERVICE_EMOJI_CHAR
    logger.debug(
        "service-emoji: name=%r id=%s emoji=%s source=%s hint=%r panel=%r",
        name, eid, emoji_src, source or "none", hint, service_text,
    )
    return {
        "name": name, "data": data, "emoji_id": eid, "emoji_source": emoji_src,
        "source": source, "html": f'<tg-emoji emoji-id="{eid}">{char}</tg-emoji>',
    }
# ===== SERVICE DETECTION v2 END =====

def normalize_service_name(service):
    """Return one stable key for service names from every data source."""
    return re.sub(r"\s+", " ", str(service or "").strip()).upper()

def service_names_match(left, right):
    return bool(normalize_service_name(left)) and normalize_service_name(left) == normalize_service_name(right)

def _service_lookup_key(value):
    """Normalize service names for reliable premium-app matching.

    Provider names are not always formatted consistently: one source may send
    ``WhatsApp Businesses`` while another sends ``WHATSAPP_BUSINESSES``.
    Comparing a compact alphanumeric key keeps the uploaded emoji database
    usable across all of those sources.
    """
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())

def get_premium_app_data(service_text, msg_text="", service_hint=None):
    """Return the uploaded premium-app record (or None) using the shared resolver."""
    return resolve_service_info(service_text, msg_text, service_hint)["data"]

def get_premium_app_icon_id(service_text, default_id=None, msg_text=""):
    """Get the custom emoji ID used as a service/app button icon."""
    app_data = get_premium_app_data(service_text, msg_text)
    if app_data and app_data.get("id"):
        return str(app_data["id"])
    return str(default_id) if default_id else None

def is_service_disabled(service):
    target = normalize_service_name(service)
    return target in {
        normalize_service_name(item)
        for item in bot_settings.get("disabled_services", [])
        if normalize_service_name(item)
    }

def enable_service(service):
    """Allow a deliberately re-added service to appear in the user menu."""
    target = normalize_service_name(service)
    if not target:
        return
    with data_lock:
        disabled = bot_settings.setdefault("disabled_services", [])
        bot_settings["disabled_services"] = [
            item for item in disabled if normalize_service_name(item) != target
        ]

def remove_service_everywhere(service):
    """Remove a service from uploaded local stock."""
    target = normalize_service_name(service)
    if not target:
        return {"local": 0}

    removed = {"local": 0}
    with data_lock:
        for batch_id, batch in list(number_batches.items()):
            if service_names_match(batch.get("service"), target):
                del number_batches[batch_id]
                dirty_number_batches.discard(batch_id)
                deleted_number_batches.add(batch_id)
                removed["local"] += 1

        _rebuild_known_numbers()  # FIX: remove deleted service numbers from known set
        disabled = bot_settings.setdefault("disabled_services", [])
        if target not in {normalize_service_name(item) for item in disabled}:
            disabled.append(target)
        save_db(include_local=True)
    return removed

def get_service_info_html(service_text, msg_text="", service_hint=None, panel_mode=False):
    """(display name, <tg-emoji> html).  Used by group messages AND Test Connection.

    Order: service_hint -> detect_service(message) -> message-extracted name /
    uploaded premium_apps -> panel name (DB lookup only when panel_mode=True).
    No match / no emoji id -> DEFAULT_SERVICE_EMOJI_ID.
    """
    info = resolve_service_info(service_text, msg_text, service_hint, panel_mode)
    return info["name"], info["html"]

def detect_language(text):
    if not text:
        return "#EN"
    text_str = str(text)

    if any('\u0600' <= c <= '\u06ff' for c in text_str):
        return "#AR"
    if any('\u0980' <= c <= '\u09ff' for c in text_str):
        return "#BN"
    if any('\u0900' <= c <= '\u097f' for c in text_str):
        return "#HI"
    if any('\u0a00' <= c <= '\u0a7f' for c in text_str):
        return "#PA"
    if any('\u0a80' <= c <= '\u0aff' for c in text_str):
        return "#GU"
    if any('\u0b00' <= c <= '\u0b7f' for c in text_str):
        return "#OR"
    if any('\u0b80' <= c <= '\u0bff' for c in text_str):
        return "#TA"
    if any('\u0c00' <= c <= '\u0c7f' for c in text_str):
        return "#TE"
    if any('\u0c80' <= c <= '\u0cff' for c in text_str):
        return "#KN"
    if any('\u0d00' <= c <= '\u0d7f' for c in text_str):
        return "#ML"
    if any('\u0d80' <= c <= '\u0dff' for c in text_str):
        return "#SI"
    if any('\u0e00' <= c <= '\u0e7f' for c in text_str):
        return "#TH"
    if any('\u0e80' <= c <= '\u0eff' for c in text_str):
        return "#LO"
    if any('\u0f00' <= c <= '\u0fff' for c in text_str):
        return "#BO"
    if any('\u1000' <= c <= '\u109f' for c in text_str):
        return "#MY"
    if any('\u1200' <= c <= '\u137f' for c in text_str):
        return "#AM"
    if any('\u1780' <= c <= '\u17ff' for c in text_str):
        return "#KM"
    if any('\u10a0' <= c <= '\u10ff' for c in text_str):
        return "#KA"
    if any('\u0530' <= c <= '\u058f' for c in text_str):
        return "#HY"
    if any('\u0590' <= c <= '\u05ff' for c in text_str):
        return "#HE"
    if any('\u0370' <= c <= '\u03ff' for c in text_str):
        return "#EL"
    if any('\u0400' <= c <= '\u04ff' for c in text_str):
        return "#RU"
    if any('\u4e00' <= c <= '\u9fff' for c in text_str):
        return "#ZH"
    if any('\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff' for c in text_str):
        return "#JA"
    if any('\uac00' <= c <= '\ud7af' for c in text_str):
        return "#KO"

    text_lower = text_str.lower()
    
    if any(w in text_lower for w in ["kode verifikasi", "jangan bagikan", "rahasia"]):
        return "#ID"
    if any(w in text_lower for w in ["kod pengesahan", "jangan kongsi"]):
        return "#MS"
    if any(w in text_lower for w in ["mã của bạn", "không chia sẻ", "mã xác minh"]):
        return "#VN"
    if any(w in text_lower for w in ["ang iyong code", "huwag ibahagi"]):
        return "#TL"
    if any(w in text_lower for w in ["código", "tu código", "verificación", "no compartas"]):
        return "#ES"
    if any(w in text_lower for w in ["seu código", "código de verificação", "não compartilhe"]):
        return "#PT"
    if any(w in text_lower for w in ["code secret", "ne partagez pas", "votre code"]):
        return "#FR"
    if any(w in text_lower for w in ["dein code", "bestätigungscode", "nicht teilen"]):
        return "#DE"
    if any(w in text_lower for w in ["il tuo codice", "codice di verifica", "non condividere"]):
        return "#IT"
    if any(w in text_lower for w in ["twój kod", "nie udostępniaj", "kod weryfikacyjny"]):
        return "#PL"
    if any(w in text_lower for w in ["doğrulama kodu", "paylaşmayın", "onay kodu"]):
        return "#TR"
    if any(w in text_lower for w in ["jouw code", "verificatiecode", "niet delen"]):
        return "#NL"
    if any(w in text_lower for w in ["din kod", "verifieringskod", "dela inte"]):
        return "#SV"
    if any(w in text_lower for w in ["bekræftelseskode", "del ikke"]):
        return "#DA"
    if any(w in text_lower for w in ["bekreftelseskode", "ikke del"]):
        return "#NO"
    if any(w in text_lower for w in ["vahvistuskoodi", "älä jaa"]):
        return "#FI"
    if any(w in text_lower for w in ["váš kód", "ověřovací kód", "nesdílejte"]):
        return "#CS"
    if any(w in text_lower for w in ["overovací kód", "nezdieľajte"]):
        return "#SK"
    if any(w in text_lower for w in ["ellenőrző kód", "ne oszd meg"]):
        return "#HU"
    if any(w in text_lower for w in ["codul tău", "codul de verificare", "nu partaja"]):
        return "#RO"
    if any(w in text_lower for w in ["kontrolni kod", "kod za potvrdu", "ne delite"]):
        return "#HR"
    if any(w in text_lower for w in ["код за потвърждение", "не споделяйте"]):
        return "#BG"
    if any(w in text_lower for w in ["ваш код", "код підтвердження"]):
        return "#UK"
    if any(w in text_lower for w in ["msimbo wako", "usishiriki"]):
        return "#SW"
    if any(w in text_lower for w in ["verifikasiekode", "moenie deel nie"]):
        return "#AF"
    
    return "#EN"

def parse_chat_id(text):
    text = text.strip()
    if text.startswith("-100") or (text.startswith("-") and text[1:].isdigit()):
        return text
    if "t.me/" in text:
        parts = text.split("/")
        username = parts[-1]
        if username:
            return "@" + username if not username.startswith("@") else username
    if text.startswith("@"):
        return text
    return "@" + text


def mask_secret(value):
    """Single shared masker for API keys/passwords (admin messages, logs, URLs)."""
    v = str(value or "")
    if not v:
        return ""
    if len(v) <= 6:
        return "***"
    return f"{v[:3]}***{v[-2:]}"


def mask_url_secrets_for_display(value):
    """Hide credentials in API URLs before rendering them in Telegram HTML."""
    url = str(value or "")
    url = re.sub(r"(?i)(://[^/@:]+:)[^@/]+@", r"\1••••@", url)
    sensitive_query = re.compile(
        r"(?i)([?&](?:token|key|api[_-]?key|api[_-]?token|access[_-]?token|"
        r"client[_-]?secret|secret|password|auth)=)([^&#]*)"
    )

    def mask_parameter(match):
        value = match.group(2)
        if value.lower() in {
            "{token}", "{key}", "token", "key", "your_token", "your_key"
        }:
            return match.group(0)
        return match.group(1) + "••••"

    return sensitive_query.sub(mask_parameter, url)


def panel_api_configuration_text(panel):
    """Render API panel settings safely; never reveal credentials in chat."""
    name = html.escape(str(panel.get("name", "Provider")))
    panel_type = html.escape(str(panel.get("type", "API Panel")))
    status = "🟢 Monitoring" if panel.get("status") == "ON" else "🔴 Stopped"
    api_url = html.escape(
        mask_url_secrets_for_display(panel.get("api_url", "Not set"))
    )
    full_api_url = html.escape(
        mask_url_secrets_for_display(panel.get("full_api_url", "Not set"))
    )
    token = html.escape(mask_secret(panel.get("token")))
    return (
        f"⚙️ <b>Configure {name}</b>\n\n"
        f"<b>Type:</b> {panel_type}\n"
        f"<b>Status:</b> {status}\n"
        f"<b>API URL:</b> <code>{api_url}</code>\n"
        f"<b>Token:</b> <code>{token}</code>\n"
        f"<b>Full API URL:</b> <code>{full_api_url}</code>"
    )


def show_panel_api_configuration(chat_id, message_id, panel_index):
    """Show saved API settings, with a new-message fallback if edit fails."""
    with data_lock:
        if not isinstance(panel_index, int) or not (
            0 <= panel_index < len(bot_settings["panels"])
        ):
            panel = None
            markup = None
        else:
            panel = copy.deepcopy(bot_settings["panels"][panel_index])
            markup = panel_config_keyboard(panel_index)
    if panel is None:
        send_message(chat_id, "⚠️ This API panel no longer exists.")
        return None
    text = render_body_text(panel_api_configuration_text(panel))
    if message_id:
        result = edit_message(
            chat_id,
            message_id,
            text,
            reply_markup=markup,
        )
        if result and result.get("ok"):
            return result
        logger.warning(
            "Could not edit API panel configuration message chat=%s panel=%s",
            chat_id,
            panel_index,
        )
    return send_message(chat_id, text, reply_markup=markup)


def is_admin(user_id):
    return user_id in bot_settings["admins"] or user_id == OWNER_ID

def get_number_limit():
    """Return the safe per-request number limit used by all number flows."""
    try:
        return max(1, min(100, int(bot_settings.get("number_limit", bot_settings.get("num_req", 3)))))
    except (TypeError, ValueError):
        return 3

def get_change_number_cooldown():
    """Return the seconds a user must wait before changing a number."""
    try:
        return max(0, min(86400, int(bot_settings.get("change_number_cooldown", 3))))
    except (TypeError, ValueError):
        return 3

def _get_telegram_bot_identity(force_refresh=False):
    now = time.monotonic()
    with force_join_bot_id_lock:
        cached = dict(force_join_bot_id_cache)
        if (
            not force_refresh
            and cached.get("id")
            and now - cached.get("time", 0) < 3600
        ):
            return cached["id"], cached.get("username")

    result = api_call("getMe", {})
    bot = result.get("result", {}) if isinstance(result, dict) else {}
    bot_id = (
        bot.get("id")
        if isinstance(result, dict) and result.get("ok")
        else None
    )
    if bot_id:
        with force_join_bot_id_lock:
            force_join_bot_id_cache.update({
                "id": int(bot_id),
                "username": bot.get("username"),
                "time": time.monotonic(),
            })
        return int(bot_id), bot.get("username")
    return None, None


def _force_join_bot_admin_status(channel, force_refresh=False):
    channel = normalize_force_join_channel(channel)
    channel_id = channel.get("id")
    if not channel_id:
        return None, "Missing Telegram chat ID"
    now = time.monotonic()
    with force_join_admin_status_cache_lock:
        cached = force_join_admin_status_cache.get(str(channel_id))
        if (
            not force_refresh
            and cached
            and now - cached["time"] < 60
        ):
            return cached["is_admin"], cached.get("description", "")

    bot_id, _ = _get_telegram_bot_identity(force_refresh=force_refresh)
    if not bot_id:
        return None, "Could not identify this bot with getMe"
    result = api_call(
        "getChatMember",
        {"chat_id": channel_id, "user_id": bot_id},
    )
    member = result.get("result", {}) if isinstance(result, dict) else {}
    status = member.get("status")
    is_admin_status = bool(
        isinstance(result, dict)
        and result.get("ok")
        and status in {"administrator", "creator"}
    )
    known = bool(
        isinstance(result, dict)
        and result.get("ok")
        and status
    )
    if known:
        with force_join_admin_status_cache_lock:
            force_join_admin_status_cache[str(channel_id)] = {
                "is_admin": is_admin_status,
                "description": status,
                "time": time.monotonic(),
            }
        return is_admin_status, status
    return None, str(result.get("description", "Telegram could not check bot status"))


def _warn_force_join_api_error(channel, config_signature, reason):
    channel = normalize_force_join_channel(channel)
    warning_key = (tuple(config_signature), channel.get("id", ""))
    with force_join_warning_lock:
        if warning_key in force_join_warning_keys:
            return
        force_join_warning_keys.add(warning_key)

    label = channel.get("title") or channel.get("id") or "unknown channel"
    safe_reason = html.escape(str(reason or "Telegram API check failed"))
    logger.warning(
        "Force Join API error for channel %s; user checks will fail open: %s",
        channel.get("id") or label,
        reason,
    )
    message = (
        "⚠️ <b>Force Join check could not reach a channel.</b>\n"
        f"Channel: <code>{html.escape(str(label))}</code>\n"
        f"Configured ID: <code>{html.escape(str(channel.get('id') or 'missing'))}</code>\n"
        f"Telegram response: {safe_reason}\n"
        "This channel is skipped for membership checks until its configuration is fixed."
    )
    try:
        startup_executor.submit(send_message, OWNER_ID, render_body_text(message))
    except RuntimeError:
        logger.warning("Could not queue the one-time Force Join owner alert.")


def _warn_force_join_missing_invite(channel, reason):
    channel = normalize_force_join_channel(channel)
    warning_key = ("missing_invite", channel.get("id", ""))
    with force_join_warning_lock:
        if warning_key in force_join_warning_keys:
            return
        force_join_warning_keys.add(warning_key)
    message = (
        "⚠️ <b>Force Join needs an invite link.</b>\n"
        f"Channel: <b>{html.escape(channel.get('title') or channel.get('id') or 'unknown')}</b>\n"
        "No usable private-channel invite link could be created. Open Admin Panel → "
        "Force Join System → Set invite link, or add a valid link using "
        "<code>-100… | https://t.me/+…</code>.\n"
        f"Telegram response: {html.escape(str(reason or 'no invite link returned'))}"
    )
    try:
        startup_executor.submit(send_message, OWNER_ID, render_body_text(message))
    except RuntimeError:
        logger.warning("Could not queue the one-time missing-invite alert.")


def _ensure_force_join_invite_link(channel):
    """Create and persist a private invite link when legacy settings lack one."""
    channel = normalize_force_join_channel(channel)
    if channel.get("username"):
        return f"https://t.me/{channel['username']}"
    existing_link = channel.get("invite_link")
    if _valid_force_join_invite_url(existing_link):
        return existing_link
    channel_id = channel.get("id")
    if not channel_id:
        return None

    cache_key = str(channel_id)
    now = time.monotonic()
    with force_join_invite_creation_lock:
        cached = force_join_invite_creation_cache.get(cache_key)
        if cached and now - cached["time"] < 60:
            return cached.get("url")

    chat_result = api_call("getChat", {"chat_id": channel_id})
    chat = (
        chat_result.get("result", {})
        if isinstance(chat_result, dict) and chat_result.get("ok")
        else {}
    )
    if chat:
        channel["id"] = str(chat.get("id") or channel_id)
        channel["title"] = str(chat.get("title") or channel.get("title") or channel_id)
        channel["username"] = str(chat.get("username") or "").lstrip("@")
        chat_invite_link = chat.get("invite_link", "")
        if _valid_force_join_invite_url(chat_invite_link):
            channel["invite_link"] = chat_invite_link

        with data_lock:
            for index, configured in enumerate(bot_settings.get("fj_channels", [])):
                if normalize_force_join_channel(configured).get("id") == channel_id:
                    bot_settings["fj_channels"][index] = channel
                    save_db()
                    break
        if channel.get("username"):
            invalidate_force_join_cache()
            return f"https://t.me/{channel['username']}"
        if _valid_force_join_invite_url(channel.get("invite_link")):
            invalidate_force_join_cache()
            return channel["invite_link"]

    result = api_call(
        "createChatInviteLink",
        {"chat_id": channel_id, "name": "Force Join"},
    )
    invite_link = (
        result.get("result", {}).get("invite_link", "")
        if isinstance(result, dict) and result.get("ok")
        else ""
    )
    if not _valid_force_join_invite_url(invite_link):
        reason = (
            result.get("description", "Telegram returned no valid invite link")
            if isinstance(result, dict)
            else "Telegram returned an invalid response"
        )
        _warn_force_join_missing_invite(channel, reason)
        with force_join_invite_creation_lock:
            force_join_invite_creation_cache[cache_key] = {
                "url": None,
                "time": time.monotonic(),
            }
        return None

    with data_lock:
        for index, configured in enumerate(bot_settings.get("fj_channels", [])):
            normalized = normalize_force_join_channel(configured)
            if normalized.get("id") == channel_id:
                normalized["invite_link"] = invite_link
                bot_settings["fj_channels"][index] = normalized
                save_db()
                break
    invalidate_force_join_cache()
    with force_join_invite_creation_lock:
        force_join_invite_creation_cache[cache_key] = {
            "url": invite_link,
            "time": time.monotonic(),
        }
    return invite_link


def get_missing_force_join_channels(
    user_id,
    force_refresh=False,
    log_context="",
):
    """Return only channels confirmed as not joined; API errors fail open."""
    with data_lock:
        enabled = bool(bot_settings.get("fj_on"))
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    user_key = str(user_id)

    if not enabled:
        if log_context == "start":
            logger.info(
                "Force Join decision user=%s context=start result=fj_off",
                user_key,
            )
        return []
    if not channels:
        if log_context == "start":
            logger.info(
                "Force Join decision user=%s context=start result=no_channels",
                user_key,
            )
        return []
    if is_admin(user_id):
        if log_context == "start":
            logger.info(
                "Force Join decision user=%s context=start result=admin_bypass",
                user_key,
            )
        return []

    config_signature = tuple(
        force_join_channel_key(channel) for channel in channels
    )
    now = time.monotonic()
    if not force_refresh:
        with force_join_cache_lock:
            cached = force_join_cache.get(user_key)
            if cached and cached.get("channels") == config_signature:
                has_errors = bool(cached.get("has_errors"))
                ttl = (
                    FORCE_JOIN_MISSING_CACHE_SECONDS
                    if cached.get("missing") or has_errors
                    else FORCE_JOIN_JOINED_CACHE_SECONDS
                )
                if now - cached["time"] < ttl:
                    missing = list(copy.deepcopy(cached["missing"]))
                    if log_context == "start":
                        logger.info(
                            "Force Join decision user=%s context=start result=%s missing=%s api_errors=%s cached=true",
                            user_key,
                            "blocked" if missing else "allowed",
                            [
                                item.get("title") or item.get("id")
                                for item in missing
                            ],
                            cached.get("has_errors", False),
                        )
                    return missing

    def check_channel(channel):
        channel_id = channel.get("id")
        if not channel_id:
            return channel, "error", "No Telegram chat ID; configure the channel ID"
        try:
            result = api_call(
                "getChatMember",
                {"chat_id": channel_id, "user_id": user_id},
            )
        except Exception as exc:
            return channel, "error", f"Telegram request failed: {type(exc).__name__}"
        if not isinstance(result, dict) or not result.get("ok"):
            reason = (
                result.get("description", "Telegram getChatMember failed")
                if isinstance(result, dict)
                else "Telegram getChatMember returned an invalid response"
            )
            return channel, "error", reason
        member = result.get("result", {}) or {}
        status = member.get("status")
        if status in {"left", "kicked"}:
            return channel, "missing", status
        if status == "restricted":
            if member.get("is_member", False):
                return channel, "joined", status
            return channel, "missing", status
        if status in {"member", "administrator", "creator"}:
            return channel, "joined", status
        return channel, "error", f"Unrecognized member status: {status or 'missing'}"

    with ThreadPoolExecutor(max_workers=min(8, len(channels))) as pool:
        outcomes = list(pool.map(check_channel, channels))

    missing = []
    status_by_channel = {}
    for channel, state, description in outcomes:
        key = force_join_channel_key(channel)
        status_by_channel[key] = {
            "state": state,
            "description": description,
        }
        if state == "missing":
            missing.append(channel)
        elif state == "error":
            _warn_force_join_api_error(
                channel,
                config_signature,
                description,
            )

    cache_record = {
        "joined": not missing,
        "missing": tuple(missing),
        "statuses": status_by_channel,
        "channels": config_signature,
        "has_errors": any(state == "error" for _, state, _ in outcomes),
        "time": time.monotonic(),
    }
    with force_join_cache_lock:
        force_join_cache[user_key] = cache_record

    if log_context == "start":
        logger.info(
            "Force Join decision user=%s context=start result=%s missing=%s api_errors=%s cached=false",
            user_key,
            "blocked" if missing else "allowed",
            [item.get("title") or item.get("id") for item in missing],
            cache_record["has_errors"],
        )
    return copy.deepcopy(missing)


def check_force_join(user_id, log_context=""):
    return not get_missing_force_join_channels(
        user_id,
        log_context=log_context,
    )


def _force_join_channel_details(channel):
    normalized = normalize_force_join_channel(channel)
    label = normalized.get("title") or normalized.get("username") or normalized.get("id")
    username = normalized.get("username")
    if not username and str(normalized.get("id", "")).startswith("@"):
        username = str(normalized["id"])[1:]
    if username:
        url = f"https://t.me/{username}"
    else:
        url = normalized.get("invite_link")
    return label or "Channel", url if _valid_force_join_invite_url(url) else None


def _refresh_force_join_channel_metadata(channel, force_refresh=False):
    """Enrich migrated legacy IDs/usernames with their current chat title."""
    channel = normalize_force_join_channel(channel)
    channel_id = channel.get("id")
    if not channel_id:
        return channel
    fallback_titles = {
        channel_id,
        channel.get("username", ""),
        "@" + channel.get("username", "") if channel.get("username") else "",
    }
    if channel.get("title") not in fallback_titles:
        return channel

    now = time.monotonic()
    with force_join_metadata_cache_lock:
        cached = force_join_metadata_cache.get(channel_id)
        if (
            not force_refresh
            and cached
            and now - cached["time"] < 3600
        ):
            return copy.deepcopy(cached["channel"])

    result = api_call("getChat", {"chat_id": channel_id})
    chat = (
        result.get("result", {})
        if isinstance(result, dict) and result.get("ok")
        else {}
    )
    if chat:
        channel["id"] = str(chat.get("id") or channel_id)
        channel["title"] = str(chat.get("title") or channel["title"])
        channel["username"] = str(chat.get("username") or channel["username"]).lstrip("@")
        invite_link = chat.get("invite_link", "")
        if _valid_force_join_invite_url(invite_link):
            channel["invite_link"] = invite_link
        with data_lock:
            for index, configured in enumerate(bot_settings.get("fj_channels", [])):
                if normalize_force_join_channel(configured).get("id") == channel_id:
                    bot_settings["fj_channels"][index] = channel
                    save_db()
                    break
        invalidate_force_join_cache()

    with force_join_metadata_cache_lock:
        force_join_metadata_cache[channel_id] = {
            "channel": copy.deepcopy(channel),
            "time": time.monotonic(),
        }
    return channel


def build_force_join_prompt(chat_id):
    with data_lock:
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    channels = [
        _refresh_force_join_channel_metadata(channel)
        for channel in channels
    ]
    for channel in channels:
        if not channel.get("username") and not channel.get("invite_link"):
            _ensure_force_join_invite_link(channel)
    with data_lock:
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    missing = get_missing_force_join_channels(chat_id)
    missing_keys = {force_join_channel_key(channel) for channel in missing}
    with force_join_cache_lock:
        cached = force_join_cache.get(str(chat_id), {})
        status_by_channel = (
            cached.get("statuses", {})
            if cached.get("channels")
            == tuple(force_join_channel_key(channel) for channel in channels)
            else {}
        )

    kb = []
    status_lines = []
    for channel in channels:
        key = force_join_channel_key(channel)
        label, url = _force_join_channel_details(channel)
        channel_status = status_by_channel.get(key, {}).get("state", "error")
        if key in missing_keys:
            status_lines.append(f"❌ {html.escape(label)} — not joined")
        elif channel_status == "error":
            status_lines.append(
                f"⚠️ {html.escape(label)} — membership could not be checked"
            )
        else:
            status_lines.append(f"✅ {html.escape(label)} — joined")

        if url:
            kb.append([{
                "text": label[:64],
                "icon_custom_emoji_id": "5789428375261023681",
                "url": url,
                "style": "primary",
            }])
        elif key in missing_keys or channel_status == "error":
            status_lines[-1] += " (invite link not configured)"

    if missing:
        text = (
            f"{PEM['warn']} <b>Join the remaining channels to continue:</b>\n"
            + "\n".join(status_lines)
        )
        kb.append([{
            "text": "🔄 Refresh status",
            "icon_custom_emoji_id": "5352694861990501856",
            "callback_data": "refresh_fj",
            "style": "primary",
        }])
    else:
        text = (
            f"{PEM['ok']} <b>All required channels are joined or temporarily "
            "unavailable.</b>\nTap Verify to continue."
        )
        if status_lines:
            text += "\n" + "\n".join(status_lines)
        kb.append([{
            "text": "✅ Verify",
            "icon_custom_emoji_id": "5352694861990501856",
            "callback_data": "check_fj",
            "style": "success",
        }])
    return render_body_text(text), {"inline_keyboard": kb}, missing


def send_force_join_msg(chat_id, message_id=None):
    text, markup, _ = build_force_join_prompt(chat_id)
    if message_id:
        result = edit_message(chat_id, message_id, text, reply_markup=markup)
        if result and result.get("ok"):
            return result
    result = send_message(chat_id, text, reply_markup=markup)
    if result and result.get("ok"):
        return result

    logger.error(
        "Force Join prompt with buttons failed for user %s; sending plain-text fallback.",
        chat_id,
    )
    with data_lock:
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    fallback_lines = [
        "⚠️ <b>Could not display the Force Join buttons.</b>",
        "Join the configured channels using these links:",
    ]
    for channel in channels:
        label, url = _force_join_channel_details(channel)
        link_text = (
            html.escape(url)
            if url
            else "invite link unavailable — contact the bot admin"
        )
        fallback_lines.append(
            f"• {html.escape(label)}: {link_text}"
        )
    fallback_text = render_body_text("\n".join(fallback_lines))
    fallback = send_message(chat_id, fallback_text)
    if not fallback or not fallback.get("ok"):
        logger.error("Plain-text Force Join fallback also failed for user %s.", chat_id)
    return fallback


def add_force_join_channel_from_input(text):
    """Validate channel identity, permissions, and invite link before saving."""
    raw = str(text or "").strip()
    channel_input = raw
    supplied_invite = ""
    if "|" in raw:
        channel_input, supplied_invite = (
            part.strip() for part in raw.split("|", 1)
        )
        if not _valid_force_join_invite_url(supplied_invite):
            return False, (
                "❌ Invalid invite link. Use an HTTPS Telegram link such as "
                "<code>https://t.me/+…</code>."
            ), False

    candidate = normalize_force_join_channel(channel_input)
    if supplied_invite:
        candidate["invite_link"] = supplied_invite
    channel_id = candidate.get("id", "")
    if not channel_id:
        return False, (
            "❌ Send a channel ID or public username. For a private channel, "
            "send <code>-100… | https://t.me/+…</code>."
        ), False
    if channel_id.startswith("@"):
        if not re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{4,31}", channel_id):
            return False, "❌ Invalid channel username. Send @username or a channel ID.", False
    elif not re.fullmatch(r"-?\d+", channel_id):
        return False, "❌ Invalid channel ID. Send @username or a numeric ID.", False

    chat_result = api_call("getChat", {"chat_id": channel_id})
    chat = chat_result.get("result", {}) if isinstance(chat_result, dict) else {}
    if not isinstance(chat_result, dict) or not chat_result.get("ok") or not chat:
        reason = (
            chat_result.get("description", "Telegram could not find that channel")
            if isinstance(chat_result, dict)
            else "Invalid Telegram response"
        )
        return False, (
            "❌ getChat failed: "
            f"<code>{html.escape(str(reason))}</code>"
        ), False
    if chat.get("type") not in {"channel", "supergroup"}:
        return False, "❌ That chat is not a channel or supergroup.", False

    canonical_id = str(chat.get("id", "")).strip()
    if not canonical_id:
        return False, "❌ Telegram did not return a channel ID.", False
    username = str(
        chat.get("username") or candidate.get("username") or ""
    ).strip().lstrip("@")
    normalized = {
        "id": canonical_id,
        "title": str(chat.get("title") or candidate.get("title") or canonical_id),
        "username": username,
        "invite_link": "",
        "bot_is_admin": None,
    }

    with data_lock:
        existing = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    if any(
        old.get("id") == canonical_id
        or (username and old.get("username", "").lower() == username.lower())
        for old in existing
    ):
        return False, "⚠️ This channel is already in Force Join.", False

    bot_admin, bot_status = _force_join_bot_admin_status(
        normalized,
        force_refresh=True,
    )
    if bot_admin is not True:
        return False, (
            "❌ The bot must be an administrator of the channel "
            f"(current status: <code>{html.escape(str(bot_status or 'unknown'))}</code>). "
            "Promote the bot to admin, then try again."
        ), False
    normalized["bot_is_admin"] = True

    invite_link = candidate.get("invite_link") or chat.get("invite_link") or ""
    if invite_link and not _valid_force_join_invite_url(invite_link):
        invite_link = ""
    if not username and not invite_link:
        invite_result = api_call(
            "createChatInviteLink",
            {"chat_id": canonical_id, "name": "Force Join"},
        )
        invite = (
            invite_result.get("result", {}).get("invite_link", "")
            if isinstance(invite_result, dict) and invite_result.get("ok")
            else ""
        )
        if _valid_force_join_invite_url(invite):
            invite_link = invite
    if not username and not invite_link:
        return False, (
            "❌ Bot admin access is confirmed, but no invite link is available. "
            "Send the ID with an invite link: "
            "<code>-100… | https://t.me/+…</code>."
        ), False
    normalized["invite_link"] = invite_link

    with data_lock:
        channels = bot_settings.setdefault("fj_channels", [])
        if any(
            normalize_force_join_channel(item).get("id") == canonical_id
            for item in channels
        ):
            return False, "⚠️ This channel is already in Force Join.", False
        was_empty = not channels
        channels.append(normalized)
        if was_empty:
            bot_settings["fj_on"] = True
        save_db()
    invalidate_force_join_cache()
    return True, normalized["title"], was_empty


def force_join_diagnostic_text(admin_id):
    """Show live Telegram API results for each configured channel."""
    with data_lock:
        enabled = bool(bot_settings.get("fj_on"))
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    channels = [
        _refresh_force_join_channel_metadata(channel, force_refresh=True)
        for channel in channels
    ]
    lines = [
        "<b>Force Join diagnostic</b>",
        f"Enabled: <b>{'ON' if enabled else 'OFF'}</b>",
        f"Configured channels: <b>{len(channels)}</b>",
    ]
    if not channels:
        lines.append("No channels are configured.")
        return render_body_text("\n".join(lines))

    for index, channel in enumerate(channels, start=1):
        channel_id = channel.get("id") or "(missing ID)"
        label = channel.get("title") or channel_id
        lines.append(
            f"\n<b>{index}. {html.escape(str(label))}</b>\n"
            f"Saved value: <code>{html.escape(str(channel_id))}</code>"
        )
        if not channel.get("id"):
            lines.append("getChat: ❌ no channel ID configured")
            lines.append("Bot status: ❌ unavailable")
            lines.append("Your membership: ❌ unavailable")
            lines.append("Join URL: ⚠️ not configured")
            continue

        chat_result = api_call("getChat", {"chat_id": channel["id"]})
        chat = chat_result.get("result", {}) if isinstance(chat_result, dict) else {}
        if isinstance(chat_result, dict) and chat_result.get("ok") and chat:
            lines.append(
                "getChat: ✅ "
                + html.escape(str(chat.get("title") or "ok"))
            )
        else:
            reason = (
                chat_result.get("description", "failed")
                if isinstance(chat_result, dict)
                else "invalid response"
            )
            lines.append(f"getChat: ❌ {html.escape(str(reason))}")

        bot_admin, bot_status = _force_join_bot_admin_status(
            channel,
            force_refresh=True,
        )
        if bot_admin is True:
            lines.append(f"Bot status: ✅ {html.escape(str(bot_status))}")
        elif bot_admin is False:
            lines.append(f"Bot status: ⚠️ {html.escape(str(bot_status))}")
        else:
            lines.append(f"Bot status: ❌ {html.escape(str(bot_status))}")

        admin_result = api_call(
            "getChatMember",
            {"chat_id": channel["id"], "user_id": admin_id},
        )
        admin_member = (
            admin_result.get("result", {})
            if isinstance(admin_result, dict)
            else {}
        )
        if isinstance(admin_result, dict) and admin_result.get("ok"):
            lines.append(
                "Your membership: ✅ "
                + html.escape(str(admin_member.get("status", "unknown")))
            )
        else:
            reason = (
                admin_result.get("description", "failed")
                if isinstance(admin_result, dict)
                else "invalid response"
            )
            lines.append(f"Your membership: ❌ {html.escape(str(reason))}")

        _, join_url = _force_join_channel_details(channel)
        if join_url:
            safe_url = html.escape(join_url, quote=True)
            lines.append(
                f'Join URL: <a href="{safe_url}">{html.escape(join_url)}</a>'
            )
        else:
            lines.append("Join URL: ⚠️ not configured")
    return render_body_text("\n".join(lines))


def is_user_banned(user_id):
    if is_admin(user_id):
        return False
    user_key = str(user_id)
    now = time.time()
    # Never wait on Firestore while answering a button/message. Prefer the
    # user cache, then SQLite; Firestore is only a background refresh source.
    try:
        with user_cache_lock:
            cached_user = user_cache.get(user_key)
        local_user = cached_user if cached_user is not None else _read_local_user(user_key)
        with user_banned_cache_lock:
            previous = user_banned_cache.get(user_key)
            if local_user is not None:
                banned = bool(local_user.get("banned", False))
                user_banned_cache[user_key] = {
                    "banned": banned,
                    "time": previous.get("time", now - 60) if previous else now - 60,
                }
            elif previous and now - previous["time"] < 60:
                banned = bool(previous["banned"])
            else:
                banned = False
                user_banned_cache[user_key] = {"banned": False, "time": now - 60}
            should_refresh = bool(
                db
                and (not previous or now - previous.get("time", 0) >= 60)
                and user_key not in user_ban_refresh_inflight
            )
            if should_refresh:
                user_ban_refresh_inflight.add(user_key)
        if should_refresh:
            _submit_firebase_task(_refresh_ban_status_from_firebase, user_id)
        return banned
    except Exception:
        logger.exception("Could not read local ban status for user %s", user_id)
        with user_banned_cache_lock:
            previous = user_banned_cache.get(user_key)
            return bool(previous and previous.get("banned", False))


user_ban_refresh_inflight = set()


def _refresh_ban_status_from_firebase(user_id):
    user_key = str(user_id)
    try:
        succeeded, exists, remote_fields = _fetch_firebase_user(user_id)
        if not succeeded:
            with user_banned_cache_lock:
                item = user_banned_cache.get(user_key)
                if item is not None:
                    item["time"] = time.time()
            return
        if not exists or "banned" not in remote_fields:
            with user_banned_cache_lock:
                item = user_banned_cache.get(user_key)
                if item is not None:
                    item["time"] = time.time()
            return
        banned = bool(remote_fields["banned"])
        with user_cache_lock:
            cached_user = user_cache.get(user_key)
            if cached_user is not None:
                cached_user["banned"] = banned
        conn = get_db()
        try:
            conn.execute(
                "UPDATE users SET banned = ? WHERE user_id = ?",
                (int(banned), user_key),
            )
            conn.commit()
        finally:
            conn.close()
        with user_banned_cache_lock:
            user_banned_cache[user_key] = {
                "banned": banned,
                "time": time.time(),
            }
    except Exception:
        logger.exception("Background Firestore ban refresh failed for user %s", user_key)
        with user_banned_cache_lock:
            item = user_banned_cache.get(user_key)
            if item is not None:
                item["time"] = time.time()
    finally:
        with user_banned_cache_lock:
            user_ban_refresh_inflight.discard(user_key)

# ==========================================
# Captcha Auto Login & Parsing Core
# ==========================================
# ==========================================
# OTP Code Extraction (BEGIN OTP-EXTRACT)
# ==========================================
# Priority: (a) code next to an OTP keyword, (b) FB-/G-/<#> prefixes,
# (c) grouped "123-456" / "123 456", (d) last-resort 4-8 digits that do not
# look like a date, time, phone number or amount.
_OTP_INVISIBLE_RE = re.compile(
    "[\u00ad\u061c\u180e\u200b-\u200f\u202a-\u202e\u2060-\u2064\u2066-\u2069\ufeff]"
)
_OTP_SPACE_RE = re.compile("[\u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]")

_OTP_KEYWORDS = (
    r"(?:pass\s?codes?|codes?|otps?|pins?|verification|verify|confirmation|"
    r"authentication|authorization|auth|one[\s\-]?time|"
    r"কোড|ওটিপি|পিন|ভেরিফিকেশন|"
    r"الرمز|رمز|الكود|كود|التحقق|تحقق|"
    r"कोड|ओटीपी|पिन|सत्यापन|"
    r"код\w{0,3}|пароль|"
    r"código|codigo|clave|kod|kodu|kode|mã|รหัส)"
)
_OTP_KW_RE = re.compile(r"(?<!\w)" + _OTP_KEYWORDS + r"(?!\w)", re.I)

# Words/characters allowed between a keyword and its code ("code is: 123456").
_OTP_SEP = (
    r"(?:[\s:：=\-–—>→*\"'“”«»\[\(]|(?<!\w)(?:is|are|was|هو|هي|হলো|হল|है|es|é)(?!\w))*"
)
_OTP_GROUPED = r"(?<!\d)(?:\d{3}[-\s]\d{3}|\d{2}[-\s]\d{2}[-\s]\d{2})(?!\d)"
_OTP_TIGHT_RE = re.compile(
    _OTP_KW_RE.pattern + _OTP_SEP + r"(?P<tok>" + _OTP_GROUPED + r"|(?<![A-Za-z0-9])(?=[A-Za-z0-9]*\d)[A-Za-z0-9]{4,10}(?![A-Za-z0-9]))",
    re.I,
)
_OTP_STRONG_REVERSE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?P<tok>\d{4,10})(?![A-Za-z0-9])\s*[,:]?\s*"
    r"(?:(?:is|are)\s+(?:your|the|an?|ur)\b|as\s+your\b|(?:ist|est)\s+(?:dein|ihr|votre))",
    re.I,
)
_OTP_DIGIT_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9])\d{4,8}(?![A-Za-z0-9])")
# Plain 4-8 digit codes and grouped "123-456" / "12 34 56" codes, in text order.
_OTP_CANDIDATE_RE = re.compile(
    r"(?P<g>(?<![\d+])(?<!\d[-\s./])(?:\d{3}[-\s]\d{3}|\d{2}[-\s]\d{2}[-\s]\d{2})"
    r"(?!\d)(?![-\s./:,]\d))|(?P<d>(?<![A-Za-z0-9])\d{4,8}(?![A-Za-z0-9]))"
)
_OTP_PREFIX_RE = re.compile(r"(?<![A-Za-z0-9])(?:FB|G|IG|WA|TG)\s?[-–]\s?(?P<tok>\d{4,8})(?![A-Za-z0-9])")
_OTP_HASH_RE = re.compile(r"<#>")

_OTP_MONTHS = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)
_OTP_BAD_BEFORE_RE = re.compile(
    r"(?:\d[\s\-./,:)(]|[+$€£¥₹৳#@/\\]|"
    r"(?<![A-Za-z])" + _OTP_MONTHS + r"\.?\s*|"
    r"(?<![A-Za-z])(?<!\d)(?<!\d\s)(?:call|tel|telephone|phone|mobile|mob|fax|dial|ph|"
    r"ref|reference|order|invoice|txn|trx|transaction|tracking|id|account|acct|a/c|"
    r"card|ending|ends|amount|amt|balance|bal|total|price|rs\.?|inr|usd|bdt|tk\.?|"
    r"taka|aed|eur|ফোন|নম্বর|নাম্বার|অ্যাকাউন্ট)\s*(?:no\.?|number|num|#)?\s*[:#.\-]?\s*)$",
    re.I,
)
_OTP_BAD_AFTER_RE = re.compile(
    r"^(?:[-/.:,]\d|\s\d{3,4}(?!\d)|\s?(?:%|am\b|pm\b|hrs?\b|hours?\b|min(?:s|utes?)?\b|"
    r"sec(?:s|onds?)?\b|usd\b|bdt\b|tk\b|taka\b|rs\b|inr\b|eur\b|aed\b|dollars?\b|"
    r"৳|টাকা|রুপি|km\b|kg\b|gb\b|mb\b|kb\b)|\s?" + _OTP_MONTHS + r"\b)",
    re.I,
)
_OTP_DATE8_RE = re.compile(
    r"^(?:(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])|"
    r"(?:0[1-9]|[12]\d|3[01])(?:0[1-9]|1[0-2])(?:19|20)\d{2})$"
)


def _normalize_otp_text(text):
    """ASCII digits, no invisible characters, plain spaces, HTML entities decoded."""
    s = "" if text is None else str(text)
    if "&" in s:
        s = html.unescape(s)
    s = _OTP_INVISIBLE_RE.sub("", s)
    s = _OTP_SPACE_RE.sub(" ", s)
    return "".join(
        str(unicodedata.decimal(ch)) if unicodedata.category(ch) == "Nd" else ch
        for ch in s
    )


def _otp_digits_plausible(text, start, end):
    """False when a digit run looks like a date, time, phone, id or amount."""
    token = text[start:end]
    if len(token) == 8 and _OTP_DATE8_RE.match(token):
        return False
    before = text[max(0, start - 30):start]
    after = text[end:end + 20]
    if _OTP_BAD_BEFORE_RE.search(before):
        return False
    if _OTP_BAD_AFTER_RE.match(after):
        return False
    return True


def _otp_grouped_plausible(token, sep_text):
    """2-2-2 groups separated by '-' that read as DD-MM-YY are dates, not codes."""
    parts = re.findall(r"\d+", token)
    if len(parts) == 3 and "-" in sep_text:
        a, b, c = (int(x) for x in parts)
        if (1 <= b <= 12 and 1 <= a <= 31) or (1 <= a <= 12 and 1 <= b <= 31):
            return False
    return True


def _otp_candidate_ok(text, m):
    """Plausibility check for a match of _OTP_CANDIDATE_RE."""
    token = m.group(0)
    if m.group("g") is not None and not _otp_grouped_plausible(token, token):
        return False
    return _otp_digits_plausible(text, m.start(), m.end())


def _otp_format(token):
    """Whitespace is removed ('123 456' -> '123456'); '-' is kept as before."""
    return re.sub(r"\s+", "", token)


def extract_otp_code(text):
    clean = _normalize_otp_text(text)
    if not clean.strip():
        return None

    # (a1) keyword directly followed by the code: "code is 123456", "OTP: A1B2C3".
    for m in _OTP_TIGHT_RE.finditer(clean):
        tok = m.group("tok")
        if re.fullmatch(r"\d+", tok):
            if len(tok) <= 10:
                return tok
        elif re.fullmatch(r"\d{3}[-\s]\d{3}|\d{2}[-\s]\d{2}[-\s]\d{2}", tok):
            return _otp_format(tok)
        elif re.search(r"\d", tok) and re.search(r"[A-Za-z]", tok):
            return tok

    # (a2) "123456 is your Facebook confirmation code", "Use 123456 as your ...".
    for m in _OTP_STRONG_REVERSE_RE.finditer(clean):
        tok = m.group("tok")
        if len(tok) <= 8 and _otp_digits_plausible(clean, m.start("tok"), m.end("tok")):
            return tok

    # (a3) keyword followed (within a short window) by a valid 4-8 digit code.
    for kw in _OTP_KW_RE.finditer(clean):
        window_end = min(len(clean), kw.end() + 60)
        for t in _OTP_CANDIDATE_RE.finditer(clean, kw.end(), window_end):
            if _otp_candidate_ok(clean, t):
                return _otp_format(t.group(0))

    # (a4) valid 4-8 digit code followed shortly by a keyword.
    for t in _OTP_CANDIDATE_RE.finditer(clean):
        if not _otp_candidate_ok(clean, t):
            continue
        if _OTP_KW_RE.search(clean, t.end(), min(len(clean), t.end() + 70)):
            return _otp_format(t.group(0))

    # (b) provider prefixes: "FB-12345", "G-123456", "<#> ...".
    pm = _OTP_PREFIX_RE.search(clean)
    if pm:
        return pm.group("tok")
    hm = _OTP_HASH_RE.search(clean)
    if hm:
        for t in _OTP_CANDIDATE_RE.finditer(clean, hm.end(), min(len(clean), hm.end() + 80)):
            if _otp_candidate_ok(clean, t):
                return _otp_format(t.group(0))

    # (c) "123-456" / "123 456" / "12 34 56" with strict digit boundaries.
    for gm in re.finditer(
        r"(?<![\d+])(?<!\d[-\s./])(\d{3}[-\s]\d{3}|\d{2}[-\s]\d{2}[-\s]\d{2})(?!\d)(?![-\s./:,]\d)",
        clean,
    ):
        tok = gm.group(1)
        if not _otp_grouped_plausible(tok, tok):
            continue
        if _OTP_BAD_BEFORE_RE.search(clean[max(0, gm.start() - 30):gm.start()]):
            continue
        if _OTP_BAD_AFTER_RE.match(clean[gm.end():gm.end() + 20]):
            continue
        return _otp_format(tok)

    # (d) last resort: first plausible 4-8 digit number.
    for t in _OTP_DIGIT_TOKEN_RE.finditer(clean):
        if _otp_digits_plausible(clean, t.start(), t.end()):
            return t.group(0)

    return None
# ==========================================
# OTP Code Extraction (END OTP-EXTRACT)
# ==========================================

def _looks_like_phone(value):
    text = str(value).strip()
    digits = re.sub(r"\D", "", text)
    return 5 <= len(digits) <= 18 and not re.search(r"[A-Za-z]", text)


def _pick_service_hint(value):
    """Return a usable service/sender text (never a phone number) or ''."""
    hint = _clean_service_hint(value)
    return "" if (not hint or _looks_like_phone(hint)) else hint


def _find_hint_col(header_cells, skip_idx, p_config=None):
    if p_config and p_config.get("svc_col_idx"):
        try:
            return int(p_config.get("svc_col_idx")) - 1
        except (TypeError, ValueError):
            logger.debug("Ignored exception in _find_hint_col (line 4799)", exc_info=True)
    for i, cell in enumerate(header_cells):
        if i in skip_idx:
            continue
        if SERVICE_HINT_HEADER_RE.search(cell.get_text(" ", strip=True)):
            return i
    return None


# ==========================================
# Panel parse / fetch helpers (BEGIN PANEL-FETCH)
# ==========================================
# Everything the monitor thread and "Test Connection" need to talk to an API
# panel lives here, so both paths behave identically:
#   * parse_panel_response_ex  - parser that also reports WHY rows were skipped
#   * fetch_api_panel          - URL/token fallbacks + pagination + diagnostics
#   * apply_records_limit      - "records" limit that never drops the newest OTP
#   * build_panel_test_summary - text shown by Test Connection
from urllib.parse import parse_qsl, urlencode, urlunsplit

# Known field names (checked in this order before any guessing happens).
_PANEL_NUM_KEYS = ("number", "num", "phone", "msisdn", "to", "dest",
                   "destination", "mobile", "recipient", "phone_number")
_PANEL_MSG_KEYS = ("message", "msg", "sms", "text", "content", "body",
                   "sms_text", "smstext", "message_text")
_PANEL_TIME_KEYS = ("time", "date", "datetime", "timestamp", "created_at",
                    "created", "received_at", "sent_at", "dt", "date_time")
_PANEL_ID_KEYS = ("id", "uid", "sl", "serial", "seq", "sno", "slno", "trx",
                  "txn", "ref", "no")

_PF_SECRET_QS_RE = re.compile(
    r"(?i)((?:token|key|api_?key|apikey|secret|password|pass|auth)=)([^&\s\"'<>]+)")
_PF_TOKEN_ERR_RE = re.compile(
    r"token|api[\s_-]?key|\bkey\b|unauthori[sz]ed|forbidden|denied|authenticat|"
    r"invalid (?:auth|credential|access)|expired", re.I)
_PF_DATE_RE = re.compile(r"\d{4}[-/]\d{2}[-/]\d{2}")
_PF_CLOCK_RE = re.compile(r"\d{2}:\d{2}:\d{2}")
_PF_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
)
_PF_LIMIT_PARAMS = {"limit", "records", "per_page", "perpage", "page_size",
                    "pagesize", "length", "size", "count", "idisplaylength",
                    "max", "rows"}
_PF_PAGE_PARAMS = ("page", "pageno", "page_no", "p")
_PF_OFFSET_PARAMS = ("start", "offset", "idisplaystart", "skip")
_PF_DATE_PARAMS = {"date", "from", "to", "date_from", "date_to", "dt1", "dt2",
                   "start_date", "end_date", "fromdate", "todate", "datefrom",
                   "dateto", "startdate", "enddate", "since", "until"}
_PF_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
_pf_log_last = {}


# ---------- secrets ----------


def mask_text(text, token=""):
    """Hide the panel token everywhere (URLs, exception texts, raw bodies)."""
    s = str(text)
    token = (token or "").strip()
    if len(token) >= 4:
        s = s.replace(token, mask_secret(token))
    return _PF_SECRET_QS_RE.sub(lambda m: m.group(1) + mask_secret(m.group(2)), s)


def _set_panel_field(p, key, value):
    with data_lock:
        p[key] = value


# ---------- small parsing helpers ----------
def _pf_scalar(v):
    return v is not None and not isinstance(v, (dict, list))


def _is_id_key(k):
    k = str(k).lower()
    return k in _PANEL_ID_KEYS or k.startswith("id") or k.endswith("_id")


def _parse_ts(value):
    """Best-effort timestamp -> float epoch (None when it cannot be read)."""
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    try:
        if isinstance(value, (int, float)):
            v = float(value)
        else:
            s = str(value).strip()
            if not s:
                return None
            if re.fullmatch(r"\d{10}(?:\.\d+)?|\d{13}", s):
                v = float(s)
            else:
                s = s.replace("Z", "")
                s = re.sub(r"\.\d+(?=$|[+-]\d{2}:?\d{2}$)", "", s)
                if "T" in s or re.search(r"\d{2}:\d{2}(?::\d{2})?\s?[+-]\d{2}:?\d{2}$", s):
                    s = re.sub(r"\s?[+-]\d{2}:?\d{2}$", "", s)
                for fmt in _PF_TS_FORMATS:
                    try:
                        return datetime.strptime(s, fmt).timestamp()
                    except ValueError:
                        logger.debug("Ignored exception in _parse_ts (line 4903)", exc_info=True)
                        continue
                return None
        if v > 1e12:
            v /= 1000.0
        return v if v > 1e8 else None
    except (ValueError, OverflowError, OSError):
        return None


def _pf_new_stats():
    return {"format": "", "rows": 0, "no_number": 0, "no_otp": 0, "ambiguous": 0,
            "guessed_number": 0, "duplicates": 0, "error_kind": None,
            "error_detail": "", "meta": {}}


def _pf_phone_candidates(pairs):
    """(key, value) pairs -> [(digits, raw)] for values that look like phone numbers."""
    out = []
    for k, v in pairs:
        if not _pf_scalar(v):
            continue
        if k is not None and (_is_id_key(k) or k in _PANEL_TIME_KEYS):
            continue
        s = str(v).strip()
        digits = re.sub(r"\D", "", s)
        if not (7 <= len(digits) <= 18) or re.search(r"[A-Za-z]", s):
            continue
        if _PF_DATE_RE.search(s) or _PF_CLOCK_RE.search(s) or "." in s:
            continue
        if digits not in [d for d, _ in out]:
            out.append((digits, s))
    return out


def _pf_known_number(lower):
    for k in _PANEL_NUM_KEYS:
        if k in lower and _pf_scalar(lower[k]):
            cv = re.sub(r"\D", "", str(lower[k]))
            if 5 <= len(cv) <= 18:
                return cv
    if "sender" in lower and _pf_scalar(lower["sender"]) and _looks_like_phone(lower["sender"]):
        return re.sub(r"\D", "", str(lower["sender"]))
    return None


def _pf_process_record(item, p_config, stats):
    """One JSON record (dict or positional row) -> result dict or None."""
    if isinstance(item, dict):
        lower = {str(k).lower(): v for k, v in item.items()}
        pairs = [(str(k).lower(), v) for k, v in item.items()]
    else:
        lower = {}
        pairs = [(None, v) for v in item]

    hint = ""
    for k in SERVICE_HINT_FIELDS:
        if k in lower and not hint:
            hint = _pick_service_hint(lower[k])
    ts = None
    for k in _PANEL_TIME_KEYS:
        if k in lower and _pf_scalar(lower[k]):
            ts = _parse_ts(lower[k])
            break

    # 1) well-known field names first
    num = _pf_known_number(lower)
    msg = None
    for k in _PANEL_MSG_KEYS:
        if k in lower and _pf_scalar(lower[k]) and len(str(lower[k])) > 4:
            msg = str(lower[k])
            break

    # 2) guessing fallback - only for what the known fields did not give us
    if msg is None:
        for k, v in pairs:
            if not _pf_scalar(v) or (k is not None and k in _PANEL_TIME_KEYS):
                continue
            vs = str(v).strip()
            if len(vs) > 4 and not vs.isdigit() and extract_otp_code(vs):
                if msg is None or len(vs) > len(msg):
                    msg = vs
    if num is None:
        cands = _pf_phone_candidates(pairs)
        if len(cands) == 1:
            num = cands[0][0]
        elif len(cands) >= 2:
            if isinstance(item, list):
                # positional row: honour an explicitly configured column index,
                # otherwise keep the legacy "second numeric value" choice (counted).
                idx = None
                try:
                    idx = int(p_config.get("num_col_idx")) if p_config and p_config.get("num_col_idx") else None
                except (TypeError, ValueError):
                    idx = None
                if idx and 1 <= idx <= len(item) and _pf_scalar(item[idx - 1]):
                    cv = re.sub(r"\D", "", str(item[idx - 1]))
                    if 5 <= len(cv) <= 18:
                        num = cv
                if num is None:
                    num = cands[1][0]
                    stats["guessed_number"] += 1
            else:
                # dict without a known number field and several numeric values:
                # refuse to guess (wrong number = OTP sent to the wrong user).
                stats["ambiguous"] += 1

    if num is None:
        stats["no_number"] += 1
        return None
    if msg is None:
        stats["no_otp"] += 1
        return None
    otp = extract_otp_code(msg)
    if not otp:
        stats["no_otp"] += 1
        return None
    return {"number": num, "message": msg, "otp": otp, "service_hint": hint, "ts": ts}


def _pf_dict_is_record(node):
    lower = {str(k).lower(): v for k, v in node.items()}
    if _pf_known_number(lower):
        return True
    if any(isinstance(v, (dict, list)) for v in node.values()):
        return False                       # a container, never a record
    # flat dict without a known number: a record only if it carries OTP-ish data
    for k in _PANEL_MSG_KEYS:
        if k in lower and _pf_scalar(lower[k]):
            s = str(lower[k])
            if len(s) > 4 and re.search(r"\d", s):
                return True
    return bool(_pf_phone_candidates([(str(k).lower(), v) for k, v in node.items()]))


def _pf_traverse(node, p_config, stats, out, depth=0):
    if depth > 12:
        return
    if isinstance(node, list):
        if (len(node) >= 2 and all(not isinstance(c, (dict, list)) for c in node)
                and any(isinstance(c, str) and len(c) > 4 and re.search(r"\d", c) for c in node)):
            stats["rows"] += 1
            r = _pf_process_record(node, p_config, stats)
            if r:
                out.append(r)
            return
        for child in node:
            if isinstance(child, (dict, list)):
                _pf_traverse(child, p_config, stats, out, depth + 1)
    elif isinstance(node, dict):
        if _pf_dict_is_record(node):
            # a record is processed once and NOT descended into (no double counting)
            stats["rows"] += 1
            r = _pf_process_record(node, p_config, stats)
            if r:
                out.append(r)
            return
        for v in node.values():
            if isinstance(v, (dict, list)):
                _pf_traverse(v, p_config, stats, out, depth + 1)


def _pf_int(v):
    try:
        if isinstance(v, bool) or v is None:
            return None
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def _pf_page_meta(data):
    meta = {"total": None, "current": None, "last": None, "next_url": None, "has_more": None}
    if not isinstance(data, dict):
        return meta
    scopes = [data] + [v for v in data.values() if isinstance(v, dict)]
    for sc in scopes:
        lower = {str(k).lower(): v for k, v in sc.items()}
        for k in ("total", "recordstotal", "itotalrecords", "itotaldisplayrecords",
                  "recordsfiltered", "total_records", "totalrecords", "total_count"):
            if meta["total"] is None and k in lower and _pf_int(lower[k]) is not None:
                meta["total"] = _pf_int(lower[k])
        for k in ("current_page", "currentpage", "page"):
            if meta["current"] is None and k in lower and _pf_int(lower[k]) is not None:
                meta["current"] = _pf_int(lower[k])
        for k in ("last_page", "total_pages", "totalpages", "pages", "page_count"):
            if meta["last"] is None and k in lower and _pf_int(lower[k]) is not None:
                meta["last"] = _pf_int(lower[k])
        for k in ("next_page_url", "next_url", "nextpage", "next"):
            if meta["next_url"] is None and isinstance(lower.get(k), str) and lower[k].strip():
                meta["next_url"] = lower[k].strip()
        for k in ("has_more", "hasmore", "more"):
            if meta["has_more"] is None and isinstance(lower.get(k), bool):
                meta["has_more"] = lower[k]
    return meta


def _pf_panel_error(data):
    """The panel's own error message from a JSON body (None when it is not an error)."""
    if not isinstance(data, dict):
        return None
    lower = {str(k).lower(): v for k, v in data.items()}
    st = lower.get("status")
    bad = False
    if isinstance(st, str) and st.strip().lower() in (
            "error", "fail", "failed", "failure", "false", "unauthorized", "forbidden", "denied"):
        bad = True
    if st is False or lower.get("success") is False or lower.get("ok") is False:
        bad = True
    if lower.get("error") not in (None, "", False, 0, [], {}):
        bad = True
    if _pf_int(lower.get("code")) in (401, 403) or _pf_int(lower.get("status_code")) in (401, 403):
        bad = True
    if not bad:
        return None
    for k in ("error", "message", "msg", "detail", "reason", "description"):
        if lower.get(k) not in (None, "", False):
            return str(lower[k])[:200]
    return "Panel returned an error status"


def _pf_detect_format(text):
    s = (text or "").lstrip("\ufeff \t\r\n")
    if not s:
        return "empty"
    if s[0] in "{[":
        return "json"
    head = s[:300].lower()
    if head.startswith("<") or "<html" in head or "<!doctype" in head or "<table" in head:
        return "html"
    return "text"


def _pf_parse_table(response_text, p_config, stats, out):
    """Auto Captcha / CPT HTML table parsing (unchanged logic, now with counters)."""
    n_col_name = p_config.get("num_col_name", "number").lower() if p_config else "number"
    m_col_name = p_config.get("msg_col_name", "message").lower() if p_config else "message"
    n_idx = int(p_config.get("num_col_idx", 1)) - 1 if p_config and p_config.get("num_col_idx") else 1
    m_idx = int(p_config.get("msg_col_idx", 2)) - 1 if p_config and p_config.get("msg_col_idx") else 2
    soup = BeautifulSoup(response_text, 'html.parser')
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if not rows:
            continue
        final_n_idx, final_m_idx = n_idx, m_idx
        header_cells = rows[0].find_all(['th', 'td'])
        for i, cell in enumerate(header_cells):
            c_text = cell.get_text(strip=True).lower()
            if n_col_name in c_text:
                final_n_idx = i
            if m_col_name in c_text:
                final_m_idx = i
        hint_idx = _find_hint_col(header_cells, {final_n_idx, final_m_idx}, p_config)
        for row in rows:
            cols = row.find_all(['td', 'th'])
            if all(c.name == 'th' for c in cols):
                continue
            if len(cols) > max(final_n_idx, final_m_idx):
                stats["rows"] += 1
                num_text = cols[final_n_idx].get_text(separator=" ", strip=True)
                msg_text = cols[final_m_idx].get_text(separator=" ", strip=True)
                clean_num = re.sub(r'\D', '', num_text)
                if not (clean_num and 5 <= len(clean_num) <= 18):
                    stats["no_number"] += 1
                    continue
                otp = extract_otp_code(msg_text)
                if not (otp and len(msg_text) > 4):
                    stats["no_otp"] += 1
                    continue
                hint = ""
                if hint_idx is not None and 0 <= hint_idx < len(cols):
                    hint = _pick_service_hint(cols[hint_idx].get_text(separator=" ", strip=True))
                out.append({"number": clean_num, "message": msg_text, "otp": otp,
                            "service_hint": hint, "ts": None})


def parse_panel_response_ex(response_text, p_config=None):
    """Parse a panel response. Returns (results, stats); never swallows errors silently."""
    results, stats = [], _pf_new_stats()
    p_type = p_config.get("type", "API Panel") if p_config else "API Panel"
    text = response_text if isinstance(response_text, str) else str(response_text or "")

    if p_type == "Auto Captcha Panel":
        stats["format"] = "html"
        try:
            raw = []
            _pf_parse_table(text, p_config, stats, raw)
            results = raw
        except Exception as e:
            logger.exception("Error parsing captcha panel table")
            stats["error_kind"], stats["error_detail"] = "parser_error", f"{type(e).__name__}: {e}"[:200]
        return results, stats

    fmt = _pf_detect_format(text)
    stats["format"] = fmt
    if fmt == "empty":
        stats["error_kind"], stats["error_detail"] = "empty_body", "Empty response body"
        return results, stats
    if fmt != "json":
        short = text.strip()[:200]
        if fmt == "text" and _PF_TOKEN_ERR_RE.search(short) and len(short) < 200:
            stats["error_kind"], stats["error_detail"] = "token", short
        else:
            stats["error_kind"] = "invalid_json"
            stats["error_detail"] = f"Response is {fmt.upper()}, not JSON"
        return results, stats
    try:
        data = json.loads(text.lstrip("\ufeff"))
    except ValueError as e:
        stats["error_kind"], stats["error_detail"] = "invalid_json", f"Invalid JSON: {str(e)[:120]}"
        return results, stats

    raw = []
    try:
        _pf_traverse(data, p_config, stats, raw)
    except Exception as e:
        logger.exception("Error parsing API panel JSON")
        stats["error_kind"], stats["error_detail"] = "parser_error", f"{type(e).__name__}: {e}"[:200]
    stats["meta"] = _pf_page_meta(data)

    seen = set()
    for r in raw:
        uid = f"{r['number']}_{r['otp']}"
        if uid in seen:
            stats["duplicates"] += 1
            continue
        seen.add(uid)
        results.append(r)

    if not results and not stats["error_kind"]:
        perr = _pf_panel_error(data)
        if perr:
            stats["error_kind"] = "token" if _PF_TOKEN_ERR_RE.search(perr) else "panel_error"
            stats["error_detail"] = perr
    return results, stats


def parse_panel_response(response_text, p_config=None):
    """Backward compatible wrapper: only the parsed list."""
    return parse_panel_response_ex(response_text, p_config)[0]


# ---------- records limit ----------
def apply_records_limit(items, limit):
    """Apply the panel "records" limit without ever dropping the newest OTPs.

    The panel's ordering is derived from the row timestamps. If the ordering
    cannot be determined the limit is NOT applied (already-seen OTPs are
    de-duplicated downstream anyway, so nothing is delivered twice)."""
    info = {"limit": limit or 0, "applied": False, "dropped": 0, "order": "n/a", "note": ""}
    try:
        limit = int(limit or 0)
    except (TypeError, ValueError):
        limit = 0
    if limit <= 0 or len(items) <= limit:
        return items, info
    stamps = [i.get("ts") for i in items]
    known = [s for s in stamps if s is not None]
    if len(known) == len(items):
        info["order"] = "by timestamp"
        newest = sorted(range(len(items)), key=lambda n: stamps[n], reverse=True)[:limit]
        keep_idx = set(newest)
        kept = [it for n, it in enumerate(items) if n in keep_idx]
    elif len(known) >= 2 and known[0] != known[-1]:
        if known[0] > known[-1]:
            info["order"] = "newest first"
            kept = items[:limit]
        else:
            info["order"] = "oldest first"
            kept = items[-limit:]
    else:
        info["order"] = "unknown"
        info["note"] = "Row order could not be determined; records limit NOT applied."
        return items, info
    info["applied"] = True
    info["dropped"] = len(items) - len(kept)
    return kept, info


# ---------- URL helpers ----------
def _pf_expand_placeholders(url, p):
    today = datetime.now()
    rep = {
        "{today}": today.strftime("%Y-%m-%d"),
        "{yesterday}": datetime.fromtimestamp(today.timestamp() - 86400).strftime("%Y-%m-%d"),
        "{date_from}": str(p.get("date_from") or today.strftime("%Y-%m-%d")),
        "{date_to}": str(p.get("date_to") or today.strftime("%Y-%m-%d")),
    }
    for k, v in rep.items():
        url = url.replace(k, v)
    return url


def build_api_url_candidates(p):
    """The ONE place that knows the URL / token format fallbacks.

    Returns (candidates, api_url, full_url, token); candidates are the
    un-expanded URLs in the order they should be tried."""
    full_url = (p.get("full_api_url") or "").strip()
    url = (p.get("api_url") or "").strip()
    token = (p.get("token") or "").strip()
    cands = []
    if full_url:
        cands.append(full_url)
    elif url:
        if "{token}" in url or "{key}" in url:
            cands.append(url.replace("{token}", token).replace("{key}", token))
        elif "token=" in url or "key=" in url:
            cands.append(url)
        else:
            sep = '&' if '?' in url else '?'
            cands.append(f"{url}{sep}token={token}")
            cands.append(f"{url}{sep}key={token}&start=0")
            cands.append(f"{url}{sep}key={token}")
    return cands, url, full_url, token


def _pf_url_param_names(url):
    try:
        return sorted({k.lower() for k, _ in parse_qsl(urlsplit(url).query, keep_blank_values=True)})
    except ValueError:
        return []


def _pf_next_page_url(url, meta, rows_this_page, fetched_rows):
    """(next_url, note). next_url None + note => more rows exist but we cannot page."""
    nu = meta.get("next_url")
    if isinstance(nu, str) and nu.strip():
        return urljoin(url, nu.strip()), ""
    more = meta.get("has_more") is True
    if meta.get("last") and meta.get("current") and meta["current"] < meta["last"]:
        more = True
    if meta.get("total") is not None and fetched_rows < meta["total"]:
        more = True
    if not more or rows_this_page <= 0:
        return None, ""
    parts = urlsplit(url)
    q = parse_qsl(parts.query, keep_blank_values=True)
    names = {k.lower(): n for n, (k, _) in enumerate(q)}
    for name in _PF_OFFSET_PARAMS:
        if name in names:
            n = names[name]
            cur = _pf_int(q[n][1]) or 0
            q[n] = (q[n][0], str(cur + rows_this_page))
            return urlunsplit(parts._replace(query=urlencode(q))), ""
    for name in _PF_PAGE_PARAMS:
        if name in names:
            n = names[name]
            cur = _pf_int(q[n][1]) or 1
            q[n] = (q[n][0], str(cur + 1))
            return urlunsplit(parts._replace(query=urlencode(q))), ""
    if meta.get("current") or meta.get("last"):
        q.append(("page", str((meta.get("current") or 1) + 1)))
        return urlunsplit(parts._replace(query=urlencode(q))), ""
    return None, "More rows exist than were returned, but no page/offset parameter is known."


def _pf_log_attempt(name, attempt, verbose):
    line = "API fetch panel=%s url=%s http=%s kind=%s detail=%s parsed=%s"
    args = (name, attempt["url"], attempt["status"], attempt["kind"] or "ok",
            attempt["error"] or "-", attempt["parsed"])
    if verbose:
        logger.info(line, *args)
        return
    if attempt["kind"] is None:
        logger.debug(line, *args)
        return
    key = (name, attempt["url"], attempt["kind"])
    now = time.time()
    if now - _pf_log_last.get(key, 0) >= 60:      # one line/minute/failure, not every 5 s
        _pf_log_last[key] = now
        logger.warning(line, *args)


_PF_FAIL_RANK = {"token": 6, "panel_error": 5, "invalid_json": 4, "http": 3,
                 "parser_error": 3, "empty": 2, "empty_body": 2, "network": 1}


def fetch_api_panel(p, idx=None, verbose=False):
    """Fetch + parse an API panel. Used by BOTH the monitor thread and Test Connection.

    Returns a dict:
      ok, items, status, format, url_masked, url_params, working_template,
      rows, pages, total_reported, stats(parsed/skipped counters), attempts[],
      failure {kind, detail} (None when ok), pagination_note, raw_text, elapsed
    """
    t0 = time.monotonic()
    name = p.get("name", "Panel")
    cands, api_url, full_url, token = build_api_url_candidates(p)
    result = {"ok": False, "items": [], "status": None, "format": "", "url_masked": "",
              "url_params": [], "working_template": None, "rows": 0, "pages": 0,
              "total_reported": None, "attempts": [], "failure": None,
              "pagination_note": "", "raw_text": "", "elapsed": 0.0,
              "retry_after": None,
              "skipped": {"no_number": 0, "no_otp": 0, "ambiguous": 0,
                          "duplicates": 0, "guessed_number": 0}}
    if not cands:
        result["failure"] = {"kind": "no_url", "detail": "API URL / Full API URL is not set"}
        return result

    try:
        timeout = (5, max(3, min(60, float(p.get("timeout", 10)))))
        max_pages = max(1, min(10, int(p.get("max_pages", 5))))
        deadline = max(5.0, float(p.get("fetch_deadline", 45)))
    except (TypeError, ValueError):
        timeout, max_pages, deadline = (5, 10), 5, 45.0
    headers = {"User-Agent": _PF_UA}
    best_fail = None

    def _get(u):
        return requests.get(u, headers=headers, timeout=timeout)

    def _retry_after_seconds(response):
        try:
            return max(0.0, float(response.headers.get("Retry-After", 10)))
        except (TypeError, ValueError):
            return 10.0

    for raw_url in cands:
        if time.monotonic() - t0 > deadline:
            break
        try_url = _pf_expand_placeholders(raw_url, p)
        attempt = {"url": mask_text(try_url, token), "status": None, "error": None,
                   "kind": None, "parsed": 0}
        result["attempts"].append(attempt)
        try:
            res = _get(try_url)
        except Exception as e:                                   # network layer
            attempt["kind"] = "network"
            attempt["error"] = mask_text(f"{type(e).__name__}: {e}", token)[:200]
            _pf_log_attempt(name, attempt, verbose)
            if best_fail is None or _PF_FAIL_RANK["network"] > _PF_FAIL_RANK[best_fail["kind"]]:
                best_fail = {"kind": "network", "detail": attempt["error"]}
            continue

        status = res.status_code
        attempt["status"] = status
        if status == 429:
            retry_after = _retry_after_seconds(res)
            detail = "HTTP 429: rate limited"
            attempt["kind"], attempt["error"] = "http", detail
            _pf_log_attempt(name, attempt, verbose)
            result.update({
                "status": status,
                "url_masked": attempt["url"],
                "failure": {"kind": "http", "detail": detail},
                "retry_after": retry_after,
                "elapsed": round(time.monotonic() - t0, 2),
            })
            return result

        body = res.text or ""
        items, st = parse_panel_response_ex(body, p)
        attempt["parsed"] = len(items)
        if not result["raw_text"]:
            result["raw_text"] = mask_text(body[:4000], token)

        if not items:
            if status in (401, 403):
                kind, detail = "token", f"HTTP {status}: " + (st["error_detail"] or "access denied")
            elif status >= 400:
                kind = "token" if st["error_kind"] == "token" else "http"
                detail = f"HTTP {status}" + (f": {st['error_detail']}" if st["error_detail"] else "")
            elif st["error_kind"]:
                kind, detail = st["error_kind"], st["error_detail"]
            else:
                kind, detail = "empty", "Valid response but 0 OTP rows"
            attempt["kind"], attempt["error"] = kind, mask_text(detail, token)[:200]
            _pf_log_attempt(name, attempt, verbose)
            if best_fail is None or _PF_FAIL_RANK.get(kind, 0) > _PF_FAIL_RANK.get(best_fail["kind"], 0):
                best_fail = {"kind": kind, "detail": attempt["error"], "status": status,
                             "format": st["format"], "rows": st["rows"], "stats": st}
            continue

        # ---- success: first page parsed, now follow pagination (bounded) ----
        _pf_log_attempt(name, attempt, verbose)
        all_items = list(items)
        seen = {f"{i['number']}_{i['otp']}" for i in all_items}
        sk = result["skipped"]
        for k in ("no_number", "no_otp", "ambiguous", "duplicates", "guessed_number"):
            sk[k] += st[k]
        total_rows, rows_this, page_url, meta, pages = st["rows"], st["rows"], try_url, st["meta"], 1
        while pages < max_pages and time.monotonic() - t0 <= deadline:
            nxt, note = _pf_next_page_url(page_url, meta, rows_this, total_rows)
            if not nxt:
                result["pagination_note"] = note
                break
            if nxt == page_url:
                break
            try:
                pres = _get(nxt)
            except Exception as e:
                result["pagination_note"] = "Page %d failed: %s" % (
                    pages + 1, mask_text(f"{type(e).__name__}: {e}", token)[:120])
                break
            if pres.status_code == 429:
                result["retry_after"] = _retry_after_seconds(pres)
                result["pagination_note"] = (
                    f"Page {pages + 1} returned HTTP 429"
                )
                break
            if pres.status_code >= 400:
                result["pagination_note"] = f"Page {pages + 1} returned HTTP {pres.status_code}"
                break
            pitems, pst = parse_panel_response_ex(pres.text or "", p)
            new = 0
            for it in pitems:
                uid = f"{it['number']}_{it['otp']}"
                if uid in seen:
                    sk["duplicates"] += 1
                    continue
                seen.add(uid)
                all_items.append(it)
                new += 1
            for k in ("no_number", "no_otp", "ambiguous", "guessed_number"):
                sk[k] += pst[k]
            sk["duplicates"] += pst["duplicates"]
            pages += 1
            total_rows += pst["rows"]
            rows_this, meta, page_url = pst["rows"], pst["meta"], nxt
            _pf_log_attempt(name, {"url": mask_text(nxt, token), "status": pres.status_code,
                                   "kind": None, "error": None, "parsed": len(pitems)}, verbose)
            if new == 0:
                result["pagination_note"] = (f"Page {pages} added no new OTPs "
                                             "(panel may ignore the page parameter).")
                break
        else:
            if pages >= max_pages:
                result["pagination_note"] = f"Page limit ({max_pages}) reached; older rows not fetched."

        result.update({
            "ok": True, "items": all_items, "status": status, "format": st["format"],
            "url_masked": mask_text(try_url, token), "url_params": _pf_url_param_names(try_url),
            "rows": total_rows, "pages": pages,
            "total_reported": st["meta"].get("total"),
        })
        # remember a fallback format that worked (never for Full API URLs)
        if not full_url and api_url and token:
            template = raw_url.replace(token, "{token}")
            if template != api_url:
                result["working_template"] = template
        break

    if not result["ok"]:
        fb = best_fail or {"kind": "network", "detail": "No attempt could be made (deadline reached)"}
        result["failure"] = {"kind": fb["kind"], "detail": fb["detail"]}
        result["status"] = fb.get("status")
        result["format"] = fb.get("format", "")
        result["rows"] = fb.get("rows", 0)
        if fb.get("stats"):
            for k in ("no_number", "no_otp", "ambiguous", "duplicates", "guessed_number"):
                result["skipped"][k] = fb["stats"][k]
        if result["attempts"]:
            result["url_masked"] = result["attempts"][-1]["url"]
    result["elapsed"] = round(time.monotonic() - t0, 2)
    return result


# ---------- Test Connection text ----------
_PF_FAIL_TITLES = {
    "network": "❌ <b>Network error</b> (could not reach the panel)",
    "http": "❌ <b>HTTP error</b> from the panel",
    "invalid_json": "❌ <b>Invalid response</b> (not valid JSON)",
    "token": "❌ <b>Token / key rejected</b> by the panel",
    "panel_error": "❌ <b>Panel returned an error</b>",
    "parser_error": "❌ <b>Parser error</b> (see bot log)",
    "empty_body": "❌ <b>Empty response</b> from the panel",
    "empty": "⚠️ <b>Connected, but the panel really returned 0 OTPs</b>",
    "no_url": "❌ <b>API URL not set</b>",
}


def build_panel_test_summary(p, result, limit_info=None):
    """HTML block for Test Connection (samples are appended by the caller)."""
    esc = html.escape
    limit_info = limit_info or {}
    L = []
    if result.get("ok"):
        L.append("✅ <b>Connection Successful!</b>")
    else:
        fail = result.get("failure") or {"kind": "empty", "detail": ""}
        L.append(_PF_FAIL_TITLES.get(fail["kind"], "❌ <b>Connection failed</b>"))
        if fail.get("detail"):
            L.append(f"💬 <code>{esc(str(fail['detail'])[:250])}</code>")
    if result.get("status") is not None or result.get("format"):
        L.append(f"🌐 HTTP: <b>{result.get('status') if result.get('status') is not None else '-'}</b>"
                 f" | Response: <b>{esc((result.get('format') or '?').upper())}</b>")
    if result.get("url_masked"):
        L.append(f"🔗 URL (token masked): <code>{esc(result['url_masked'][:150])}</code>")
    if result.get("rows") is not None and result.get("format") != "":
        extra = f" (pages fetched: {result.get('pages', 1)})" if result.get("pages") else ""
        L.append(f"📦 Rows/items in response: <b>{result.get('rows', 0)}</b>{extra}")
    if result.get("total_reported") is not None:
        L.append(f"📊 Total reported by panel: <b>{result['total_reported']}</b>")
    parsed = len(result.get("items", [])) if "items" in result else result.get("parsed", 0)
    L.append(f"🎯 Total parsed OTPs: <b>{parsed}</b>")
    sk = result.get("skipped", {})
    dropped = limit_info.get("dropped", 0)
    total_skip = (sk.get("no_number", 0) + sk.get("no_otp", 0) + sk.get("duplicates", 0) + dropped)
    if total_skip:
        amb = f" (incl. {sk['ambiguous']} ambiguous)" if sk.get("ambiguous") else ""
        L.append(f"🚫 Excluded: <b>{total_skip}</b> → no number: {sk.get('no_number', 0)}{amb}, "
                 f"no OTP: {sk.get('no_otp', 0)}, duplicate: {sk.get('duplicates', 0)}, "
                 f"records limit: {dropped}")
    if limit_info.get("limit"):
        L.append(f"🔢 Records limit: {limit_info['limit']} "
                 f"({'applied, order: ' + limit_info.get('order', '?') if limit_info.get('applied') else 'not cutting'})")
        if limit_info.get("note"):
            L.append(f"⚠️ {esc(limit_info['note'])}")
    if sk.get("guessed_number"):
        L.append(f"⚠️ {sk['guessed_number']} row(s) used a guessed number column - verify field mapping.")
    if result.get("ok"):
        names = set(result.get("url_params") or [])
        if not (names & _PF_LIMIT_PARAMS) and not (names & _PF_DATE_PARAMS):
            L.append("ℹ️ URL has no limit/page/date parameter: the panel's default window applies "
                     "(verify against the website if counts differ).")
    if result.get("pagination_note"):
        L.append(f"ℹ️ {esc(result['pagination_note'])}")
    atts = result.get("attempts") or []
    if atts and (not result.get("ok") or len(atts) > 1):
        L.append("🔁 <b>Attempts:</b>")
        for a in atts[:4]:
            res = f"HTTP {a['status']}" if a.get("status") is not None else "no response"
            tail = f" - {a['kind']}: {a['error']}" if a.get("kind") else f" - {a.get('parsed', 0)} OTP"
            L.append(f"• <code>{esc(a['url'][:80])}</code> → {esc(res + tail)[:140]}")
    if result.get("elapsed"):
        L.append(f"⏱ {result['elapsed']}s")
    return "\n".join(L)
# ==========================================
# Panel parse / fetch helpers (END PANEL-FETCH)
# ==========================================

def fetch_cpt_panel_cdrs(p, session, check_url):
    try:
        res = session.get(check_url, timeout=15)
        html_text = res.text
    except Exception as e:
        logger.error(f"Error fetching panel: {e}")
        return [], ""
    
    if "login" in html_text.lower() or "signin" in html_text.lower() or any(x in html_text for x in ["Sign in to your account", "Please sign in", "Welcome back!"]):
        raise Exception("Session expired")
        
    soup = BeautifulSoup(html_text, 'html.parser')
    s_ajax_source = ""
    for script in soup.find_all("script"):
        script_text = script.string or ""
        match = re.search(r'sAjaxSource":\s*"([^"]+)"', script_text)
        if match:
            s_ajax_source = match.group(1)
            break
            
    results = []
    
    n_col_name = p.get("num_col_name", "number").lower()
    m_col_name = p.get("msg_col_name", "message").lower()
    n_idx = int(p.get("num_col_idx", 1)) - 1 if p.get("num_col_idx") else 1
    m_idx = int(p.get("msg_col_idx", 2)) - 1 if p.get("msg_col_idx") else 2
    try:
        svc_idx = int(p.get("svc_col_idx")) - 1 if p.get("svc_col_idx") else None
    except (TypeError, ValueError):
        svc_idx = None

    if s_ajax_source:
        baseUrl = p.get("login_url", "").split("/client")[0].split("/login")[0].strip()
        if not baseUrl.startswith("http"):
            baseUrl = "http://" + baseUrl
            
        full_ajax_url = ""
        if s_ajax_source.startswith("http"):
            full_ajax_url = s_ajax_source
        elif s_ajax_source.startswith("/"):
            full_ajax_url = f"{baseUrl}{s_ajax_source}"
        else:
            last_slash_idx = check_url.rfind("/")
            current_dir = check_url[:last_slash_idx]
            full_ajax_url = f"{current_dir}/{s_ajax_source}"

        if "iDisplayLength" not in full_ajax_url:
            query_params = "sEcho=1&iColumns=7&iDisplayStart=0&iDisplayLength=10000&sSearch=&iSortingCols=1&iSortCol_0=0&sSortDir_0=desc"
            divider = "&" if "?" in full_ajax_url else "?"
            full_ajax_url += f"{divider}{query_params}"

        ajax_headers = {
            "Referer": check_url,
            "X-Requested-With": "XMLHttpRequest"
        }
        
        try:
            ajax_res = session.get(full_ajax_url, headers=ajax_headers, timeout=15)
            data_dict = ajax_res.json()
            rows = data_dict.get("aaData", [])
            for row_val in rows:
                if not isinstance(row_val, list):
                    continue
                    
                if len(row_val) < max(n_idx, m_idx) + 1:
                    continue
                    
                num_val = row_val[n_idx] if (0 <= n_idx < len(row_val)) else row_val[2]
                msg_val = row_val[m_idx] if (0 <= m_idx < len(row_val)) else row_val[4]
                
                clean_num = re.sub(r'\D', '', str(num_val))
                if clean_num and 5 <= len(clean_num) <= 18:
                    otp = extract_otp_code(msg_val)
                    if otp and len(msg_val) > 4:
                        hint = ""
                        if svc_idx is not None and 0 <= svc_idx < len(row_val):
                            hint = _pick_service_hint(row_val[svc_idx])
                        results.append({"number": clean_num, "message": msg_val, "otp": otp, "service_hint": hint})
        except Exception as e:
            logger.error(f"Error fetching AJAX data: {e}")
                    
    else:
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            if not rows:
                continue
            
            final_n_idx = n_idx
            final_m_idx = m_idx
            
            header_cells = rows[0].find_all(['th', 'td'])
            for i, cell in enumerate(header_cells):
                c_text = cell.get_text(strip=True).lower()
                if n_col_name in c_text:
                    final_n_idx = i
                if m_col_name in c_text:
                    final_m_idx = i

            hint_idx = _find_hint_col(header_cells, {final_n_idx, final_m_idx}, p)

            for row in rows:
                cols = row.find_all(['td', 'th'])
                if all(c.name == 'th' for c in cols):
                    continue
                
                if len(cols) > max(final_n_idx, final_m_idx):
                    num_text = cols[final_n_idx].get_text(separator=" ", strip=True)
                    msg_text = cols[final_m_idx].get_text(separator=" ", strip=True)
                    
                    clean_num = re.sub(r'\D', '', num_text)
                    if clean_num and 5 <= len(clean_num) <= 18:
                        otp = extract_otp_code(msg_text)
                        if otp and len(msg_text) > 4:
                            hint = ""
                            if hint_idx is not None and 0 <= hint_idx < len(cols):
                                hint = _pick_service_hint(cols[hint_idx].get_text(separator=" ", strip=True))
                            results.append({"number": clean_num, "message": msg_text, "otp": otp, "service_hint": hint})
                            
    return results, html_text

def attempt_auto_login(p, idx):
    login_url = p.get("login_url", "").strip()
    if not login_url.startswith("http"):
        login_url = "http://" + login_url
        
    if not login_url.lower().endswith('/login') and not login_url.lower().endswith('.php'):
        login_url = f"{login_url.rstrip('/')}/login"
        
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    })
    
    try:
        res = session.get(login_url, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        all_text = res.text
        
        captcha_match = re.search(r'(\d+\s*[\+\-\*]\s*\d+)\s*[=\?:]', all_text)
        if not captcha_match:
            captcha_match = re.search(r'what is\s*(\d+\s*[\+\-\*]\s*\d+)', all_text, re.I)
        if not captcha_match:
            elements = soup.find_all(["label", "div", "span", "p", "strong"])
            for el in elements:
                txt = el.get_text(separator=" ", strip=True)
                if any(op in txt for op in ["+", "-", "*"]):
                    m = re.search(r'(\d+\s*[\+\-\*]\s*\d+)', txt)
                    if m:
                        captcha_match = m
                        break
                        
        captcha_text = captcha_match.group(1) if captcha_match else "0 + 0"
        answer = "0"
        m2 = re.search(r'(\d+)\s*([\+\-\*])\s*(\d+)', captcha_text)
        if m2:
            a, op, b = int(m2.group(1)), m2.group(2), int(m2.group(3))
            if op == '+':
                answer = str(a + b)
            elif op == '-':
                answer = str(a - b)
            elif op == '*':
                answer = str(a * b)

        form = soup.find("form")
        if not form:
            _set_panel_field(p, "login_status", "❌ No login form found")
            return False
            
        action = form.get("action")
        post_url = urljoin(login_url, action) if action else login_url

        form_data = {}
        for hidden in form.find_all("input", type="hidden"):
            name = hidden.get("name")
            if name:
                form_data[name] = hidden.get("value") or ""
        
        user_input = form.find("input", {"name": re.compile(r"user|email|id", re.I)}) or \
                     form.find("input", {"type": "text", "placeholder": re.compile(r"user|email", re.I)}) or \
                     form.find("input", {"type": "text"})
                     
        pass_input = form.find("input", {"name": re.compile(r"pass", re.I)}) or \
                     form.find("input", {"type": "password"})
                     
        captcha_input = form.find("input", {"placeholder": re.compile(r"answer|ans|code|verification|value|captcha", re.I)}) or \
                        form.find("input", {"name": re.compile(r"ans|captcha|ver|code", re.I)})
        
        user_field = user_input.get("name") if user_input else "username"
        pass_field = pass_input.get("name") if pass_input else "password"
        captcha_field = captcha_input.get("name") if captcha_input else "answer"

        form_data[user_field] = p.get("username", "")
        form_data[pass_field] = p.get("password", "")
        if captcha_field:
            form_data[captcha_field] = answer

        login_req = session.post(post_url, data=form_data, allow_redirects=True, timeout=15)
        
        msg_link = p.get("msg_link", "").strip()
        if not msg_link.startswith("http") and msg_link != "":
            msg_link = "http://" + msg_link
            
        check_url = msg_link if msg_link else f"{login_url.split('/login')[0]}/client/SMSCDRStats"
        
        check_res = session.get(check_url, timeout=10)
        
        if 'logout' in login_req.text.lower() or 'logout' in check_res.text.lower() or 'sms reports' in check_res.text.lower() or 'dashboard' in check_res.text.lower() or 'cdrs' in check_res.text.lower():
            with data_lock:
                panel_sessions[idx] = session
            _set_panel_field(p, "login_status", "✅ Active & Fetching")
            return True
        else:
            _set_panel_field(p, "login_status", f"❌ Login Failed (Math: {captcha_text} = {answer})")
            return False
            
    except Exception as e:
        logger.error(f"Auto login error: {e}")
        _set_panel_field(p, "login_status", f"❌ Error: {str(e)[:20]}")
        
    return False

# ==========================================
# OTP Forwarding Reliability (BEGIN OTP-DELIVERY)
# ==========================================
# An OTP is marked "processed" only AFTER every delivery target (forward
# groups + user inboxes) has been attempted.  Each target is independent:
# a failure for one group never blocks another group or an inbox.
OTP_SEND_RETRIES = 3                 # retries per target after the first try
OTP_RETRY_BACKOFF_SECONDS = 1.0      # 1s, 2s, 4s ...
OTP_RETRY_AFTER_CAP_SECONDS = 60.0   # longest Telegram retry_after we wait inline
OTP_MAX_PENDING_CYCLES = 5           # monitor cycles before giving up a target
OTP_BASELINE_SETTING = "otp_baseline_state"

otp_pending = {}             # otp key -> {"targets": [...], "cycles": int}
otp_baselined_panels = set()
otp_baseline_state = "done"  # "in_progress" only on a true first start

# Chat-level problems: retrying cannot help, the owner must fix the group.
_OWNER_ALERT_MARKERS = (
    "chat not found",
    "bot is not a member",
    "not enough rights",
    "bot was kicked",
    "have no rights to send",
    "chat_write_forbidden",
    "chat_admin_required",
    "peer_id_invalid",
    "group chat was deleted",
    "group chat was upgraded",
)
# User-level permanent problems (inbox): log only, no owner alert.
_PERMANENT_MARKERS = _OWNER_ALERT_MARKERS + (
    "bot was blocked by the user",
    "user is deactivated",
    "bot can't initiate conversation",
)


def make_otp_key(num, otp, msg_text):
    """Dedup by normalized phone digits and OTP only; message text is irrelevant."""
    return _canonical_otp_key(num, otp)


def remember_processed_otps(keys):
    """Add keys to the bounded FIFO set and persist them.

    The oldest keys are evicted one by one once the limit is exceeded; the
    set is never cleared wholesale (that re-forwarded old OTPs).
    """
    keys = [k for k in keys if k is not None]
    evicted = False
    with data_lock:
        for key in keys:
            processed_otps[key] = None
        while len(processed_otps) > PROCESSED_OTPS_LIMIT:
            processed_otps.popitem(last=False)
            evicted = True
    try:
        save_processed_otps(keys, prune=evicted)
    except Exception as exc:
        logger.error("Could not persist %s processed OTP key(s): %s", len(keys), exc)
    return evicted


def _tg_result_info(result):
    """Normalise an api_call() result -> (ok, error_code, description, retry_after)."""
    if not isinstance(result, dict) or not result:
        return False, None, "no response from Telegram (network error or timeout)", None
    if result.get("ok"):
        return True, None, "", None
    description = str(result.get("description") or "unknown Telegram API error")
    retry_after = (result.get("parameters") or {}).get("retry_after")
    return False, result.get("error_code"), description, retry_after


def _strip_tg_emoji(text):
    """Replace <tg-emoji ...>X</tg-emoji> with the plain fallback emoji X."""
    return re.sub(r"<tg-emoji\b[^>]*>(.*?)</tg-emoji>", r"\1", str(text), flags=re.S)


def _strip_button_icons(markup, drop_style=False):
    """Copy of an inline keyboard without premium button icons (and styles)."""
    if not isinstance(markup, dict):
        return markup
    clean = copy.deepcopy(markup)
    for row in clean.get("inline_keyboard", []) or []:
        for button in row:
            if isinstance(button, dict):
                button.pop("icon_custom_emoji_id", None)
                if drop_style:
                    button.pop("style", None)
    return clean


def send_otp_message(chat_id, text, reply_markup=None):
    """Send one OTP message with fallback and retries.

    * 400 (premium emoji / entity / button problem): resend without
      icon_custom_emoji_id, then as plain text without tg-emoji.
    * 429 / 5xx / no response: up to OTP_SEND_RETRIES retries with backoff,
      honouring Telegram's retry_after.
    * Chat-level problems (chat not found, bot not a member, no rights) are
      permanent: no retries, and the caller is told to alert the owner.

    Returns a dict: ok, stage, error_code, description, transient, alert.
    """
    stages = (
        ("full", text, reply_markup),
        ("no_button_icons", text, _strip_button_icons(reply_markup)),
        ("plain_text", _strip_tg_emoji(text), _strip_button_icons(reply_markup, drop_style=True)),
    )
    stage_idx = 0
    retries = 0
    while True:
        stage, stage_text, stage_markup = stages[stage_idx]
        result = send_message(chat_id, stage_text, reply_markup=stage_markup)
        ok, code, description, retry_after = _tg_result_info(result)
        if ok:
            if stage_idx:
                logger.warning(
                    "OTP delivered to chat_id=%s using fallback stage=%s", chat_id, stage
                )
            return {"ok": True, "stage": stage, "error_code": None,
                    "description": "", "transient": False, "alert": False}

        logger.warning(
            "OTP send failed chat_id=%s stage=%s code=%s retry=%s/%s error=%s",
            chat_id, stage, code, retries, OTP_SEND_RETRIES, description,
        )
        lowered = description.lower()
        alert = any(marker in lowered for marker in _OWNER_ALERT_MARKERS)
        permanent = alert or any(marker in lowered for marker in _PERMANENT_MARKERS)
        fail = {"ok": False, "stage": stage, "error_code": code,
                "description": description, "alert": alert}
        if permanent:
            return dict(fail, transient=False)

        if code == 400:
            if stage_idx < len(stages) - 1:
                stage_idx += 1          # emoji/entity/button fallback, not a retry
                continue
            return dict(fail, transient=False)
        if code in (401, 403, 404):
            return dict(fail, transient=False)

        # 429, 5xx or no response: transient.
        retries += 1
        if retries > OTP_SEND_RETRIES:
            return dict(fail, transient=True)
        if code == 429:
            try:
                delay = float(retry_after) if retry_after is not None else 1.0
            except (TypeError, ValueError):
                delay = 1.0
            if delay > OTP_RETRY_AFTER_CAP_SECONDS:
                return dict(fail, transient=True)   # retried on the next cycle
            delay += 0.2
        else:
            delay = OTP_RETRY_BACKOFF_SECONDS * (2 ** (retries - 1))
        time.sleep(delay)


def _alert_owner_chat_problem(chat_id, description):
    try:
        notify_owner_error(
            f"OTP forwarding to chat {chat_id} failed: {description}. "
            "Check that the bot is a member/admin with permission to post.",
            key=f"otp-forward-{chat_id}",
        )
    except Exception:
        logger.exception("Could not alert the owner about chat %s", chat_id)


def deliver_otp_targets(state):
    """Attempt every remaining target once; return True when none remain.

    Targets that failed transiently stay in ``state['targets']`` for the next
    monitor cycle; permanent failures are logged (and the owner alerted for
    group problems) and dropped.  One target never affects another.
    """
    remaining = []
    for target in state["targets"]:
        try:
            outcome = send_otp_message(target["chat_id"], target["text"], target["markup"])
        except Exception as exc:
            logger.exception("Unexpected OTP delivery error chat_id=%s", target["chat_id"])
            outcome = {"ok": False, "stage": "exception", "error_code": None,
                       "description": f"{type(exc).__name__}: {exc}",
                       "transient": True, "alert": False}
        if outcome["ok"]:
            continue
        logger.error(
            "OTP delivery failed kind=%s chat_id=%s code=%s transient=%s error=%s",
            target["kind"], target["chat_id"], outcome["error_code"],
            outcome["transient"], outcome["description"],
        )
        if target["kind"] == "group" and outcome["alert"]:
            _alert_owner_chat_problem(target["chat_id"], outcome["description"])
        if outcome["transient"]:
            remaining.append(target)
    state["targets"] = remaining
    state["cycles"] += 1
    return not remaining


def _attempt_pending_otp(key):
    with data_lock:
        state = otp_pending.get(key)
        if state is None or state.get("in_flight"):
            return
        state["in_flight"] = True
    try:
        finished = deliver_otp_targets(state)
    except Exception:
        logger.exception("OTP delivery crashed for key=%s", key)
        finished = False
    if not finished and state["cycles"] >= OTP_MAX_PENDING_CYCLES:
        logger.error(
            "Giving up OTP delivery after %s cycles; undelivered chat_ids=%s",
            state["cycles"], [t["chat_id"] for t in state["targets"]],
        )
        finished = True
    if finished:
        try:
            remember_processed_otps([key])
        except Exception:
            logger.exception("Could not persist a completed OTP dedup key.")
        finally:
            with data_lock:
                if otp_pending.get(key) is state:
                    otp_pending.pop(key, None)
    else:
        with data_lock:
            if otp_pending.get(key) is state:
                state["in_flight"] = False


def retry_pending_otp_deliveries():
    for key in list(otp_pending):
        _attempt_pending_otp(key)


def process_panel_otp_item(p, item):
    """Plan and deliver one OTP.  Returns False when it was already seen."""
    global recent_traffic
    num = item["number"]
    otp = item["otp"]
    msg_text = item["message"]
    key = make_otp_key(num, otp, msg_text)
    clean_api_num = _phone_number_digits(num)
    if key is None or not clean_api_num:
        logger.warning("Ignoring OTP item with an invalid number or empty code.")
        return False

    with data_lock:
        if key in processed_otps or key in otp_pending:
            return False
        char, iso = get_flag_and_code(num)
        app_full_name, prem_app_html = get_service_info_html(
            p.get("name", "Panel"), msg_text,
            service_hint=item.get("service_hint"), panel_mode=True,
        )
        fw_groups = list(bot_settings.get("fw_groups", []))
        # Dynamic pricing: check service_prices["SERVICE::COUNTRY"] first.
        # app_full_name and iso are resolved just above via get_service_info_html / get_flag_and_code.
        _svc_price_key = f"{app_full_name.upper()}::{iso.upper()}"
        _price_map = bot_settings.get("service_prices") or {}
        reward = float(
            _price_map.get(_svc_price_key,
                           _price_map.get(app_full_name.upper(),
                                          bot_settings.get("otp_reward", 0.0)))
        )

    display_num = f"+{num}" if not str(num).startswith("+") else str(num)
    masked = mask_number(display_num)
    lang = detect_language(msg_text)
    lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
    display_msg = render_body_text(
        f"{get_flag_info_html(display_num)} {iso} | {prem_app_html} {masked} | 💬 {lang_name}"
    )

    targets = []
    for fw in fw_groups:
        kb = build_group_otp_keyboard(fw, otp)["inline_keyboard"]
        targets.append({"kind": "group", "chat_id": fw["chat_id"], "text": display_msg,
                        "markup": {"inline_keyboard": kb}})

    owners = []
    session_now = time.time()
    with sessions_lock:
        for uid, session_data in user_active_sessions.items():
            session_expiry = session_data.get("expires_at")
            if session_expiry is None:
                session_expiry = (
                    float(session_data.get("created_at", session_now))
                    + ACTIVE_USER_SESSION_TTL_SECONDS
                )
            if session_expiry <= session_now:
                continue
            for act_num in session_data.get("nums", []):
                act_clean = _phone_number_digits(act_num)
                if act_clean and act_clean == clean_api_num:
                    owners.append(uid)
                    break
    owners = list(set(owners))
    for owner_id in owners:
        inbox_msg = render_body_text(
            f"{get_flag_info_html(display_num)} {iso} | {prem_app_html} {display_num} | 💬 {lang_name}"
        )
        inbox_kb = build_inbox_otp_keyboard(otp)["inline_keyboard"]
        if reward > 0:
            inbox_kb.append([{"text": f"Added {reward} tk", "icon_custom_emoji_id": "5420396762189831222",
                              "callback_data": "ignore", "style": "primary"}])
        targets.append({"kind": "inbox", "chat_id": owner_id, "text": inbox_msg,
                        "markup": {"inline_keyboard": inbox_kb}})

    # Register first: from here on this OTP can never be planned twice, so the
    # side effects below (traffic row, rewards, counters) happen exactly once
    # even if delivery has to be retried on later cycles.
    otp_pending[key] = {"targets": targets, "cycles": 0}

    try:
        current_time = time.time()
        traffic_item = {"service": app_full_name, "iso": iso, "flag": char,
                        "number": num, "time": current_time}
        with data_lock:
            recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
            recent_traffic.append(traffic_item)
            save_traffic_record(traffic_item)
    except Exception:
        logger.exception("Could not record traffic for OTP number=%s", num)

    for owner_id in owners:
        try:
            if reward > 0:
                update_balance(owner_id, reward)
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET total_otps = total_otps + 1 WHERE user_id = ?', (str(owner_id),))
                conn.commit()
            finally:
                conn.close()
            _firebase_increment_user(owner_id, total_otps=1)
        except Exception as e:
            logger.error(f"Error updating user OTP count/reward for {owner_id}: {e}")

    # ── analytics_addon: OTP counter + per-panel log ─────────────────────
    try:
        analytics_increment("total_otps")
        _panel_name_log = (
            p.get("name", "Unknown") if isinstance(p, dict) else "Unknown"
        )
        analytics_increment_panel(
            _panel_name_log,
            number=str(num),
            service=str(app_full_name),
            iso=str(iso),
        )
    except Exception:
        logger.exception("analytics OTP increment failed")
    # ─────────────────────────────────────────────────────────────────────

    _attempt_pending_otp(key)
    return True


def baseline_panel_items(idx, parsed_data):
    """First start only: treat the panel's existing rows as already seen."""
    keys = [make_otp_key(i["number"], i["otp"], i["message"]) for i in parsed_data]
    remember_processed_otps(keys)
    otp_baselined_panels.add(idx)
    logger.info("OTP baseline: marked %s existing row(s) of panel %s as seen", len(keys), idx)


def init_otp_baseline_state():
    """A true first start (no processed OTPs stored) baselines existing data."""
    global otp_baseline_state
    state = _read_setting_raw(OTP_BASELINE_SETTING)
    if state not in ("in_progress", "done"):
        with data_lock:
            first_start = not processed_otps
        state = "in_progress" if first_start else "done"
        _write_setting_raw(OTP_BASELINE_SETTING, state)
    otp_baseline_state = state


def finish_otp_baseline_if_ready(panels):
    global otp_baseline_state
    if otp_baseline_state != "in_progress":
        return
    active = [idx for idx, p in panels if p.get("status") == "ON"]
    if active and all(idx in otp_baselined_panels for idx in active):
        otp_baseline_state = "done"
        try:
            _write_setting_raw(OTP_BASELINE_SETTING, "done")
        except Exception as exc:
            logger.warning("Could not store OTP baseline state: %s", exc)
# ==========================================
# OTP Forwarding Reliability (END OTP-DELIVERY)
# ==========================================

# ==========================================
# Panel monitor (per-panel workers: a slow panel never blocks the others)
# ==========================================
panel_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="panel-fetch")
panel_inflight = {}        # stable panel monitor ID -> Future
panel_inflight_lock = threading.RLock()
panel_fetch_state = {}     # stable panel monitor ID -> last fetch status (guarded by data_lock)
_deleted_panel_monitor_ids = set()
otp_item_lock = threading.Lock()   # OTP planning/dedup stays single-file across panels


def _panel_monitor_id(panel):
    with data_lock:
        key = panel.get("_monitor_id")
        if not key:
            key = uuid.uuid4().hex
            panel["_monitor_id"] = key
        _deleted_panel_monitor_ids.discard(str(key))
        return str(key)


def _panel_state_update(p, **kw):
    key = str(p.get("_monitor_id") or _panel_monitor_id(p))
    with data_lock:
        if key not in _deleted_panel_monitor_ids:
            panel_fetch_state.setdefault(key, {}).update(kw)


def _process_panel_items(idx, p, parsed_data):
    if otp_baseline_state == "in_progress" and idx not in otp_baselined_panels:
        baseline_panel_items(idx, parsed_data)
        return
    for item in parsed_data:
        try:
            with otp_item_lock:
                process_panel_otp_item(p, item)
        except Exception:
            logger.exception("OTP processing failed for panel %s", idx)


def _monitor_one_panel(idx, p):
    with data_lock:
        next_fetch = p.get("_next_fetch", 0)
    try:
        if time.time() < float(next_fetch or 0):
            return
    except (TypeError, ValueError):
        pass

    if p.get("type") == "Auto Captcha Panel":
        with data_lock:
            sess = panel_sessions.get(idx)
        if not sess:
            now = time.time()
            with data_lock:
                if now - p.get("last_login_attempt", 0) < 30:
                    return
                p["last_login_attempt"] = now
            success = attempt_auto_login(p, idx)
            with data_lock:
                save_db()
            if not success:
                return
            with data_lock:
                sess = panel_sessions.get(idx)
        try:
            parsed_data, _res_text = fetch_cpt_panel_cdrs(p, sess, p["msg_link"])
            with data_lock:
                p["login_status"] = "✅ Active & Fetching"
        except Exception as e:
            logger.error(f"Panel fetch error: {e}")
            with data_lock:
                p["login_status"] = "❌ Session Expired (Retrying...)"
                panel_sessions.pop(idx, None)
                save_db()
            return
    elif p.get("api_url") or p.get("full_api_url"):
        res = fetch_api_panel(p, idx=idx)
        with data_lock:
            if res.get("retry_after") is not None:
                try:
                    retry_after = max(0.0, float(res["retry_after"]))
                except (TypeError, ValueError):
                    retry_after = 10.0
                try:
                    previous_delay = max(0.0, float(p.get("_retry_delay", 0)))
                except (TypeError, ValueError):
                    previous_delay = 0.0
                delay = min(
                    120.0,
                    max(10.0, retry_after, previous_delay * 2),
                )
                p["_retry_delay"] = delay
                p["_next_fetch"] = time.time() + delay
            elif res.get("ok"):
                p["_retry_delay"] = 0
                p["_next_fetch"] = 0
        fail = res.get("failure") or {}
        _panel_state_update(
            p, last_run=time.time(), ok=res["ok"], status=res.get("status"),
            error_kind=fail.get("kind"), error=fail.get("detail"),
            rows=res.get("rows", 0), parsed=len(res.get("items", [])),
        )
        if res.get("working_template"):
            with data_lock:
                p["api_url"] = res["working_template"]
                save_db()
        parsed_data = res["items"]
        if not parsed_data:
            return
        parsed_data, _limit_info = apply_records_limit(parsed_data, p.get("records", 0))
    else:
        return
    _process_panel_items(idx, p, parsed_data)


def _monitor_one_panel_safe(idx, p):
    try:
        _monitor_one_panel(idx, p)
    except Exception:
        logger.exception("Panel monitor worker failed for panel %s", idx)
        _panel_state_update(p, ok=False, error_kind="worker_error", last_run=time.time())


def panel_monitor_thread():
    try:
        init_otp_baseline_state()
    except Exception:
        logger.exception("Could not initialise the OTP baseline state")
    while True:
        try:
            retry_pending_otp_deliveries()
            with data_lock:
                panels_copy = [
                    (idx, p, _panel_monitor_id(p))
                    for idx, p in enumerate(bot_settings.get("panels", []))
                ]

            active_panel_ids = {panel_id for _, _, panel_id in panels_copy}
            with panel_inflight_lock:
                stale_panel_ids = [
                    panel_id for panel_id in panel_inflight
                    if panel_id not in active_panel_ids
                ]
                for panel_id in stale_panel_ids:
                    future = panel_inflight.pop(panel_id, None)
                    if future:
                        future.cancel()
            if stale_panel_ids:
                with data_lock:
                    for panel_id in stale_panel_ids:
                        panel_fetch_state.pop(panel_id, None)

            for idx, p, panel_id in panels_copy:
                if p.get("status") != "ON":
                    continue
                try:
                    if time.time() < float(p.get("_next_fetch", 0) or 0):
                        continue
                except (TypeError, ValueError):
                    pass
                with panel_inflight_lock:
                    fut = panel_inflight.get(panel_id)
                    if fut is not None and not fut.done():
                        continue        # this panel is still busy; the others carry on
                    panel_inflight[panel_id] = panel_executor.submit(
                        _monitor_one_panel_safe, idx, p
                    )

            finish_otp_baseline_if_ready([(idx, p) for idx, p, _ in panels_copy])
        except Exception as e:
            logger.error(f"Panel monitor error: {e}")
        time.sleep(5)

# ==========================================
# UI Keyboards & Menu Builders
# ==========================================
def get_cancel_kb():
    return {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_state", "style": "danger"}]]}

def main_menu(user_id):
    # ── advanced_features: translated menu labels ─────────────────────────
    gt = lambda key: get_text(user_id, key)   # noqa: E731
    kb = [
        [
            {"text": gt("menu_get_number"), "icon_custom_emoji_id": "5337132498965010628", "style": "primary"},
            {"text": gt("menu_search"),     "icon_custom_emoji_id": "5463352748751753567", "style": "primary"},
        ],
        [
            {"text": gt("menu_traffic"),    "icon_custom_emoji_id": "5352877703043258544", "style": "success"},
            {"text": gt("menu_2fa"),        "icon_custom_emoji_id": "5267421176841398765", "style": "primary"},
        ],
        [
            {"text": gt("menu_refer"),      "icon_custom_emoji_id": "5420396762189831222", "style": "success"},
            {"text": gt("menu_withdrawal"), "icon_custom_emoji_id": "5352585194295564660", "style": "danger"},
        ],
        [
            {"text": gt("menu_support"),    "icon_custom_emoji_id": "5420145051336485498", "style": "primary"},
        ],
    ]
    if is_admin(user_id):
        kb.append([{"text": gt("menu_admin"), "icon_custom_emoji_id": "5420155432272438703", "style": "danger"}])
    return {
        "keyboard": kb,
        "resize_keyboard": True,
        "input_field_placeholder": "Choose an option",
    }


def send_start_welcome(chat_id):
    """Send the configured welcome and the regular reply-keyboard menu."""
    c_msg = bot_settings.get("custom_messages", {}).get("start", {})
    text = render_body_text(c_msg.get("text", f"{PEM['hi']} Welcome!"))
    if not text or text.strip() == "":
        text = render_body_text(
            f"{PEM['hi']} Welcome to the Number & OTP Service Bot!\n"
            "Use the menu below to get started."
        )
    buttons = []
    for button in c_msg.get("buttons", []):
        button_copy = button.copy()
        button_copy.setdefault("style", "primary")
        buttons.append([button_copy])
    if buttons:
        send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})
        send_message(
            chat_id,
            render_body_text(f"{PEM['gear']} Navigation Menu:"),
            reply_markup=main_menu(chat_id),
        )
    else:
        send_message(chat_id, text, reply_markup=main_menu(chat_id))


def build_get_number_screen():
    """Build the service selection screen used by GET NUMBER and its Back action."""
    global service_screen_cache
    index = _get_inventory_index()
    version = index["version"]
    with inventory_index_lock:
        if service_screen_cache and service_screen_cache[0] == version:
            return copy.deepcopy(service_screen_cache[1])

    all_services = list(index["services"].values())
    if not all_services:
        result = (None, None)
        return result

    with data_lock:
        c_msg = copy.deepcopy(
            bot_settings.get("custom_messages", {}).get("get_number", {})
        )
    txt = render_body_text(c_msg.get("text", f"{PEM['pin']} Select Service"))
    kb = []
    for service in sorted(all_services, key=lambda item: normalize_service_name(item)):
        emoji_id = get_premium_app_icon_id(service, "5352694861990501856")
        service_token = _store_callback_payload("service", service)
        service_cb = f"g_s_{service_token}"
        if len(service_cb.encode("utf-8")) > 64:
            logger.warning("Skipping service %r: callback_data exceeds Telegram's 64-byte limit", service)
            continue
        kb.append([{
            "text": f"{service}",
            "icon_custom_emoji_id": emoji_id,
            "callback_data": service_cb,
            "style": "primary"
        }])

    for button in c_msg.get("buttons", []):
        button_copy = button.copy()
        if "style" not in button_copy:
            button_copy["style"] = "primary"
        kb.append([button_copy])

    kb.append([{
        "text": "Close",
        "icon_custom_emoji_id": "5420130255174145507",
        "callback_data": "close_msg",
        "style": "danger"
    }])
    result = (txt, {"inline_keyboard": kb})
    with inventory_index_lock:
        if inventory_cache_version == version:
            service_screen_cache = (version, copy.deepcopy(result))
    return result


def _get_inventory_index():
    """Return a cached service/country index built from a brief stock snapshot."""
    global inventory_index_cache
    while True:
        with inventory_index_lock:
            if inventory_index_cache is not None:
                return inventory_index_cache
            version = inventory_cache_version

        with data_lock:
            batch_snapshot = [
                (
                    batch_id,
                    str(batch.get("service", "")).strip(),
                    str(batch.get("country", "")).strip(),
                    bool(batch.get("numbers")),
                )
                for batch_id, batch in number_batches.items()
            ]
            disabled_service_keys = {
                normalize_service_name(item)
                for item in bot_settings.get("disabled_services", [])
                if normalize_service_name(item)
            }

        services = {}
        countries_by_service = {}
        batches_by_pair = {}
        for batch_id, service, country, has_numbers in batch_snapshot:
            if (
                not service
                or not country
                or not has_numbers
                or normalize_service_name(service) in disabled_service_keys
            ):
                continue
            service_key = normalize_service_name(service)
            country_key = normalize_service_name(country)
            services.setdefault(service_key, service)
            countries_by_service.setdefault(service_key, set()).add(country)
            batches_by_pair.setdefault((service_key, country_key), []).append(batch_id)

        snapshot = {
            "version": version,
            "services": services,
            "countries_by_service": {
                key: tuple(values)
                for key, values in countries_by_service.items()
            },
            "batches_by_pair": {
                key: tuple(batch_ids)
                for key, batch_ids in batches_by_pair.items()
            },
        }
        with inventory_index_lock:
            if inventory_cache_version == version:
                inventory_index_cache = snapshot
                return snapshot

def build_country_selection_screen(service):
    """Build the country screen used by a service selection and its Back action."""
    service = normalize_service_name(service)
    index = _get_inventory_index()
    version = index["version"]
    with inventory_index_lock:
        cached = country_screen_cache.get(service)
        if cached and cached[0] == version:
            return copy.deepcopy(cached[1])

    local_countries = index["countries_by_service"].get(service, ())

    all_countries = sorted(local_countries, key=lambda item: normalize_service_name(item))
    with data_lock:
        c_msg = copy.deepcopy(
            bot_settings.get("custom_messages", {}).get("select_country", {})
        )
        flags_db = copy.deepcopy(bot_settings.get("premium_flags", {}))
    raw_txt = c_msg.get(
        "text",
        "📌 Select a country for {service}:",
    ).replace("{service}", service)
    txt = render_body_text(raw_txt)

    kb = []
    for country in all_countries:
        emoji_id = "5780471598922337683"
        for flag_data in flags_db.values():
            iso = flag_data.get("iso", "").upper()
            name = flag_data.get("name", "").upper()
            if (
                country.upper() == iso
                or country.upper() == name
                or country.upper() in name
                or name in country.upper()
            ):
                if flag_data.get("id"):
                    emoji_id = flag_data["id"]
                    break
        country_token = _store_callback_payload("service_country", service, country)
        country_cb = f"g_c_{country_token}"
        if len(country_cb.encode("utf-8")) > 64:
            logger.warning("Skipping country %r for %r: callback_data exceeds 64 bytes", country, service)
            continue
        kb.append([{
            "text": f"{country}",
            "icon_custom_emoji_id": emoji_id,
            "callback_data": country_cb,
            "style": "success",
        }])

    for button in c_msg.get("buttons", []):
        button_copy = button.copy()
        if "style" not in button_copy:
            button_copy["style"] = "primary"
        kb.append([button_copy])

    kb.append([{
        "text": "Back",
        "icon_custom_emoji_id": "5267490665117275176",
        "callback_data": "back_get_services",
        "style": "danger",
    }])
    result = (txt, {"inline_keyboard": kb})
    if all_countries:
        with inventory_index_lock:
            if inventory_cache_version == version:
                country_screen_cache[service] = (version, copy.deepcopy(result))
    return result


def _decode_legacy_service_country(payload):
    """Best-effort decoder for old raw callbacks; new buttons use opaque IDs."""
    index = _get_inventory_index()
    matches = []
    for service_key, service in index["services"].items():
        for country in index["countries_by_service"].get(service_key, ()):
            if payload == f"{service}_{country}":
                matches.append((service, country))
    if matches:
        return max(matches, key=lambda item: len(item[0]) + len(item[1]))
    service, separator, country = str(payload).partition("_")
    return (service, country) if separator and service and country else None


def fresh_get_number_screen():
    """Retry once with a fresh stock index before showing an empty screen."""
    result = build_get_number_screen()
    if result[1] is None:
        invalidate_inventory_cache()
        result = build_get_number_screen()
    return result


def inventory_diagnostic_snapshot():
    with data_lock:
        batch_counts = [
            len(batch.get("numbers", []))
            for batch in number_batches.values()
        ]
        batch_count = len(batch_counts)
        used_count = len(used_numbers_list)
        disabled_names = list(bot_settings.get("disabled_services", []))
    number_count = sum(batch_counts)
    disabled_services = {
        normalize_service_name(item)
        for item in disabled_names
        if normalize_service_name(item)
    }
    return {
        "batches": batch_count,
        "numbers": number_count,
        "disabled_services": len(disabled_services),
        "used_numbers": used_count,
    }


def selection_stock_diagnostics(user_id, service, country):
    service_key = normalize_service_name(service)
    country_key = normalize_service_name(country)
    with data_lock:
        batch_snapshot = [
            (
                normalize_service_name(batch.get("service", "")),
                normalize_service_name(batch.get("country", "")),
                tuple(batch.get("numbers", [])),
            )
            for batch in number_batches.values()
        ]
        pending_history = {
            number
            for pending_user, pending_service, pending_country, number, _ in pending_number_usage
            if (
                str(pending_user) == str(user_id)
                and pending_service == service_key
                and pending_country == country_key
            )
        }
        disabled = is_service_disabled(service)
    matching_batches = []
    same_service_other_range = False
    for batch_service, batch_country, numbers in batch_snapshot:
        if batch_service != service_key:
            continue
        if batch_country == country_key:
            matching_batches.append(numbers)
        elif numbers:
            same_service_other_range = True
    matching_numbers = [
        number
        for numbers in matching_batches
        for number in numbers
        if not number.get("to_remove")
    ]
    live_already_used = sum(
        1 for number in matching_numbers
        if user_id in number.get("used_by", [])
    )
    history_used = len(pending_history)
    try:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM number_usage "
                "WHERE user_id = ? AND service = ? AND country = ?",
                (str(user_id), service_key, country_key),
            ).fetchone()
            history_used = max(
                history_used,
                int(row[0] or 0) if row else 0,
            )
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        logger.exception("Could not read number usage history")
    already_used = max(live_already_used, history_used)
    return {
        "matching_batches": len(matching_batches),
        "matching_numbers": len(matching_numbers),
        "already_used": already_used,
        "live_already_used": live_already_used,
        "history_used": history_used,
        "same_service_other_range": same_service_other_range,
        "service_disabled": disabled,
    }

def get_admin_text():
    with user_cache_lock:
        users_count = len(all_known_users)
    with data_lock:
        total_files = len(number_batches)
        available_nums = sum(len(b["numbers"]) for b in number_batches.values())

    txt = f"""
{PEM['admin']} <b>ADMIN CONTROL PANEL</b> {PEM['admin']}
{ANIMATED_BANNER}

{PEM['graph']} <b>DATABASE OVERVIEW</b>
— — — — — — — — — —
{PEM['user']} Users      » {users_count}
{PEM['file']} Files      » {total_files}
{PEM['num']} Numbers    » {total_uploaded_stats}
{PEM['ok']} Assigned   » {total_assigned_stats}
{PEM['rocket']} Available  » {available_nums}

{PEM['graph']} <b>STOCK LEVEL</b>
— — — — — — — — — —
[██████░░░░░░░░░] {available_nums} free
"""
    return render_body_text(txt)


def system_status_text():
    """Return a concise live snapshot for the admin System Status button."""
    with user_cache_lock:
        users_count = len(all_known_users)
    with data_lock:
        batch_count = len(number_batches)
        available_numbers = sum(
            len(batch.get("numbers", []))
            for batch in number_batches.values()
        )
        force_join_on = bool(bot_settings.get("fj_on"))
    try:
        conn = get_db()
        try:
            pending_count = conn.execute(
                "SELECT COUNT(*) FROM pending_telegram_updates"
            ).fetchone()[0]
            db_user_count = conn.execute(
                "SELECT COUNT(*) FROM users"
            ).fetchone()[0]
            if db_user_count:
                users_count = db_user_count
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not query database counts for system status")
        pending_count = -1
    try:
        db_size = os.path.getsize(DB_FILE)
    except OSError:
        db_size = 0
    with chat_update_queue_lock:
        queued_chat_updates = sum(len(items) for items in chat_update_queues.values())
        active_chat_worker_count = len(active_chat_workers)
    with _scheduler_condition:
        scheduled_task_count = len(_scheduled_tasks)
    last_poll_text = (
        time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(last_poll_at))
        if last_poll_at
        else "not yet"
    )
    uptime_seconds = max(0, int(time.time() - PROCESS_STARTED_AT))
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    error_text = _redact_secret(last_polling_error or "none")
    return render_body_text(
        "<b>System Status</b>\n"
        f"Instance: <code>{INSTANCE_ID}</code>\n"
        f"PID: <code>{os.getpid()}</code>\n"
        f"Uptime: <b>{hours}h {minutes}m {seconds}s</b>\n"
        f"Users: <b>{users_count}</b>\n"
        f"Batches: <b>{batch_count}</b>\n"
        f"Available numbers: <b>{available_numbers}</b>\n"
        f"Pending updates: <b>{pending_count}</b>\n"
        f"Queued chat updates: <b>{queued_chat_updates}</b>\n"
        f"Active chat workers: <b>{active_chat_worker_count}</b>\n"
        f"State writes waiting: <b>{_state_persistence_queue.qsize()}</b>\n"
        f"Scheduled tasks: <b>{scheduled_task_count}</b>\n"
        f"Background threads: <b>{threading.active_count()}</b>\n"
        f"Last poll: <code>{last_poll_text}</code>\n"
        f"Last polling error: <code>{html.escape(error_text[:240])}</code>\n"
        f"SQLite size: <b>{db_size:,} bytes</b>\n"
        f"Force Join: <b>{'ON' if force_join_on else 'OFF'}</b>"
    )


def admin_panel_keyboard():
    return {"inline_keyboard": [
        [{"text": "LEADER BOARD SYSTEM", "icon_custom_emoji_id": "5353032893096567467", "callback_data": "lb_main", "style": "success"}],
        [{"text": "Upload Number", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "upload_num", "style": "primary"},
         {"text": "Delete files", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "delete_files", "style": "danger"}],
        [{"text": "Broadcast", "icon_custom_emoji_id": "5789428375261023681", "callback_data": "broadcast_msg", "style": "success"},
         {"text": "System", "icon_custom_emoji_id": "5420155432272438703", "callback_data": "system_settings", "style": "primary"}],
        [{"text": "Used number", "icon_custom_emoji_id": "5352694861990501856", "callback_data": "show_used", "style": "success"},
         {"text": "Unused number", "icon_custom_emoji_id": "5352597830089347330", "callback_data": "show_unused", "style": "success"}],
        [{"text": "System Status", "icon_custom_emoji_id": "5420155432272438703", "callback_data": "system_status", "style": "primary"},
         {"text": "Download backup", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "download_backup", "style": "success"}],
        # ── analytics_addon ───────────────────────────────────────────
        [{"text": "📊 Analytics", "icon_custom_emoji_id": "5352877703043258544", "callback_data": "adm_analytics", "style": "primary"}],
        # ──────────────────────────────────────────────────────────────
        [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]
    ]}

def system_settings_keyboard():
    return {"inline_keyboard": [
        [{"text": "Force Join System", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "manage_fj", "style": "primary"},
         {"text": "Admin Management", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "manage_admins", "style": "danger"}],
        [{"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "callback_data": "manage_otp_groups", "style": "danger"},
         {"text": "User Management", "icon_custom_emoji_id": "5193063022226086560", "callback_data": "user_management", "style": "primary"}],
        [{"text": "Panel MANAGEMENT", "icon_custom_emoji_id": "5336879280578138635", "callback_data": "manage_panels", "style": "danger"},
         {"text": "Subscription", "icon_custom_emoji_id": "5190899075968441286", "callback_data": "dummy_alert", "style": "success"}],
        [{"text": "WAO Control", "icon_custom_emoji_id": "5193100774988617665", "callback_data": "WAO_control", "style": "primary"},
         {"text": "Premium Emoji", "icon_custom_emoji_id": "5352552689983067014", "callback_data": "manage_emojis", "style": "success"}],
        [{"text": "Menu Design", "icon_custom_emoji_id": "5190751148704833975", "callback_data": "menu_design_list", "style": "primary"},
         {"text": "Test", "icon_custom_emoji_id": "5190781475468915802", "callback_data": "test_message_flow", "style": "primary"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]
    ]}

def number_settings_keyboard():
    return {"inline_keyboard": [
        # Use descriptive labels so these buttons have the same visual width
        # as the other system-menu buttons on Telegram clients.
        [{"text": f"Number Limit: {get_number_limit()}", "icon_custom_emoji_id": "6312260233171312151", "callback_data": "set_number_limit", "style": "success"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]
    ]}

def number_settings_text():
    return render_body_text(
        '<tg-emoji emoji-id="6312024679984929396">🔹</tg-emoji> '
        '<b>NUMBER LIMIT SETTINGS</b>\n\n'
        '<b>Do you want:</b>'
    )

def get_user_management_text():
    with user_cache_lock:
        total = len(all_known_users)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt = (
        f"{ANIMATED_BANNER}\n"
        f"👋 <b>USER VIEW</b>\n"
        f"{ANIMATED_BANNER}\n"
        f"📊 LIVE STATISTICS:\n"
        f"{ANIMATED_BANNER}\n"
        f"🫂 TOTAL USERS: {total}\n"
        f"✅ VERIFIED USERS: (Hidden to save DB Cost)\n"
        f"🚫 BANNED USERS: (Hidden to save DB Cost)\n"
        f"{ANIMATED_BANNER}\n"
        f"⌛ UPDATED: {now_str}"
    )
    return render_body_text(txt)

def user_management_keyboard():
    return {"inline_keyboard": [
        [{"text": "Manage Balance", "icon_custom_emoji_id": "5190576863226933563", "callback_data": "um_manage_balance", "style": "primary"},
         {"text": "Ban/Unban User", "icon_custom_emoji_id": "5334807341109908955", "callback_data": "um_ban_unban", "style": "danger"}],
        [{"text": "User Profile", "icon_custom_emoji_id": "5352861489541714456", "callback_data": "um_user_profile", "style": "success"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def menu_design_list_keyboard():
    return {"inline_keyboard": [
        [{"text": "Edit /start Menu", "icon_custom_emoji_id": "5395444784611480792", "callback_data": "md_edit_start", "style": "primary"}],
        [{"text": "Edit GET NUMBER", "icon_custom_emoji_id": "5337132498965010628", "callback_data": "md_edit_get_number", "style": "success"},
         {"text": "Edit Search Number", "icon_custom_emoji_id": "5190645917711114179", "callback_data": "md_edit_search_number", "style": "success"}],
        [{"text": "Edit Select Country", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "md_edit_select_country", "style": "primary"}],
        [{"text": "Edit TRAFFIC", "icon_custom_emoji_id": "5353032893096567467", "callback_data": "md_edit_traffic", "style": "primary"},
         {"text": "Edit Refer", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "md_edit_refer", "style": "primary"}],
        [{"text": "Edit WITHDRAWAL", "icon_custom_emoji_id": "5352585194295564660", "callback_data": "md_edit_withdrawal", "style": "danger"},
         {"text": "Edit SUPPORT", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "md_edit_support", "style": "danger"}],
        [{"text": "Reset Defaults", "icon_custom_emoji_id": "5192812028632274956", "callback_data": "md_reset_defaults", "style": "success"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def menu_edit_options_keyboard(menu_key):
    return {"inline_keyboard": [
        [{"text": "Edit Body (Text)", "icon_custom_emoji_id": "5395444784611480792", "callback_data": f"md_text_{menu_key}", "style": "primary"}],
        [{"text": "Edit Inline Buttons", "icon_custom_emoji_id": "5420155432272438703", "callback_data": f"md_btns_{menu_key}", "style": "success"}],
        [{"text": "Back to Menus", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "menu_design_list", "style": "danger"}]
    ]}

def menu_buttons_list_keyboard(menu_key):
    kb = []
    btns = bot_settings["custom_messages"].get(menu_key, {}).get("buttons", [])
    for idx, btn in enumerate(btns):
        kb.append([{"text": f"Del: {btn['text']}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"md_delbtn_{menu_key}_{idx}", "style": "danger"}])
    kb.append([{"text": "Add Inline Button", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"md_addbtn_{menu_key}", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"md_edit_{menu_key}", "style": "primary"}])
    return {"inline_keyboard": kb}

def emoji_settings_keyboard():
    return {"inline_keyboard": [
        [{"text": "Upload Flags (TXT)", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "up_flags_txt", "style": "primary"},
         {"text": "Download Flags", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_flags_txt", "style": "success"}],
        [{"text": "Upload Services (TXT)", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "up_apps_txt", "style": "primary"},
         {"text": "Download Services", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_apps_txt", "style": "success"}],
        [{"text": "Delete All Flags", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "del_all_flags", "style": "danger"},
         {"text": "Add Single Emoji", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_single_emoji", "style": "success"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]
    ]}

def fj_settings_text(result_message=""):
    with data_lock:
        enabled = bool(bot_settings.get("fj_on"))
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    lines = []
    if not enabled or not channels:
        lines.append(
            f"{PEM['warn']} <b>Users can currently enter without Force Join.</b>"
        )
    lines.extend([
        f"{PEM['link']} <b>FORCE JOIN SYSTEM</b>",
        f"Status: <b>{'ON' if enabled else 'OFF'}</b> · Channels: <b>{len(channels)}</b>",
    ])
    if result_message:
        lines.append("")
        lines.append(result_message)
    return render_body_text("\n".join(lines))


def fj_settings_keyboard():
    with data_lock:
        enabled = bool(bot_settings.get("fj_on"))
        channels = [
            normalize_force_join_channel(channel)
            for channel in bot_settings.get("fj_channels", [])
        ]
    channels = [
        _refresh_force_join_channel_metadata(channel)
        for channel in channels
    ]
    status_text = "ON" if enabled else "OFF"
    status_icon = (
        "5352694861990501856"
        if enabled
        else "5318840353510408444"
    )
    kb = [[{
        "text": f"STATUS: {status_text}",
        "icon_custom_emoji_id": status_icon,
        "callback_data": "toggle_fj",
        "style": "primary",
    }]]
    for idx, channel in enumerate(channels):
        is_bot_admin, _ = _force_join_bot_admin_status(channel)
        admin_icon = "✅" if is_bot_admin is True else "⚠️"
        label = channel.get("title") or channel.get("id") or "Unknown channel"
        kb.append([{
            "text": f"{admin_icon} {label[:43]} · Remove",
            "icon_custom_emoji_id": "5420130255174145507",
            "callback_data": f"del_fj_{idx}",
            "style": "danger",
        }])
        if (
            not channel.get("username")
            and not _valid_force_join_invite_url(channel.get("invite_link"))
        ):
            kb.append([{
                "text": f"Set invite link: {label[:38]}",
                "icon_custom_emoji_id": "5420517437885943844",
                "callback_data": f"set_fj_invite_{idx}",
                "style": "primary",
            }])
    kb.append([{
        "text": "Test Force Join",
        "icon_custom_emoji_id": "5352694861990501856",
        "callback_data": "test_fj",
        "style": "primary",
    }])
    kb.append([{
        "text": "Add Channel",
        "icon_custom_emoji_id": "5420323438508155202",
        "callback_data": "add_fj",
        "style": "success",
    }])
    kb.append([{
        "text": "Back",
        "icon_custom_emoji_id": "5267490665117275176",
        "callback_data": "system_settings",
        "style": "primary",
    }])
    return {"inline_keyboard": kb}

def admin_settings_keyboard():
    kb = []
    for idx, adm in enumerate(bot_settings["admins"]):
        text_btn = f"Owner: {adm}" if adm == OWNER_ID else f"Delete: {adm}"
        icon_id = "5353032893096567467" if adm == OWNER_ID else "5420130255174145507"
        cb_data = "ignore" if adm == OWNER_ID else f"del_adm_{idx}"
        kb.append([{"text": text_btn, "icon_custom_emoji_id": icon_id, "callback_data": cb_data, "style": "danger" if adm != OWNER_ID else "primary"}])
    kb.append([{"text": "Add Admin", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_adm", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}])
    return {"inline_keyboard": kb}

def otp_groups_list_keyboard():
    kb = [[{"text": "Edit OTP Button Link", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "edit_otp_link", "style": "primary"}]]
    kb.append([{"text": f"Inline Buttons ({len(bot_settings.get('otp_inline_buttons') or [])})", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "oib_menu", "style": "primary"}])
    for idx, fg in enumerate(bot_settings["fw_groups"]):
        kb.append([{"text": f"Group: {fg['chat_id']}", "icon_custom_emoji_id": "5193063022226086560", "callback_data": f"manage_fw_{idx}", "style": "primary"}])
    kb.append([{"text": "Add Forward Group", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_fw", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}])
    return {"inline_keyboard": kb}


def otp_inline_buttons_text():
    with data_lock:
        items = normalize_otp_inline_buttons(bot_settings.get("otp_inline_buttons"))
    lines = [
        "🔘 <b>OTP INLINE BUTTONS</b>",
        ANIMATED_BANNER,
        "Shown under every OTP group message (2 per row).",
        "✅ enabled · ⛔ disabled (tap the name to switch)",
        "",
    ]
    if not items:
        lines.append("<i>No buttons yet. Tap Add Inline Button.</i>")
    for i, b in enumerate(items, 1):
        lines.append(f"{i}. {'✅' if b['enabled'] else '⛔'} <b>{html.escape(b['text'])}</b> — {html.escape(b['url'])}")
    return "\n".join(lines)


def otp_inline_buttons_keyboard():
    with data_lock:
        items = normalize_otp_inline_buttons(bot_settings.get("otp_inline_buttons"))
    kb = []
    for b in items:
        kb.append([
            {"text": f"{'✅' if b['enabled'] else '⛔'} {b['text'][:20]}", "callback_data": f"oib_tog_{b['id']}",
             "style": "success" if b["enabled"] else "danger"},
            {"text": "Edit", "icon_custom_emoji_id": "5420517437885943844", "callback_data": f"oib_edit_{b['id']}", "style": "primary"},
            {"text": "Del", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"oib_del_{b['id']}", "style": "danger"},
        ])
    kb.append([{"text": "Add Inline Button", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "oib_add", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}])
    return {"inline_keyboard": kb}


def _oib_cancel_kb():
    return {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "oib_cancel", "style": "danger"}]]}


def _oib_show_menu(chat_id, msg_id):
    text, kb = render_body_text(otp_inline_buttons_text()), otp_inline_buttons_keyboard()
    if msg_id:
        edit_message(chat_id, msg_id, text, reply_markup=kb)
    else:
        send_message(chat_id, text, reply_markup=kb)


def _oib_find(bid):
    for b in bot_settings.get("otp_inline_buttons") or []:
        if isinstance(b, dict) and str(b.get("id")) == str(bid):
            return b
    return None


def handle_oib_callback(call, data, chat_id, msg_id):
    """Admin Panel -> System -> OTP GROUP -> Inline Buttons (Add/Edit/Delete/Enable/Disable)."""
    cid = call.get("id")
    if data == "oib_menu":
        _oib_show_menu(chat_id, msg_id)
        answer_callback(cid)
    elif data == "oib_cancel":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        _oib_show_menu(chat_id, msg_id)
        answer_callback(cid)
    elif data == "oib_add":
        safe_set_user_state(chat_id, "wait_for_oib_add")
        safe_set_temp_data(chat_id, {"msg_id": msg_id})
        edit_message(chat_id, msg_id, render_body_text(
            "📝 Send the button as <code>NAME - https://link</code>\n"
            "Examples:\n<code>BOT - https://t.me/example</code>\n<code>CHANNEL - https://t.me/examplechannel</code>"),
            reply_markup=_oib_cancel_kb())
        answer_callback(cid)
    elif data.startswith("oib_edit_"):
        bid = data[len("oib_edit_"):]
        with data_lock:
            item = _oib_find(bid)
            cur = f"{item.get('text')} - {item.get('url')}" if item else None
        if not item:
            answer_callback(cid, "❌ Button not found.", show_alert=True)
            _oib_show_menu(chat_id, msg_id)
            return
        safe_set_user_state(chat_id, "wait_for_oib_edit")
        safe_set_temp_data(chat_id, {"msg_id": msg_id, "oib_id": bid})
        edit_message(chat_id, msg_id, render_body_text(
            f"📝 Current: <code>{html.escape(cur)}</code>\nSend the new <code>NAME - https://link</code>:"),
            reply_markup=_oib_cancel_kb())
        answer_callback(cid)
    elif data.startswith("oib_tog_"):
        bid = data[len("oib_tog_"):]
        with data_lock:
            item = _oib_find(bid)
            if item:
                item["enabled"] = not bool(item.get("enabled", True))
                state_now = item["enabled"]
                save_db()
        if not item:
            answer_callback(cid, "❌ Button not found.", show_alert=True)
        else:
            answer_callback(cid, "✅ Enabled" if state_now else "⛔ Disabled")
        _oib_show_menu(chat_id, msg_id)
    elif data.startswith("oib_del_"):
        bid = data[len("oib_del_"):]
        with data_lock:
            before = len(bot_settings.get("otp_inline_buttons") or [])
            bot_settings["otp_inline_buttons"] = [
                b for b in (bot_settings.get("otp_inline_buttons") or [])
                if not (isinstance(b, dict) and str(b.get("id")) == bid)
            ]
            removed = len(bot_settings["otp_inline_buttons"]) < before
            if removed:
                save_db()
        answer_callback(cid, "✅ Button deleted!" if removed else "❌ Button not found.", show_alert=True)
        _oib_show_menu(chat_id, msg_id)
    else:
        answer_callback(cid)


def handle_oib_text_state(chat_id, state, text, msg, temp):
    """Add/Edit input: "NAME - https://link" (name may itself contain "-")."""
    parsed = parse_otp_inline_button_input(text, msg.get("entities"))
    if not parsed:
        send_message(chat_id, render_body_text(
            "❌ Invalid button. Use <code>BOT - https://t.me/example</code> "
            "(valid http/https link, name up to 32 characters)."), reply_markup=_oib_cancel_kb())
        return
    name, url, emoji_id = parsed
    temp = temp or {}
    with data_lock:
        items = bot_settings.setdefault("otp_inline_buttons", [])
        if state == "wait_for_oib_add":
            if any(_otp_inline_url_key(b.get("url")) == _otp_inline_url_key(url) for b in items if isinstance(b, dict)):
                dup = True
            else:
                dup = False
                new_item = normalize_otp_inline_button({
                    "id": uuid.uuid4().hex[:8], "text": name, "url": url,
                    "emoji_id": emoji_id, "enabled": True})
                items.append(new_item)
                save_db()
        else:
            dup = False
            item = _oib_find(temp.get("oib_id"))
            if item is None:
                dup = None
            else:
                if any(b is not item and isinstance(b, dict) and _otp_inline_url_key(b.get("url")) == _otp_inline_url_key(url) for b in items):
                    dup = True
                else:
                    item["text"], item["url"] = name, url
                    if emoji_id:
                        item["emoji_id"] = emoji_id
                    save_db()
    if dup is None:
        send_message(chat_id, render_body_text("❌ Button no longer exists."))
    elif dup:
        send_message(chat_id, render_body_text("❌ A button with this link already exists. Send a different link."), reply_markup=_oib_cancel_kb())
        return
    delete_message(chat_id, msg["message_id"])
    safe_set_user_state(chat_id, None)
    safe_set_temp_data(chat_id, None)
    _oib_show_menu(chat_id, temp.get("msg_id"))

def specific_fw_group_keyboard(idx):
    group = bot_settings["fw_groups"][idx]
    kb = []
    for b_idx, btn in enumerate(group.get("buttons", [])):
        kb.append([{"text": f"Del: {btn['text']}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_fwbtn_{idx}_{b_idx}", "style": "danger"}])
    
    kb.append([{"text": "Add Inline Button", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"add_fwbtn_{idx}", "style": "success"}])
    kb.append([{"text": "Delete Entire Group", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"del_fw_{idx}", "style": "danger"}])
    kb.append([{"text": "Back to Groups", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "primary"}])
    return {"inline_keyboard": kb}

def WAO_control_keyboard():
    w_status = "ON" if bot_settings["withdraw_on"] else "OFF"
    sup_status = "ON" if bot_settings.get("support_link") else "OFF"
    grp_status = "ON" if bot_settings.get("w_group") else "OFF"
    return {"inline_keyboard": [
        [{"text": f"WITHDRAW: {w_status}", "icon_custom_emoji_id": "5348469219761626211", "callback_data": "WAO_toggle_w", "style": "primary"}],
        [{"text": f"MIN WITHDRAW: {bot_settings['min_withdraw']}", "icon_custom_emoji_id": "5352877703043258544", "callback_data": "WAO_min_w", "style": "success"},
         {"text": f"OTP REWARD: {bot_settings['otp_reward']}", "icon_custom_emoji_id": "5190576863226933563", "callback_data": "WAO_otp_r", "style": "primary"}],
        [{"text": f"REFER REWARD: {bot_settings['refer_reward']}", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "WAO_ref_r", "style": "success"},
         {"text": f"COOLDOWN: {get_change_number_cooldown()}s", "icon_custom_emoji_id": "5337172996211648018", "callback_data": "WAO_cool", "style": "primary"}],
        [{"text": f"NUM/REQ: {get_number_limit()}", "icon_custom_emoji_id": "5337132498965010628", "callback_data": "WAO_num_req", "style": "success"},
         {"text": f"NUM/SHARE: {bot_settings['num_share']}", "icon_custom_emoji_id": "5352862640592949843", "callback_data": "WAO_num_share", "style": "primary"}],
        [{"text": f"SUPPORT LINK: {sup_status}", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "WAO_sup_link", "style": "success"},
         {"text": "W. METHODS", "icon_custom_emoji_id": "5190899075968441286", "callback_data": "manage_w_methods", "style": "primary"}],
        [{"text": f"W. GROUP: {grp_status}", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "WAO_w_group", "style": "success"},
         {"text": "BACK", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}],
        [{"text": "💲 SERVICE × COUNTRY PRICES", "icon_custom_emoji_id": "5190576863226933563",
          "callback_data": "service_prices", "style": "success"}],
    ]}

def w_methods_keyboard():
    kb = []
    for idx, m in enumerate(bot_settings["w_methods"]):
        kb.append([{"text": f"Delete: {m}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_wm_{idx}", "style": "danger"}])
    kb.append([{"text": "Add Method", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_wm", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "WAO_control", "style": "primary"}])
    return {"inline_keyboard": kb}

def service_prices_keyboard():
    """Admin keyboard to manage per-service-per-country OTP prices."""
    kb = []
    with data_lock:
        price_map = dict(bot_settings.get("service_prices") or {})
    for key in sorted(price_map.keys()):
        val = fmt_money(price_map[key])
        kb.append([
            {"text": f"✏️ {key}: {val}৳", "callback_data": f"sp_edit_{key}", "style": "primary"},
            {"text": "🗑", "callback_data": f"sp_del_{key}", "style": "danger"},
        ])
    kb.append([{"text": "➕ Add Price Rule", "icon_custom_emoji_id": "5420323438508155202",
                "callback_data": "sp_add", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176",
                "callback_data": "WAO_control", "style": "danger"}])
    return {"inline_keyboard": kb}

def typed_panels_list_keyboard(p_type):
    kb = []
    for idx, p in enumerate(bot_settings["panels"]):
        if p.get("type", "API Panel") != p_type:
            continue
        action_text = f"Turn OFF {p['name']}" if p['status'] == 'ON' else f"Turn ON {p['name']}"
        action_icon = "5318840353510408444" if p['status'] == 'ON' else "5192812028632274956"
        icon_id = "5420155432272438703"
        kb.append([
            {"text": action_text, "icon_custom_emoji_id": action_icon, "callback_data": f"tog_pnl_{idx}", "style": "danger" if p['status'] == 'ON' else "success"},
            {"text": f"{p['name']}", "icon_custom_emoji_id": icon_id, "callback_data": f"conf_pnl_{idx}", "style": "primary"}
        ])
    add_cb = "add_api_panel" if p_type == "API Panel" else "add_cpt_panel"
    kb.append([{"text": "Add New Provider", "icon_custom_emoji_id": "5420323438508155202", "callback_data": add_cb, "style": "success"}])
    kb.append([{"text": "Delete Provider", "icon_custom_emoji_id": "5336944168944047463", "callback_data": f"list_del_{'api' if p_type=='API Panel' else 'cpt'}", "style": "danger"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_panels", "style": "primary"}])
    return {"inline_keyboard": kb}

def panel_config_keyboard(idx):
    p = bot_settings["panels"][idx]
    
    kb = []
    action_text = "Turn OFF" if p['status'] == 'ON' else "Turn ON"
    action_icon = "5318840353510408444" if p['status'] == 'ON' else "5192812028632274956"
    kb.append([{"text": action_text, "icon_custom_emoji_id": action_icon, "callback_data": f"tog_pnl_{idx}", "style": "danger" if p['status'] == 'ON' else "success"}])
    
    if p["type"] != "Auto Captcha Panel":
        rec_count_text = "All (Unlimited)" if p.get('records', 0) == 0 else str(p.get('records'))
        kb.append([{"text": "Set API URL", "icon_custom_emoji_id": "5420517437885943844", "callback_data": f"set_p_api_{idx}", "style": "primary"}])
        kb.append([{"text": "Set Token", "icon_custom_emoji_id": "5353022963132174959", "callback_data": f"set_p_tok_{idx}", "style": "primary"}])
        kb.append([{"text": "🌐 Full API (URL+Token)", "icon_custom_emoji_id": "5420517437885943844", "callback_data": f"set_p_fapi_{idx}", "style": "primary"}])
        kb.append([{"text": f"Set Records Count: {rec_count_text}", "icon_custom_emoji_id": "5192739271886282680", "callback_data": f"set_p_rec_{idx}", "style": "primary"}])
        
    kb.append([{"text": "Test Connection", "icon_custom_emoji_id": "5352694861990501856", "callback_data": f"test_p_conn_{idx}", "style": "success"}])
        
    back_data = "manage_api_panels" if p.get("type", "API Panel") == "API Panel" else "manage_cpt_panels"
    kb.append([{"text": "Back to Providers", "icon_custom_emoji_id": "5267490665117275176", "callback_data": back_data, "style": "danger"}])
    return {"inline_keyboard": kb}

def build_traffic_ui():
    global recent_traffic
    current_time = time.time()
    with data_lock:
        recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
    
    stats = {}
    for t in recent_traffic:
        srv = t.get("service", "Unknown")
        iso = t.get("iso", "XX")
        flag = t.get("flag", "🌍")
        
        if srv not in stats:
            stats[srv] = {}
        if iso not in stats[srv]:
            stats[srv][iso] = {"count": 0, "flag": flag}
        stats[srv][iso]["count"] += 1
        
    txt = f"📈 <b>NETWORK TRAFFIC</b>\n{ANIMATED_BANNER}\n\n"
    
    kb = []
    if not stats:
        txt += "<i>No recent traffic found in the last hour...</i>\n"
    else:
        srv_totals = []
        for srv, countries in stats.items():
            total = sum(c["count"] for c in countries.values())
            srv_totals.append((srv, total, countries))
        
        srv_totals.sort(key=lambda x: x[1], reverse=True)
        
        for srv, total, countries in srv_totals:
            app_full_name, prem_app_html = get_service_info_html(srv)
            txt += f"[ {prem_app_html} <b>{html.escape(str(app_full_name))}</b> ]\n│\n"
            
            c_list = sorted(countries.items(), key=lambda x: x[1]["count"], reverse=True)
            c_list = c_list[:7]
            
            for i, (iso, c_data) in enumerate(c_list):
                prem_flag_html = get_flag_info_html(iso)
                count = c_data["count"]
                
                c_name = iso
                for code, fdata in bot_settings.get("premium_flags", {}).items():
                    if fdata.get("iso") == iso:
                        c_name = fdata.get("name", iso)
                        break
                        
                txt += f"├ {prem_flag_html} <b>{c_name} ({iso})</b>\n"
                txt += f"│ ╰ Success: {count}\n"
                if i < len(c_list) - 1:
                    txt += "│\n"
            txt += "\n"
        
        for srv, _, _ in srv_totals:
            safe_srv = srv
            app_full_name, _ = get_service_info_html(safe_srv, safe_srv)
            explore_token = _store_callback_payload("explore_service", safe_srv)
            kb.append([{"text": f"Explore {app_full_name} Range", "icon_custom_emoji_id": "5190645917711114179", "callback_data": f"exp_rng_{explore_token}", "style": "success"}])
            
    txt = render_body_text(txt)
    kb.append([{"text": "Refresh", "icon_custom_emoji_id": "5465368548702446780", "callback_data": "refresh_traffic", "style": "primary"}])
    kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
    
    return txt, {"inline_keyboard": kb}

# ==========================================
# Message Handler
# ==========================================
def generate_emoji_txt(mode="flags"):
    if mode == "flags":
        lines = []
        for key, data in bot_settings.get("premium_flags", {}).items():
            code = data.get("code") or (key if str(key).isdigit() else "")
            line = f"{data['char']} ({code}) ({data['iso']}) {data['name']} {json.dumps({'emoji': data['char'], 'id': data['id']}, ensure_ascii=False)}"
            lines.append(line)
        return "\n".join(lines)
    else:
        lines = []
        for name, data in bot_settings.get("premium_apps", {}).items():
            line = f"{data['char']} {data['name']} {json.dumps({'emoji': data['char'], 'id': data['id']})}"
            lines.append(line)
        return "\n".join(lines)

def parse_service_app_line(line):
    """Parse one service-app TXT row without confusing the leading emoji.

    Some exported rows use a visible emoji that differs from the emoji stored
    in the JSON payload.  Removing only ``data['emoji']`` can therefore leave
    the visible emoji in the service name or produce a wrong key.  The first
    token is the display icon; the remaining text is always the service name.
    """
    json_match = re.search(r'(\{.*\})', str(line))
    if not json_match:
        return None
    try:
        data = json.loads(json_match.group(1))
    except (TypeError, json.JSONDecodeError):
        return None

    char = data.get("emoji")
    eid = data.get("id")
    prefix = str(line)[:json_match.start()].strip()
    if not prefix or not char or not eid:
        return None

    if prefix.startswith(str(char)):
        name = prefix[len(str(char)):].strip()
    else:
        prefix_parts = prefix.split(None, 1)
        name = prefix_parts[1].strip() if len(prefix_parts) == 2 else prefix

    if not name:
        return None
    return name, char, str(eid)

def parse_flag_emoji_line(line):
    match = re.search(r"(\{.*\})", str(line))
    if not match:
        return None
    try:
        payload = json.loads(match.group(1))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    prefix = str(line)[:match.start()].strip()
    code_match = re.search(r"\((\d{1,4})\)", prefix)
    iso_match = re.search(r"\(([A-Za-z]{2})\)", prefix)
    char = payload.get("emoji")
    emoji_id = payload.get("id")
    if not code_match or not iso_match or not isinstance(char, str) or not char or not emoji_id:
        return None
    name = prefix
    name = re.sub(r"\(\d{1,4}\)", "", name, count=1)
    name = re.sub(r"\([A-Za-z]{2}\)", "", name, count=1)
    if name.startswith(char):
        name = name[len(char):]
    name = name.strip()
    if not name:
        return None
    return {
        "code": code_match.group(1),
        "iso": iso_match.group(1).upper(),
        "name": name,
        "char": char,
        "id": str(emoji_id),
    }


def process_emoji_text_upload(chat_id, state, temp, doc):
    prompt_id = (temp or {}).get("msg_id")
    filename = str(doc.get("file_name", ""))
    if not filename.lower().endswith(".txt"):
        message = render_body_text(f"❌ Please upload a <b>.txt</b> file only.\n{ 'Example: Flag_Emojis.txt' if state == 'wait_for_flag_txt' else 'Example: Service_Apps.txt' }")
        if prompt_id:
            edit_message(chat_id, prompt_id, message, reply_markup=get_cancel_kb())
        else:
            send_message(chat_id, message, reply_markup=get_cancel_kb())
        return
    try:
        file_size = int(doc.get("file_size", 0) or 0)
    except (TypeError, ValueError):
        file_size = 0
    if file_size > MAX_NUMBER_UPLOAD_BYTES:
        message = render_body_text("❌ TXT file exceeds the 10 MB upload limit. Please upload a smaller UTF-8 .txt file.")
        if prompt_id:
            edit_message(chat_id, prompt_id, message, reply_markup=get_cancel_kb())
        else:
            send_message(chat_id, message, reply_markup=get_cancel_kb())
        return

    started = time.monotonic()
    if prompt_id:
        edit_message(chat_id, prompt_id, "⏳ Processing emoji file…", reply_markup=get_cancel_kb())
    else:
        send_message(chat_id, "⏳ Processing emoji file…", reply_markup=get_cancel_kb())

    old_flags = copy.deepcopy(bot_settings.get("premium_flags", {}))
    old_apps = copy.deepcopy(bot_settings.get("premium_apps", {}))
    try:
        content = download_telegram_file(doc["file_id"], max_bytes=MAX_NUMBER_UPLOAD_BYTES).decode("utf-8-sig", errors="replace")
        mode = "flags" if state == "wait_for_flag_txt" else "apps"
        records = []
        invalid = []
        for line_number, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith(("#", ";")):
                continue
            parsed = parse_flag_emoji_line(line) if mode == "flags" else parse_service_app_line(line)
            if parsed:
                records.append((line_number, parsed))
            else:
                invalid.append((line_number, stripped[:120]))

        added = updated = duplicates = 0
        with data_lock:
            for _, record in records:
                if mode == "flags":
                    iso = record["iso"]
                    existing = bot_settings["premium_flags"].get(iso)
                    duplicate_id = premium_emoji_id_exists(record["id"], ignore_flag_code=iso)
                    if duplicate_id:
                        duplicates += 1
                        continue
                    normalized = {
                        "char": record["char"], "code": record["code"],
                        "iso": iso, "name": record["name"], "id": record["id"],
                    }
                    if existing == normalized:
                        duplicates += 1
                    elif existing:
                        bot_settings["premium_flags"][iso] = normalized
                        updated += 1
                    else:
                        bot_settings["premium_flags"][iso] = normalized
                        added += 1
                else:
                    name, char, emoji_id = record
                    key = name.upper()
                    existing = bot_settings["premium_apps"].get(key)
                    normalized = {"char": char, "id": str(emoji_id), "name": name}
                    if existing == normalized:
                        duplicates += 1
                    elif existing:
                        bot_settings["premium_apps"][key] = normalized
                        updated += 1
                    else:
                        bot_settings["premium_apps"][key] = normalized
                        added += 1
            if added + updated:
                save_db()

        elapsed = time.monotonic() - started
        if added + updated == 0:
            bot_settings["premium_flags"] = old_flags
            bot_settings["premium_apps"] = old_apps
            mode_help = (
                "Expected flag line: <code>🇧🇩 (880) (BD) Bangladesh {&quot;emoji&quot;: &quot;🇧🇩&quot;, &quot;id&quot;: &quot;CUSTOM_EMOJI_ID&quot;}</code>"
                if mode == "flags"
                else "Expected app line: <code>🚫 WhatsApp {&quot;emoji&quot;: &quot;??&quot;, &quot;id&quot;: &quot;CUSTOM_EMOJI_ID&quot;}</code>"
            )
            summary = (
                f"❌ No new or updated records; upload not applied ({elapsed:.2f}s).\n"
                f"New: {added} | Updated: {updated} | Duplicates skipped: {duplicates} | Invalid lines: {len(invalid)}\n"
                f"{mode_help}"
            )
        else:
            invalidate_inventory_cache()
            summary = (
                f"✅ Emoji file processed in {elapsed:.2f}s.\n"
                f"New: {added} | Updated: {updated} | Duplicates skipped: {duplicates} | Invalid lines: {len(invalid)}"
            )
        if invalid:
            examples = "\n".join(
                f"• Line {line_no}: <code>{html.escape(example)}</code>"
                for line_no, example in invalid[:3]
            )
            summary += f"\n\nFirst invalid lines:\n{examples}"
        if added + updated:
            reply_markup = emoji_settings_keyboard()
        else:
            reply_markup = get_cancel_kb()
        rendered = render_body_text(summary)
        if prompt_id:
            edit_message(chat_id, prompt_id, rendered, reply_markup=reply_markup)
        else:
            send_message(chat_id, rendered, reply_markup=reply_markup)
        if added + updated:
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
    except Exception:
        bot_settings["premium_flags"] = old_flags
        bot_settings["premium_apps"] = old_apps
        logger.exception("Error loading premium emoji file for admin %s", chat_id)
        error = render_body_text("❌ Could not process the emoji file. Your setup is still open; please retry with a UTF-8 .txt file.")
        if prompt_id:
            edit_message(chat_id, prompt_id, error, reply_markup=get_cancel_kb())
        else:
            send_message(chat_id, error, reply_markup=get_cancel_kb())


def premium_emoji_id_exists(emoji_id, ignore_app_name=None, ignore_flag_code=None):
    target = str(emoji_id or "")
    if not target:
        return False

    ignored_app_key = normalize_service_name(ignore_app_name)
    for code, item in bot_settings.get("premium_flags", {}).items():
        if not isinstance(item, dict):
            continue
        ignored_values = {
            normalize_service_name(code),
            normalize_service_name(item.get("iso", "")),
            normalize_service_name(item.get("code", "")),
        }
        if ignore_flag_code and normalize_service_name(ignore_flag_code) in ignored_values:
            continue
        if str(item.get("id", "")) == target:
            return True

    for app_name, item in bot_settings.get("premium_apps", {}).items():
        if ignored_app_key and normalize_service_name(app_name) == ignored_app_key:
            continue
        if str(item.get("id", "")) == target:
            return True
    return False

def normalize_phone_number(value):
    """Normalize a phone number from a one-number-per-line upload."""
    candidate = str(value or "").strip().lstrip("\ufeff")
    candidate = re.sub(r"[\s().-]", "", candidate)
    if candidate.startswith("00"):
        candidate = "+" + candidate[2:]
    if not re.fullmatch(r"\+?[0-9]{5,20}", candidate):
        return None
    return candidate if candidate.startswith("+") else "+" + candidate

def parse_number_upload(content):
    """Return normalized unique numbers, invalid source lines, and duplicate count."""
    numbers = []
    seen = set()
    invalid_lines = []
    duplicates = 0
    for line_number, raw_line in enumerate(str(content or "").splitlines(), start=1):
        line = raw_line.strip().lstrip("\ufeff")
        if not line or line.startswith(("#", ";")):
            continue
        number = normalize_phone_number(line)
        if not number:
            invalid_lines.append(line_number)
        elif number in seen:
            duplicates += 1
        else:
            seen.add(number)
            numbers.append(number)
    return numbers, invalid_lines, duplicates

known_numbers = {
    normalized
    for batch in number_batches.values()
    for item in batch.get("numbers", [])
    if (
        normalized := normalize_phone_number(
            item.get("num", "") if isinstance(item, dict) else item
        )
    )
}
known_numbers.update(
    normalized
    for value in used_numbers_list
    if (normalized := normalize_phone_number(value))
)

def _rebuild_known_numbers():
    """Rebuild known_numbers in-place from current number_batches + used_numbers_list.
    Must be called inside data_lock.
    """
    known_numbers.clear()
    for batch in number_batches.values():
        for item in batch.get("numbers", []):
            raw = item.get("num", "") if isinstance(item, dict) else item
            n = normalize_phone_number(raw)
            if n:
                known_numbers.add(n)
    for value in used_numbers_list:
        n = normalize_phone_number(value)
        if n:
            known_numbers.add(n)

def clear_pending_number_upload(chat_id):
    try:
        conn = get_db()
        try:
            conn.execute(
                "DELETE FROM pending_number_uploads WHERE user_id = ?",
                (str(chat_id),),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not clear saved number-upload state for chat %s", chat_id)

def persist_pending_number_upload(chat_id):
    with user_states_lock:
        state = user_states.get(chat_id)
    if state not in PENDING_NUMBER_UPLOAD_STATES:
        clear_pending_number_upload(chat_id)
        return
    with temp_data_lock:
        data = temp_data.get(chat_id)
        data = copy.deepcopy(data) if isinstance(data, dict) else {}
    try:
        conn = get_db()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO pending_number_uploads "
                "(user_id, state, data, updated_at) VALUES (?, ?, ?, ?)",
                (str(chat_id), state, json.dumps(data), time.time()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not persist number-upload state for chat %s", chat_id)

def clear_persisted_admin_interaction(chat_id):
    try:
        conn = get_db()
        try:
            conn.execute(
                "DELETE FROM pending_admin_interactions WHERE user_id = ?",
                (str(chat_id),),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not clear persisted admin interaction for chat %s", chat_id)
    admin_interaction_updated_at.pop(chat_id, None)


def persist_admin_interaction(chat_id):
    state = safe_get_user_state(chat_id)
    if state not in ADMIN_INTERACTION_STATES or not is_admin(chat_id):
        clear_persisted_admin_interaction(chat_id)
        return
    with temp_data_lock:
        data = temp_data.get(chat_id)
        data = copy.deepcopy(data) if isinstance(data, dict) else {}
    prompt_id = admin_prompt_messages.get(chat_id) or data.get("msg_id")
    updated_at = time.time()
    try:
        serialized = json.dumps(encrypt_interaction_data(data), ensure_ascii=False)
        conn = get_db()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO pending_admin_interactions "
                "(user_id, state, data, prompt_message_id, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (str(chat_id), state, serialized, prompt_id, updated_at),
            )
            conn.commit()
        finally:
            conn.close()
        admin_interaction_updated_at[chat_id] = updated_at
    except (TypeError, ValueError):
        logger.exception("Admin interaction data is not JSON-serializable for chat %s", chat_id)
    except Exception:
        logger.exception("Could not persist admin interaction for chat %s", chat_id)


def notify_expired_admin_interactions():
    for chat_id in sorted(set(expired_admin_interaction_chats)):
        try:
            send_message(
                chat_id,
                "⚠️ Your current step expired after 30 minutes. Please start that step again.",
                reply_markup=main_menu(chat_id),
            )
        except Exception:
            logger.exception("Could not notify chat %s about expired admin setup", chat_id)
    expired_admin_interaction_chats.clear()


def expire_admin_interaction_if_needed(chat_id, state):
    if state not in PERSISTED_INTERACTION_STATES:
        return False
    updated_at = admin_interaction_updated_at.get(chat_id)
    if not updated_at or time.time() - updated_at <= ADMIN_INTERACTION_TTL_SECONDS:
        return False
    safe_set_user_state(chat_id, None)
    safe_set_temp_data(chat_id, None)
    send_message(
        chat_id,
        "⚠️ Your current step expired after 30 minutes. Please start that step again.",
        reply_markup=main_menu(chat_id),
    )
    return True


_state_persistence_queue = queue.Queue()
_state_persistence_thread = None
_state_persistence_start_lock = threading.Lock()


def _state_persistence_worker():
    """Serialize interactive-state writes away from Telegram handler threads."""
    while True:
        chat_id, state, data, updated_at = _state_persistence_queue.get()
        try:
            conn = get_db()
            try:
                user_key = str(chat_id)
                if state not in PERSISTED_INTERACTION_STATES:
                    conn.execute(
                        "DELETE FROM pending_user_states WHERE user_id = ?",
                        (user_key,),
                    )
                    conn.execute(
                        "DELETE FROM pending_number_uploads WHERE user_id = ?",
                        (user_key,),
                    )
                    conn.execute(
                        "DELETE FROM pending_admin_interactions WHERE user_id = ?",
                        (user_key,),
                    )
                else:
                    safe_data = data if isinstance(data, dict) else {}
                    serialized = json.dumps(
                        encrypt_interaction_data(safe_data),
                        ensure_ascii=False,
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO pending_user_states "
                        "(user_id, state, data, updated_at) VALUES (?, ?, ?, ?)",
                        (user_key, state, serialized, updated_at),
                    )
                    if state in PENDING_NUMBER_UPLOAD_STATES:
                        conn.execute(
                            "INSERT OR REPLACE INTO pending_number_uploads "
                            "(user_id, state, data, updated_at) VALUES (?, ?, ?, ?)",
                            (user_key, state, json.dumps(safe_data), updated_at),
                        )
                    else:
                        conn.execute(
                            "DELETE FROM pending_number_uploads WHERE user_id = ?",
                            (user_key,),
                        )
                    if state in ADMIN_INTERACTION_STATES and is_admin(chat_id):
                        prompt_id = admin_prompt_messages.get(chat_id) or safe_data.get("msg_id")
                        conn.execute(
                            "INSERT OR REPLACE INTO pending_admin_interactions "
                            "(user_id, state, data, prompt_message_id, updated_at) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (user_key, state, serialized, prompt_id, updated_at),
                        )
                    else:
                        conn.execute(
                            "DELETE FROM pending_admin_interactions WHERE user_id = ?",
                            (user_key,),
                        )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Could not persist interactive state for chat %s", chat_id)
        finally:
            _state_persistence_queue.task_done()


def _ensure_state_persistence_worker():
    global _state_persistence_thread
    with _state_persistence_start_lock:
        if _state_persistence_thread is None or not _state_persistence_thread.is_alive():
            _state_persistence_thread = threading.Thread(
                target=_state_persistence_worker,
                name="bot-state-writer",
                daemon=True,
            )
            _state_persistence_thread.start()


def _flush_state_persistence_on_exit():
    if _state_persistence_thread is not None:
        _state_persistence_queue.join()


atexit.register(_flush_state_persistence_on_exit)


def _queue_interaction_state(chat_id, state):
    if state not in PERSISTED_INTERACTION_STATES:
        data = {}
    else:
        with temp_data_lock:
            current = temp_data.get(chat_id)
            data = copy.deepcopy(current) if isinstance(current, dict) else {}
    _ensure_state_persistence_worker()
    _state_persistence_queue.put(
        (chat_id, state, data, time.time())
    )


def safe_get_user_state(chat_id):
    with user_states_lock:
        return user_states.get(chat_id)

def safe_set_user_state(chat_id, state):
    with user_states_lock:
        previous_state = user_states.get(chat_id)
        if state is None:
            user_states.pop(chat_id, None)
        else:
            user_states[chat_id] = state
    if state in PERSISTED_INTERACTION_STATES:
        admin_interaction_updated_at[chat_id] = time.time()
    else:
        admin_interaction_updated_at.pop(chat_id, None)
    if state != previous_state or previous_state in PERSISTED_INTERACTION_STATES:
        _queue_interaction_state(chat_id, state)

def safe_get_temp_data(chat_id):
    with temp_data_lock:
        return temp_data.get(chat_id)

def safe_set_temp_data(chat_id, data):
    with temp_data_lock:
        if data is None:
            temp_data.pop(chat_id, None)
        else:
            temp_data[chat_id] = data
    state = safe_get_user_state(chat_id)
    if state in PERSISTED_INTERACTION_STATES:
        admin_interaction_updated_at[chat_id] = time.time()
        _queue_interaction_state(chat_id, state)

AUTO_CAPTCHA_SETUP_STATES = {
    "wait_for_cpanel_url",
    "wait_for_cpanel_user",
    "wait_for_cpanel_pass",
    "wait_for_cpanel_msg_link",
    "wait_for_cpanel_num_col_name",
    "wait_for_cpanel_num_col_idx",
    "wait_for_cpanel_msg_col_name",
    "wait_for_cpanel_msg_col_idx",
}

def cleanup_admin_interaction(
    chat_id,
    incoming_message_id,
    state,
    original_prompt_id=None,
):
    """Remove the answered prompt without deleting a new prompt or result."""
    current_prompt_id = admin_prompt_messages.get(chat_id)
    prompt_id = original_prompt_id or current_prompt_id
    if not prompt_id or prompt_id == incoming_message_id:
        return

    if current_prompt_id == prompt_id:
        if safe_get_user_state(chat_id) is not None:
            # The same Telegram message was advanced to the next prompt.
            return
        admin_prompt_messages.pop(chat_id, None)
        delete_message(chat_id, prompt_id)
        return

    if current_prompt_id is not None:
        # A handler created a replacement prompt; discard only the answered one.
        delete_message(chat_id, prompt_id)
    # If tracking was cleared by edit_message(), that prompt became the result
    # or admin screen and should remain visible.

def update_admin_prompt(chat_id, temp, text, reply_markup=None):
    """Advance a multi-step admin prompt in place and replace it safely on failure."""
    temp = temp or {}
    prompt_id = admin_prompt_messages.get(chat_id) or temp.get("msg_id")
    if prompt_id:
        result = edit_message(chat_id, prompt_id, text, reply_markup=reply_markup)
        if result and result.get("ok"):
            temp["msg_id"] = prompt_id
            safe_set_temp_data(chat_id, temp)
            return result
        # If the old message cannot be edited, remove it before sending a
        # replacement so a failed edit never leaves two setup prompts behind.
        delete_message(chat_id, prompt_id)
        if admin_prompt_messages.get(chat_id) == prompt_id:
            admin_prompt_messages.pop(chat_id, None)

    result = send_message(chat_id, text, reply_markup=reply_markup)
    new_prompt_id = result.get("result", {}).get("message_id") if result else None
    if new_prompt_id:
        temp["msg_id"] = new_prompt_id
        safe_set_temp_data(chat_id, temp)
        admin_prompt_messages[chat_id] = new_prompt_id
        if safe_get_user_state(chat_id) in ADMIN_INTERACTION_STATES:
            _queue_interaction_state(chat_id, safe_get_user_state(chat_id))
    return result

def number_loading_text(detail="Finding Number...", change_number=False):
    """Return the shared animated loading state for number operations."""
    if change_number:
        loading_icon = (
            f'<tg-emoji emoji-id="{NEW_NUMBER_REQUEST_EMOJI_ID}">🔄</tg-emoji>'
        )
        return render_body_text(f"{loading_icon} <b>New Number Request</b>")

    loading_icon = PEM["loading"]
    return render_body_text(
        f"{loading_icon} <b>Loading numbers...</b>\n"
        f"<i>Processing... {html.escape(str(detail))}</i>\n"
        f"{loading_icon} <i>Please wait</i>"
    )

def _keep_loading_visible(message_id):
    """Return immediately; synthetic display delays only slow number delivery."""
    return message_id

def _number_keyboard_without_copy_buttons(markup):
    """Build a compatible fallback by converting copy buttons to callbacks."""
    safe_markup = {"inline_keyboard": []}
    if not isinstance(markup, dict):
        return safe_markup
    for row in markup.get("inline_keyboard", []):
        safe_row = []
        for button in row:
            if not isinstance(button, dict):
                continue
            safe_button = {
                key: value for key, value in button.items() if key != "copy_text"
            }
            if "copy_text" in button:
                copy_text = button.get("copy_text")
                copy_value = (
                    copy_text.get("text", "")
                    if isinstance(copy_text, dict)
                    else ""
                )
                safe_button["callback_data"] = "cp_" + str(copy_value)
            if safe_button:
                safe_row.append(safe_button)
        if safe_row:
            safe_markup["inline_keyboard"].append(safe_row)
    return safe_markup

def deliver_number_menu(chat_id, message_id, text, markup):
    """Replace the loading state with a number menu, with a safe fallback."""
    if message_id:
        result = edit_message(chat_id, message_id, text, reply_markup=markup)
        if isinstance(result, dict) and result.get("ok"):
            return message_id

        description = (
            result.get("description", "unknown Telegram API error")
            if isinstance(result, dict)
            else "empty response"
        )
        logger.error(
            "Number menu edit failed for chat %s: %s",
            chat_id,
            description,
        )
        if "message to edit not found" not in str(description).lower():
            fallback = _number_keyboard_without_copy_buttons(markup)
            retry_result = edit_message(
                chat_id, message_id, text, reply_markup=fallback
            )
            if isinstance(retry_result, dict) and retry_result.get("ok"):
                return message_id

            # Do not leave a stale loading message behind before retrying.
            delete_message(chat_id, message_id)

    send_result = send_message(chat_id, text, reply_markup=markup)
    delivered_message_id = (
        send_result.get("result", {}).get("message_id")
        if isinstance(send_result, dict) and send_result.get("ok")
        else None
    )
    if not delivered_message_id:
        fallback = _number_keyboard_without_copy_buttons(markup)
        fallback_result = send_message(chat_id, text, reply_markup=fallback)
        delivered_message_id = (
            fallback_result.get("result", {}).get("message_id")
            if isinstance(fallback_result, dict) and fallback_result.get("ok")
            else None
        )

    if not delivered_message_id:
        logger.error("Could not deliver number menu to chat %s", chat_id)
        send_message(
            chat_id,
            render_body_text("❌ Could not display your numbers. Please press Change Number."),
        )
    return delivered_message_id

def show_number_loading(
    chat_id,
    message_id=None,
    detail="Finding Number...",
    allow_fallback_send=True,
    change_number=False,
    force_new=False,
):
    """Show loading immediately and return the message id used for the result."""
    loading_text = number_loading_text(detail, change_number=change_number)
    if force_new:
        if message_id:
            delete_message(chat_id, message_id)
        result = send_message(chat_id, loading_text)
        loading_message_id = (
            result.get("result", {}).get("message_id")
            if isinstance(result, dict)
            else None
        )
        if not loading_message_id:
            # Retry once so a transient Telegram/API failure cannot skip the
            # visible loading state entirely.
            time.sleep(0.2)
            result = send_message(chat_id, loading_text)
            loading_message_id = (
                result.get("result", {}).get("message_id")
                if isinstance(result, dict)
                else None
            )
        if loading_message_id:
            return _keep_loading_visible(loading_message_id)
        return _keep_loading_visible(None)

    if message_id:
        result = edit_message(chat_id, message_id, loading_text)
        if result and result.get("ok"):
            return _keep_loading_visible(message_id)
        if (
            result
            and "message is not modified"
            in str(result.get("description", "")).lower()
        ):
            return _keep_loading_visible(message_id)
        logger.error(
            "Number loading edit failed for chat %s: %s",
            chat_id,
            result.get("description", "unknown Telegram API error") if result else "empty response",
        )
        if not allow_fallback_send:
            # Keep the original message id so the final number menu can retry
            # editing it instead of opening a second message.
            return _keep_loading_visible(message_id)
    if not allow_fallback_send and message_id:
        return _keep_loading_visible(message_id)
    result = send_message(chat_id, loading_text)
    if isinstance(result, dict):
        return _keep_loading_visible(result.get("result", {}).get("message_id"))
    return _keep_loading_visible(None)

def queue_start_referral(user_id, inviter_id, is_new_local_user):
    """Save referral attribution (new users only) before Force Join runs.

    The attribution is a single fast local SQLite write, so it is done inline:
    this guarantees it is stored before the user can verify Force Join, with no
    background race.  Firestore mirroring happens in the background.
    """
    try:
        _attach_start_referral(inviter_id, user_id, is_new_local_user)
    except Exception:
        logger.exception("Could not save referral for user %s", user_id)

def finish_startup_tasks(user_id, process_referral=True):
    """Load/sync user data and settle referrals after the welcome is sent."""
    def run_tasks():
        try:
            # Pay first: it only needs SQLite and must not wait on Firestore.
            if process_referral:
                _process_pending_referral(user_id)
            get_user(user_id)
        except Exception:
            logger.exception("Background startup tasks failed for user %s", user_id)

    try:
        startup_executor.submit(run_tasks)
    except Exception:
        logger.exception("Could not queue startup tasks for user %s", user_id)

def _handle_message_impl(msg):
    global total_uploaded_stats, total_assigned_stats
    chat_id = msg["chat"]["id"]
    chat_type = msg["chat"].get("type", "private")

    if chat_type != "private":
        return

    # ── Rate limiter (advanced_features) — drop flooding users ───────────
    if not check_rate_limit(chat_id):
        return
    # ─────────────────────────────────────────────────────────────────────
        
    text = msg.get("text", "")
    if text and text.startswith("/start"):
        logger.info(
            "/start user=%s instance_id=%s",
            chat_id,
            INSTANCE_ID,
        )
    try:
        # Resolve Firestore before writing the /start row or deciding whether
        # this account is eligible for referral attribution.
        get_user(chat_id)
        is_new_user = register_user_local(chat_id)
    except Exception as e:
        logger.error("Error registering user: %s", e)
        is_new_user = False

    # ── analytics_addon: new user counter ────────────────────────────────
    if is_new_user:
        try:
            analytics_increment("new_users")
        except Exception:
            logger.exception("analytics new_user increment failed")
    # ─────────────────────────────────────────────────────────────────────

    if is_user_banned(chat_id):
        send_message(chat_id, render_body_text("🚫 <b>You are banned from using this bot!</b>\nIf you think this is a mistake, please contact support."))
        return
    
    # Queue referral setup before Force Join, but keep Firestore off the
    # update-handling thread so it cannot delay the greeting or other buttons.
    if text and text.startswith("/start"):
        parts = text.split()
        if len(parts) > 1:
            payload = parts[1]
            if payload.lower().startswith(("ref_", "ref-")):
                payload = payload[4:]
            if payload.isdigit():
                inviter = int(payload)
                if inviter != chat_id:
                    queue_start_referral(chat_id, inviter, is_new_user)
                        
    is_start_request = bool(text and text.startswith("/start"))
    if not check_force_join(
        chat_id,
        log_context="start" if is_start_request else "message",
    ):
        send_force_join_msg(chat_id)
        return
        
    # ── advanced_features: include all translated menu button texts ──────
    MAIN_MENU_CMDS = get_all_menu_texts() | {"/health", "/status"}
    # ─────────────────────────────────────────────────────────────────────
    
    is_main_cmd = False
    if text in MAIN_MENU_CMDS or (text and text.startswith("/start")):
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        is_main_cmd = True

    with expired_interaction_notifications_lock:
        interaction_expired = chat_id in expired_interaction_notifications
        expired_interaction_notifications.discard(chat_id)
    if interaction_expired and not is_main_cmd:
        send_message(
            chat_id,
            render_body_text(
                "⚠️ Your previous step expired after 30 minutes. "
                "Please start again from the menu."
            ),
            reply_markup=main_menu(chat_id),
        )
        return
    
    state = safe_get_user_state(chat_id)
    temp = safe_get_temp_data(chat_id)
    has_custom_emoji = any(
        entity.get("type") == "custom_emoji"
        for entity in msg.get("entities", [])
    )
    looks_like_upload_text = bool(
        text
        and (
            re.match(r"^\s*(?:FLAG|APP)\s*\|", text, flags=re.IGNORECASE)
            or text.lstrip().startswith("{")
            or has_custom_emoji
        )
    )

    if expire_admin_interaction_if_needed(chat_id, state):
        return
    upload_states = {"wait_for_txt", "wait_for_flag_txt", "wait_for_app_txt"}
    active_admin_states = ADMIN_INTERACTION_STATES | PENDING_NUMBER_UPLOAD_STATES
    if is_admin(chat_id) and state in active_admin_states:
        expects_document = state in upload_states
        if expects_document and "document" not in msg:
            hint = {
                "wait_for_txt": "Upload a UTF-8 .txt file with one phone number per line, such as <code>+8801712345678</code>.",
                "wait_for_flag_txt": "Upload a UTF-8 <code>.txt</code> file. Example: <code>🇧🇩 (880) (BD) Bangladesh {\"emoji\":\"🇧🇩\",\"id\":\"CUSTOM_EMOJI_ID\"}</code>.",
                "wait_for_app_txt": "Upload a UTF-8 <code>.txt</code> file. Example: <code>🚫 WhatsApp {\"emoji\":\"🚫\",\"id\":\"CUSTOM_EMOJI_ID\"}</code>.",
            }.get(state, "Upload a .txt document.")
            send_message(chat_id, render_body_text(f"⚠️ {hint}"), reply_markup=get_cancel_kb())
            return
        if not expects_document and ("document" in msg or not text):
            hints = {
                "wait_for_emoji_details": "Use <code>FLAG | 880 | BD | Bangladesh</code> or <code>APP | WhatsApp</code>.",
                "wait_for_cpanel_num_col_idx": "Send a positive column index, for example <code>3</code>.",
                "wait_for_cpanel_msg_col_idx": "Send a positive column index, for example <code>5</code>.",
                "wait_for_add_fw_btn": "Use <code>Button Text - https://example.com</code>.",
                "wait_for_oib_add": "Use <code>BOT - https://t.me/example</code>.",
                "wait_for_oib_edit": "Use <code>BOT - https://t.me/example</code>.",
                "wait_for_p_rec": "Send a record count, for example <code>10</code>; use <code>0</code> for unlimited.",
                "set_number_setting": "Send the number requested in the prompt.",
                "set_WAO": "Send a text value requested in the prompt.",
            }
            hint = hints.get(state, "Send a text reply for this setup step.")
            send_message(chat_id, render_body_text(f"⚠️ This step expects text, not a file. {hint}"), reply_markup=get_cancel_kb())
            return

    if (
        is_admin(chat_id)
        and "document" in msg
        and state not in {"wait_for_txt", "wait_for_flag_txt", "wait_for_app_txt", "wait_for_broadcast"}
        and state not in ADMIN_INTERACTION_STATES
    ):
        logger.warning(
            "Admin upload received without a compatible session chat=%s state=%s "
            "instance_id=%s",
            chat_id,
            state or "none",
            INSTANCE_ID,
        )
        send_message(
            chat_id,
            "⚠️ Upload session expired, please press the upload button again.",
        )
        return
    if (
        is_admin(chat_id)
        and not is_main_cmd
        and looks_like_upload_text
        and state is None
    ):
        logger.warning(
            "Admin upload text received without a compatible session chat=%s "
            "state=%s instance_id=%s",
            chat_id,
            state or "none",
            INSTANCE_ID,
        )
        send_message(
            chat_id,
            "⚠️ Upload session expired, please press the upload button again.",
        )
        return

    if is_admin(chat_id) and state in AUTO_CAPTCHA_SETUP_STATES and text and not text.strip():
        cpanel_hints = {
            "wait_for_cpanel_url": "login URL such as <code>https://panel.example.com/login</code>",
            "wait_for_cpanel_user": "panel username",
            "wait_for_cpanel_pass": "panel password",
            "wait_for_cpanel_msg_link": "data URL such as <code>https://panel.example.com/api/messages</code>",
            "wait_for_cpanel_num_col_name": "number column name such as <code>phone</code>",
            "wait_for_cpanel_num_col_idx": "positive column index such as <code>3</code>",
            "wait_for_cpanel_msg_col_name": "message column name such as <code>sms</code>",
            "wait_for_cpanel_msg_col_idx": "positive column index such as <code>5</code>",
        }
        send_message(chat_id, render_body_text(f"❌ This field cannot be empty. Send the {cpanel_hints.get(state, 'requested value')} or cancel."), reply_markup=get_cancel_kb())
        return

    if state and not is_main_cmd:
        # Auto Captcha Panel Setup Flow 
        if state == "wait_for_cpanel_url" and text:
            if not is_http_url(text.strip()):
                update_admin_prompt(chat_id, temp, render_body_text("❌ Invalid login URL. Send a full URL such as <code>https://panel.example.com/login</code>."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["p_data"] = temp.get("p_data", {})
            temp["p_data"]["login_url"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_cpanel_user")
            update_admin_prompt(chat_id, temp, render_body_text("2️⃣ <b>Username</b>\n➡️ Enter the panel username:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_user" and text:
            if not text.strip():
                update_admin_prompt(chat_id, temp, render_body_text("❌ Username cannot be empty. Please try again."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["p_data"] = temp.get("p_data", {})
            temp["p_data"]["username"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_cpanel_pass")
            update_admin_prompt(chat_id, temp, render_body_text("3️⃣ <b>Password</b>\n➡️ Enter the panel password:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_pass" and text:
            if not text.strip():
                update_admin_prompt(chat_id, temp, render_body_text("❌ Password cannot be empty. Please try again."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["p_data"] = temp.get("p_data", {})
            temp["p_data"]["password"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_cpanel_msg_link")
            update_admin_prompt(chat_id, temp, render_body_text("4️⃣ <b>Message Link</b>\n➡️ Enter the link that returns the SMS/OTP JSON data:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_msg_link" and text:
            if not is_http_url(text.strip()):
                update_admin_prompt(chat_id, temp, render_body_text("❌ Invalid data URL. Send a full URL such as <code>https://panel.example.com/api/messages</code>."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["p_data"] = temp.get("p_data", {})
            temp["p_data"]["msg_link"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_cpanel_num_col_name")
            update_admin_prompt(chat_id, temp, render_body_text("5️⃣ <b>Number Column Name</b>\n➡️ Enter the number column name (for example: number, phone):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_num_col_name" and text:
            if not text.strip():
                update_admin_prompt(chat_id, temp, render_body_text("❌ Column name cannot be empty. Send the number column name, such as <code>phone</code>."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["p_data"] = temp.get("p_data", {})
            temp["p_data"]["num_col_name"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_cpanel_num_col_idx")
            update_admin_prompt(chat_id, temp, render_body_text("6️⃣ <b>Number Column Index</b>\n➡️ Enter the number column index (for example: 3, 5):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_num_col_idx" and text:
            if text.isdigit() and int(text) > 0:
                temp = temp or {}
                temp["p_data"] = temp.get("p_data", {})
                temp["p_data"]["num_col_idx"] = int(text)
                safe_set_temp_data(chat_id, temp)
                safe_set_user_state(chat_id, "wait_for_cpanel_msg_col_name")
                update_admin_prompt(chat_id, temp, render_body_text("7️⃣ <b>Message Column Name</b>\n➡️ Enter the message/OTP column name (for example: message, sms):"), reply_markup=get_cancel_kb())
            else:
                update_admin_prompt(chat_id, temp, render_body_text("❌ Please enter a valid number serial!"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_msg_col_name" and text:
            if not text.strip():
                update_admin_prompt(chat_id, temp, render_body_text("❌ Column name cannot be empty. Send the message/OTP column name, such as <code>sms</code>."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["p_data"] = temp.get("p_data", {})
            temp["p_data"]["msg_col_name"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_cpanel_msg_col_idx")
            update_admin_prompt(chat_id, temp, render_body_text("8️⃣ <b>Message Column Index</b>\n➡️ Enter the message column index (for example: 5, 7):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_msg_col_idx" and text:
            if text.isdigit() and int(text) > 0:
                temp = temp or {}
                temp["p_data"] = temp.get("p_data", {})
                temp["p_data"]["msg_col_idx"] = int(text)
                temp["p_data"]["login_status"] = "⏳ Pending Auto-Login..."
                
                with data_lock:
                    bot_settings["panels"].append(temp["p_data"])
                    save_db()
                
                send_message(chat_id, render_body_text(f"{PEM['ok']} <b>Auto Captcha Panel Added Successfully!</b>\nThe bot will solve the captcha and log in automatically in the background."), reply_markup=main_menu(chat_id))
                
                msg_id = temp.get("msg_id")
                if msg_id:
                    handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_cpt_panels", "id": "internal"})
                
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            else:
                update_admin_prompt(chat_id, temp, render_body_text("❌ Please enter a valid number serial!"), reply_markup=get_cancel_kb())
            return

        # User Management Flows
        elif state == "wait_for_um_bal_uid" and text:
            target_uid_str = text.strip()
            if not target_uid_str.isdigit():
                send_message(chat_id, render_body_text("❌ Invalid ID! Please send a numeric User ID."), reply_markup=get_cancel_kb())
                return
            target_uid = int(target_uid_str)
            try:
                if db:
                    user_doc = db.collection("users").document(str(target_uid)).get(timeout=3)
                    if not user_doc.exists:
                        send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                        return
                    current_bal = (user_doc.to_dict() or {}).get("balance", 0.0)
                else:
                    conn = get_db()
                    try:
                        row = conn.execute(
                            'SELECT balance FROM users WHERE user_id = ?',
                            (str(target_uid),),
                        ).fetchone()
                        if not row:
                            send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                            return
                        current_bal = row["balance"]
                    finally:
                        conn.close()
                temp = temp or {}
                temp["target_uid"] = target_uid
                safe_set_temp_data(chat_id, temp)
                safe_set_user_state(chat_id, "wait_for_um_bal_amt")
                send_message(chat_id, render_body_text(f"✅ User found!\n💰 Current Balance: {fmt_money(current_bal)} ৳\n\n📝 Send the amount to ADD (e.g. 50) or REMOVE (e.g. -50):"), reply_markup=get_cancel_kb())
            except Exception as e:
                logger.error(f"Error fetching user: {e}")
                send_message(chat_id, render_body_text("❌ Error fetching user data!"))
            return

        elif state == "wait_for_um_bal_amt" and text:
            try:
                amt = float(text.strip())
                if not math.isfinite(amt):
                    raise ValueError("Amount must be a finite number.")
                target_uid = temp.get("target_uid") if temp else None
                if not target_uid:
                    send_message(chat_id, render_body_text("❌ Session expired! Please try again."))
                    safe_set_user_state(chat_id, None)
                    safe_set_temp_data(chat_id, None)
                    return
                update_balance(target_uid, amt)
                send_message(chat_id, render_body_text(f"{PEM['ok']} Balance updated successfully for {target_uid}!"), reply_markup=main_menu(chat_id))
                send_message(target_uid, render_body_text(f"🔔 Your balance has been adjusted by <b>{fmt_money(amt)} ৳</b> by an Admin."))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            except ValueError:
                send_message(chat_id, render_body_text("❌ Invalid amount! Please send a number."), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_um_ban_uid" and text:
            target_uid_str = text.strip()
            if not target_uid_str.isdigit():
                send_message(chat_id, render_body_text("❌ Invalid ID!"), reply_markup=get_cancel_kb())
                return
            target_uid = int(target_uid_str)
            try:
                if db:
                    user_ref = db.collection("users").document(str(target_uid))
                    user_doc = user_ref.get(timeout=3)
                    if not user_doc.exists:
                        send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                        return
                    current_status = bool((user_doc.to_dict() or {}).get("banned", False))
                    new_status = not current_status
                    user_ref.update({"banned": new_status}, timeout=3)
                    local_user = get_user(target_uid)
                    local_user["banned"] = new_status
                    conn = get_db()
                    try:
                        conn.execute(
                            'UPDATE users SET banned = ? WHERE user_id = ?',
                            (int(new_status), str(target_uid)),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                else:
                    conn = get_db()
                    try:
                        row = conn.execute(
                            'SELECT banned FROM users WHERE user_id = ?',
                            (str(target_uid),),
                        ).fetchone()
                        if not row:
                            send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                            return
                        current_status = bool(row['banned'])
                        new_status = not current_status
                        conn.execute(
                            'UPDATE users SET banned = ? WHERE user_id = ?',
                            (int(new_status), str(target_uid)),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                
                user_banned_cache[target_uid] = {'banned': bool(new_status), 'time': time.time()}
                
                status_text = "BANNED 🚫" if new_status else "UNBANNED ✅"
                send_message(chat_id, render_body_text(f"✅ User {target_uid} has been {status_text}!"), reply_markup=main_menu(chat_id))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            except Exception as e:
                logger.error(f"Error updating ban status: {e}")
                send_message(chat_id, render_body_text("❌ Error updating user!"))
            return

        elif state == "wait_for_um_prof_uid" and text:
            target_uid_str = text.strip()
            if not target_uid_str.isdigit():
                send_message(chat_id, render_body_text("❌ Invalid ID!"), reply_markup=get_cancel_kb())
                return
            target_uid = int(target_uid_str)
            try:
                if db:
                    user_doc = db.collection("users").document(str(target_uid)).get(timeout=3)
                    if not user_doc.exists:
                        send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                        return
                    data = user_doc.to_dict() or {}
                else:
                    conn = get_db()
                    try:
                        row = conn.execute(
                            'SELECT * FROM users WHERE user_id = ?',
                            (str(target_uid),),
                        ).fetchone()
                        if not row:
                            send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                            return
                        data = dict(row)
                    finally:
                        conn.close()
                is_verified = True if data.get('total_otps', 0) > 0 else bool(data.get('verified', 0))
                prof_text = (
                    f"{ANIMATED_BANNER}\n"
                    f"👤 <b>USER PROFILE</b>\n"
                    f"{ANIMATED_BANNER}\n"
                    f"🆔 ID: <code>{target_uid}</code>\n"
                    f"💰 Balance: {fmt_money(data.get('balance', 0.0))} ৳\n"
                    f"🤝 Total Refers: {data.get('total_refers', 0)}\n"
                    f"🔐 Total OTPs: {data.get('total_otps', 0)}\n"
                    f"✅ Verified: {is_verified}\n"
                    f"🚫 Banned: {bool(data.get('banned', 0))}\n"
                    f"{ANIMATED_BANNER}"
                )
                kb = {"inline_keyboard": [[{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "user_management", "style": "primary"}]]}
                send_message(chat_id, render_body_text(prof_text), reply_markup=kb)
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            except Exception as e:
                logger.error(f"Error fetching user profile: {e}")
                send_message(chat_id, render_body_text("❌ Error fetching user data!"))
            return

        # Menu Design Flow
        elif state == "wait_for_menu_text" and text:
            menu_key = temp.get("menu_key") if temp else None
            if not menu_key or menu_key not in bot_settings.get("custom_messages", {}):
                send_message(chat_id, render_body_text("❌ Menu session expired. Reopen the menu editor and try again."), reply_markup=get_cancel_kb())
                return
            try:
                formatted_html_text = extract_premium_html(msg)
                with data_lock:
                    bot_settings["custom_messages"][menu_key]["text"] = formatted_html_text
                    save_db()
                delete_message(chat_id, msg["message_id"])
                preview_text = render_body_text(formatted_html_text)
                success_text = f"{PEM['ok']} <b>Message Body Updated successfully!</b>\n\n🎨 <b>Editing: {menu_key.upper()}</b>\n\nPreview of current Text:\n{preview_text}"
                msg_id = temp.get("msg_id")
                if msg_id:
                    edit_message(chat_id, msg_id, render_body_text(success_text), reply_markup=menu_edit_options_keyboard(menu_key))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            except Exception:
                logger.exception("Error saving menu text for admin %s", chat_id)
                send_message(chat_id, render_body_text("❌ Could not save this menu text. Please retry, or cancel."), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_menu_btn" and text:
            menu_key = temp.get("menu_key") if temp else None
            if not menu_key or menu_key not in bot_settings.get("custom_messages", {}):
                send_message(chat_id, render_body_text("❌ Menu session expired. Reopen the menu editor and try again."), reply_markup=get_cancel_kb())
                return
            parts = text.split("-", 1)
            if len(parts) != 2 or not parts[0].strip() or not is_http_url(parts[1].strip()):
                send_message(chat_id, render_body_text("❌ Invalid format. Use <code>Button Text - https://example.com</code>."), reply_markup=get_cancel_kb())
                return
            try:
                btn_text, btn_url = parts[0].strip(), parts[1].strip()
                emoji_id = None
                emoji_char = ""
                for ent in msg.get("entities", []):
                    if ent.get("type") == "custom_emoji":
                        emoji_id = ent.get("custom_emoji_id")
                        offset = ent.get("offset", 0)
                        length = ent.get("length", 0)
                        b_text = text.encode("utf-16-le")
                        emoji_char = b_text[offset * 2:(offset + length) * 2].decode("utf-16-le")
                        break
                if emoji_char:
                    btn_text = btn_text.replace(emoji_char, "").strip()
                if not btn_text:
                    send_message(chat_id, render_body_text("❌ Button text is required. Use <code>Button Text - https://example.com</code>."), reply_markup=get_cancel_kb())
                    return
                btn_data = {"text": btn_text, "url": btn_url, "style": "primary"}
                if emoji_id:
                    btn_data["icon_custom_emoji_id"] = emoji_id
                with data_lock:
                    bot_settings["custom_messages"][menu_key]["buttons"].append(btn_data)
                    save_db()
                delete_message(chat_id, msg["message_id"])
                msg_id = temp.get("msg_id")
                if msg_id:
                    edit_message(chat_id, msg_id, render_body_text(f"{PEM['gear']} <b>Edit Inline Buttons: {menu_key.upper()}</b>"), reply_markup=menu_buttons_list_keyboard(menu_key))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            except Exception:
                logger.exception("Error adding menu button for admin %s", chat_id)
                send_message(chat_id, render_body_text("❌ Could not add this button. Please retry, or cancel."), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_test_service" and text:
            service_name = text.strip()
            if not service_name or len(service_name) > 50:
                send_message(chat_id, render_body_text("❌ Enter a service name from 1–50 characters, for example <code>WHATSAPP</code>."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["service"] = service_name
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_test_number")
            send_message(chat_id, render_body_text("📝 Send the Number (e.g. +8801712345678):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_test_number" and text:
            normalized_number = normalize_phone_number(text)
            if not normalized_number:
                send_message(chat_id, render_body_text("❌ Invalid phone number. Example: <code>+8801712345678</code>. Please try again."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["number"] = normalized_number
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_test_otp")
            send_message(chat_id, render_body_text("📝 Send the OTP (e.g. 556677):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_test_otp" and text:
            if not text.strip():
                send_message(chat_id, render_body_text("❌ OTP cannot be empty. Send the OTP, for example <code>556677</code>."), reply_markup=get_cancel_kb())
                return
            temp = temp or {}
            temp["otp"] = text.strip()
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_test_lang")
            send_message(chat_id, render_body_text("📝 Send the Language (e.g. EN, AR):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_test_lang" and text:
            raw_lang = text.strip().lstrip("#").upper()
            if not re.fullmatch(r"[A-Z]{2,5}", raw_lang):
                send_message(chat_id, render_body_text("❌ Invalid language code. Send 2–5 letters, for example <code>EN</code> or <code>AR</code>."), reply_markup=get_cancel_kb())
                return
            lang = "#" + raw_lang
                
            srv = temp.get("service") if temp else "Unknown"
            num = temp.get("number") if temp else "0000000000"
            otp = temp.get("otp") if temp else "000000"
            
            masked = mask_number(num)
            prem_flag_html = get_flag_info_html(num)
            char, iso = get_flag_and_code(num)
            app_full_name, prem_app_html = get_service_info_html(srv)
            
            lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
            msg_text = render_body_text(f"{prem_flag_html} {iso} | {prem_app_html} {masked} | 💬 {lang_name}")
            
            for fw in bot_settings["fw_groups"]:
                send_otp_message(fw["chat_id"], msg_text, build_group_otp_keyboard(fw, otp))
                
            send_message(chat_id, render_body_text(f"{PEM['ok']} Test message formatted and sent to all Forward Groups!"), reply_markup=main_menu(chat_id))
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_emoji_extract":
            entities = msg.get("entities", [])
            custom_emoji_id = None
            emoji_text = ""
            for ent in entities:
                if ent.get("type") == "custom_emoji":
                    custom_emoji_id = ent.get("custom_emoji_id")
                    offset = ent.get("offset", 0)
                    length = ent.get("length", 0)
                    b_text = msg.get("text", "").encode('utf-16-le')
                    emoji_text = b_text[offset*2:(offset+length)*2].decode('utf-16-le')
                    break
            
            if custom_emoji_id:
                temp = temp or {}
                temp["id"] = custom_emoji_id
                temp["char"] = emoji_text
                safe_set_temp_data(chat_id, temp)
                safe_set_user_state(chat_id, "wait_for_emoji_details")
                send_message(chat_id, render_body_text(f"{PEM['ok']} Emoji ID detected: <code>{custom_emoji_id}</code>\n\n📌 Enter its type and name to save it.\n\n<b>Format:</b>\n<code>FLAG | 880 | BD | Bangladesh</code>\n or\n<code>APP | WhatsApp</code>"), reply_markup=get_cancel_kb())
            else:
                send_message(chat_id, render_body_text(f"{PEM['no']} No premium emoji was detected. Please send a custom emoji."), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_emoji_details" and text:
            replacement_entity = next(
                (entity for entity in msg.get("entities", []) if entity.get("type") == "custom_emoji"),
                None,
            )
            if replacement_entity:
                offset = int(replacement_entity.get("offset", 0))
                length = int(replacement_entity.get("length", 0))
                encoded_text = text.encode("utf-16-le")
                replacement_char = encoded_text[offset * 2:(offset + length) * 2].decode("utf-16-le")
                temp = temp or {}
                temp["id"] = replacement_entity.get("custom_emoji_id")
                temp["char"] = replacement_char
                safe_set_temp_data(chat_id, temp)
                send_message(chat_id, render_body_text("✅ New custom emoji selected. Now send FLAG | 880 | BD | Bangladesh or APP | WhatsApp."), reply_markup=get_cancel_kb())
                return
            parts = [part.strip() for part in text.split("|")]
            eid = temp.get("id") if temp else None
            char = temp.get("char") if temp else ""
            format_help = (
                "Use <code>FLAG | 880 | BD | Bangladesh</code> or "
                "<code>APP | WhatsApp</code>."
            )
            if not eid:
                send_message(chat_id, render_body_text("❌ Emoji session data is missing. Start again."), reply_markup=get_cancel_kb())
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return

            kind = parts[0].upper() if parts else ""
            if kind == "FLAG":
                if (
                    len(parts) != 4
                    or not parts[1].isdigit()
                    or not re.fullmatch(r"[A-Za-z]{2}", parts[2])
                    or not parts[3]
                ):
                    send_message(chat_id, render_body_text(f"❌ Invalid flag format. {format_help}"), reply_markup=get_cancel_kb())
                    return
                code, iso, name = parts[1], parts[2].upper(), parts[3]
                if premium_emoji_id_exists(eid, ignore_flag_code=iso):
                    send_message(chat_id, render_body_text(f"⚠️ This premium emoji ID is already assigned to another flag. Send a different custom emoji now, then repeat the format.\n\n{format_help}"), reply_markup=get_cancel_kb())
                    return
                with data_lock:
                    bot_settings["premium_flags"][iso] = {
                        "char": char, "code": code, "iso": iso,
                        "name": name, "id": str(eid),
                    }
                    save_db()
                send_message(chat_id, render_body_text(f"✅ Flag emoji saved successfully.\nCountry: {html.escape(str(name))} ({html.escape(str(iso))}) | Dial code: +{html.escape(str(code))}"), reply_markup=emoji_settings_keyboard())
            elif kind == "APP":
                if len(parts) != 2 or not parts[1]:
                    send_message(chat_id, render_body_text(f"❌ Invalid app format. {format_help}"), reply_markup=get_cancel_kb())
                    return
                name = parts[1]
                if premium_emoji_id_exists(eid, ignore_app_name=name):
                    send_message(chat_id, render_body_text(f"⚠️ This premium emoji ID is already assigned to another item. Send a different custom emoji now, then repeat the format.\n\n{format_help}"), reply_markup=get_cancel_kb())
                    return
                with data_lock:
                    bot_settings["premium_apps"][name.upper()] = {"char": char, "id": str(eid), "name": name.title()}
                    save_db()
                send_message(chat_id, render_body_text(f"✅ Service emoji saved successfully.\nName: {html.escape(str(name))}"), reply_markup=emoji_settings_keyboard())
            else:
                send_message(chat_id, render_body_text(f"❌ Invalid format. {format_help}"), reply_markup=get_cancel_kb())
                return
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state in ["wait_for_flag_txt", "wait_for_app_txt"] and "document" in msg:
            process_emoji_text_upload(chat_id, state, temp, msg["document"])
            return

        elif state in {"wait_for_flag_txt", "wait_for_app_txt"}:
            expected = "Flag_Emojis.txt" if state == "wait_for_flag_txt" else "Service_Apps.txt"
            send_message(
                chat_id,
                render_body_text(f"⚠️ This step expects a <b>.txt document</b> (for example, <code>{expected}</code>). Please attach the file; your setup is still open."),
                reply_markup=get_cancel_kb(),
            )
            return

        elif state == "wait_for_broadcast":
            msg_id = msg["message_id"]
            send_message(chat_id, render_body_text(f"{PEM['ok']} Broadcast started..."))
            threading.Thread(
                target=broadcast_copymessage,
                args=(chat_id, msg_id),
                name="message-broadcast",
                daemon=True,
            ).start()
            safe_set_user_state(chat_id, None)
            return

        elif state == "adv_wait_for_bcast":
            # ── advanced_features: targeted broadcast ─────────────────────
            safe_set_user_state(chat_id, None)
            process_targeted_broadcast_message(chat_id, msg["message_id"])
            return

        elif state == "wait_for_txt" and "document" in msg:
            doc = msg["document"]
            prompt_id = (temp or {}).get("msg_id")
            if not doc.get("file_name", "").lower().endswith(".txt"):
                prompt_text = render_body_text(
                    f"{PEM['no']} Please upload a <b>.txt</b> file only."
                )
                if prompt_id:
                    edit_message(chat_id, prompt_id, prompt_text, reply_markup=get_cancel_kb())
                else:
                    send_message(chat_id, prompt_text, reply_markup=get_cancel_kb())
                return
            try:
                file_size = int(doc.get("file_size", 0) or 0)
            except (TypeError, ValueError):
                file_size = 0
            if file_size > MAX_NUMBER_UPLOAD_BYTES:
                prompt_text = render_body_text(
                    f"{PEM['no']} File is too large. Please upload a TXT file under 10 MB."
                )
                if prompt_id:
                    edit_message(chat_id, prompt_id, prompt_text, reply_markup=get_cancel_kb())
                else:
                    send_message(chat_id, prompt_text, reply_markup=get_cancel_kb())
                return

            if prompt_id:
                edit_message(
                    chat_id,
                    prompt_id,
                    "⏳ Processing number file…",
                    reply_markup=get_cancel_kb(),
                )
            else:
                send_message(chat_id, "⏳ Processing number file…")

            try:
                file_content = download_telegram_file(
                    doc["file_id"],
                    max_bytes=MAX_NUMBER_UPLOAD_BYTES,
                ).decode("utf-8-sig")
                file_numbers, invalid_lines, duplicate_count = parse_number_upload(file_content)
                if invalid_lines:
                    shown_lines = ", ".join(str(line) for line in invalid_lines[:10])
                    if len(invalid_lines) > 10:
                        shown_lines += ", …"
                    error_text = render_body_text(
                        f"{PEM['no']} Invalid number format on line(s): {shown_lines}.\n"
                        "Use one phone number per line (for example, +8801712345678)."
                    )
                    if prompt_id:
                        edit_message(chat_id, prompt_id, error_text, reply_markup=get_cancel_kb())
                    else:
                        send_message(chat_id, error_text, reply_markup=get_cancel_kb())
                    return

                unique_numbers = []
                for number in file_numbers:
                    if number in known_numbers:
                        duplicate_count += 1
                    else:
                        unique_numbers.append(number)
                if not unique_numbers:
                    error_text = render_body_text(
                        f"{PEM['no']} No new valid numbers were found. The file is empty or all numbers are duplicates."
                    )
                    if prompt_id:
                        edit_message(chat_id, prompt_id, error_text, reply_markup=get_cancel_kb())
                    else:
                        send_message(chat_id, error_text, reply_markup=get_cancel_kb())
                    return

                temp = temp or {}
                temp["numbers"] = unique_numbers
                temp["filename"] = str(doc.get("file_name") or "numbers.txt")[:255]
                temp["upload_duplicates_skipped"] = duplicate_count
                safe_set_temp_data(chat_id, temp)
                safe_set_user_state(chat_id, "wait_for_service")
                next_text = render_body_text(
                    f"{PEM['ok']} File received: <b>{len(unique_numbers)}</b> valid unique numbers"
                    + (f" ({duplicate_count} duplicates skipped)" if duplicate_count else "")
                    + ".\n\n"
                    "📌 Enter the service name (e.g., WHATSAPP):"
                )
                if prompt_id:
                    edit_message(chat_id, prompt_id, next_text, reply_markup=get_cancel_kb())
                else:
                    send_message(chat_id, next_text, reply_markup=get_cancel_kb())
            except Exception as e:
                logger.exception("Error processing uploaded number file")
                error_text = render_body_text(
                    f"❌ Could not process this TXT file: {html.escape(str(e))}"
                )
                if prompt_id:
                    edit_message(chat_id, prompt_id, error_text, reply_markup=get_cancel_kb())
                else:
                    send_message(chat_id, error_text, reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_txt":
            prompt_id = (temp or {}).get("msg_id")
            prompt_text = render_body_text(
                f"{PEM['warn']} Please upload the number list as a <b>.txt</b> document, one phone number per line."
            )
            if prompt_id:
                edit_message(chat_id, prompt_id, prompt_text, reply_markup=get_cancel_kb())
            else:
                send_message(chat_id, prompt_text, reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_service" and text:
            temp = temp or {}
            service_text = text.strip()
            service = normalize_service_name(service_text)
            if not service or len(service) > 50:
                send_message(
                    chat_id,
                    render_body_text(f"{PEM['no']} Enter a service name up to 50 characters."),
                    reply_markup=get_cancel_kb(),
                )
                return
            temp["service"] = service
            enable_service(temp["service"])
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "wait_for_country")
            next_text = render_body_text(
                f"{PEM['ok']} Service set.\n\n"
                "🌍 Enter the country name (e.g., YEMEN):"
            )
            prompt_id = temp.get("msg_id")
            if prompt_id:
                edit_message(chat_id, prompt_id, next_text, reply_markup=get_cancel_kb())
            else:
                send_message(chat_id, next_text, reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_country" and text:
            country = text.strip().upper()
            service = temp.get("service") if temp else "UNKNOWN"
            raw_numbers = temp.get("numbers") if temp else []

            if not country or len(country) > 64:
                send_message(
                    chat_id,
                    render_body_text(f"{PEM['no']} Enter a country name up to 64 characters."),
                    reply_markup=get_cancel_kb(),
                )
                return

            clean_nums = []
            seen_numbers = set()
            for num in raw_numbers:
                normalized = normalize_phone_number(num)
                if normalized and normalized not in seen_numbers:
                    seen_numbers.add(normalized)
                    clean_nums.append(normalized)
            if not clean_nums:
                prompt_id = (temp or {}).get("msg_id")
                safe_set_temp_data(
                    chat_id,
                    {"msg_id": prompt_id, "upload_id": (temp or {}).get("upload_id")},
                )
                safe_set_user_state(chat_id, "wait_for_txt")
                retry_text = render_body_text(
                    f"{PEM['no']} This upload has no valid numbers. Please send a corrected TXT file."
                )
                if prompt_id:
                    edit_message(chat_id, prompt_id, retry_text, reply_markup=get_cancel_kb())
                else:
                    send_message(chat_id, retry_text, reply_markup=get_cancel_kb())
                return

            batch_id = str((temp or {}).get("upload_id") or uuid.uuid4().hex)
            prepared_numbers = [
                {"num": n, "shares": 0, "used_by": []}
                for n in clean_nums
            ]
            with data_lock:
                if batch_id in number_batches:
                    safe_set_user_state(chat_id, None)
                    safe_set_temp_data(chat_id, None)
                    return
                number_batches[batch_id] = {
                    "filename": temp.get("filename") if temp else "unknown.txt",
                    "service": service,
                    "country": country,
                    "numbers": prepared_numbers
                }
                known_numbers.update(clean_nums)
                dirty_number_batches.add(batch_id)
                total_uploaded_stats += len(clean_nums)
                save_db(include_local=True, clear_pending_user=chat_id)
            invalidate_inventory_cache()
            
            app_full_name, prem_app_html = get_service_info_html(service)
            prem_flag_html = get_flag_info_html(clean_nums[0]) if clean_nums else f"{PEM['world']} "
            safe_country = html.escape(country)
            safe_service = html.escape(service)
            
            broadcast_txt = f"📤 <b>NEW NUMBERS AVAILABLE</b>\n{ANIMATED_BANNER}\n{prem_flag_html} {safe_country} {prem_app_html} {safe_service}\n\n📦 Total Added: <b>{len(clean_nums)}</b>\n{ANIMATED_BANNER}\nUse /start to get your numbers!"
            broadcast_txt = render_body_text(broadcast_txt)
            
            broadcast_status = render_body_text(
                f"{PEM['ok']} ✅ {len(clean_nums)} numbers added to local stock. "
                "Starting broadcast..."
                + (
                    f" ({temp.get('upload_duplicates_skipped', 0)} duplicates skipped.)"
                    if temp and temp.get("upload_duplicates_skipped")
                    else ""
                )
            )
            prompt_id = temp.get("msg_id") if temp else None
            if prompt_id:
                edit_message(
                    chat_id,
                    prompt_id,
                    broadcast_status,
                )
            else:
                send_message(chat_id, broadcast_status)
            
            threading.Thread(
                target=broadcast_text_message,
                args=(chat_id, broadcast_txt),
                name="number-stock-broadcast",
                daemon=True,
            ).start()
            
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_service":
            send_message(
                chat_id,
                render_body_text(f"{PEM['warn']} Send the service name as text."),
                reply_markup=get_cancel_kb(),
            )
            return

        elif state == "wait_for_country":
            send_message(
                chat_id,
                render_body_text(f"{PEM['warn']} Send the country name as text."),
                reply_markup=get_cancel_kb(),
            )
            return

        elif state == "wait_for_add_wm" and text:
            method = text.strip()
            if not method or len(method) > 60:
                send_message(chat_id, render_body_text("❌ Enter a withdrawal method name (1–60 characters), for example <code>bKash</code>."), reply_markup=get_cancel_kb())
                return
            with data_lock:
                if method.casefold() not in {str(existing).casefold() for existing in bot_settings["w_methods"]}:
                    bot_settings["w_methods"].append(method)
                save_db()
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            if msg_id:
                edit_message(chat_id, msg_id, render_body_text("💳 <b>WITHDRAWAL METHODS</b>\n\nManage your withdrawal methods below:"), reply_markup=w_methods_keyboard())
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_fj_invite" and text:
            invite_link = text.strip()
            prompt_id = temp.get("msg_id") if temp else None
            channel_index = temp.get("channel_index") if temp else None
            if not _valid_force_join_invite_url(invite_link):
                delete_message(chat_id, msg["message_id"])
                if prompt_id:
                    edit_message(
                        chat_id,
                        prompt_id,
                        render_body_text(
                            "❌ Invalid invite link. Send an HTTPS Telegram invite "
                            "link such as <code>https://t.me/+…</code>."
                        ),
                        reply_markup={"inline_keyboard": [[{
                            "text": "Cancel",
                            "icon_custom_emoji_id": "5267490665117275176",
                            "callback_data": "manage_fj",
                            "style": "danger",
                        }]]},
                    )
                return

            channel_title = None
            channel_id = None
            saved = False
            with data_lock:
                channels = bot_settings.get("fj_channels", [])
                if (
                    isinstance(channel_index, int)
                    and 0 <= channel_index < len(channels)
                ):
                    channel = normalize_force_join_channel(
                        channels[channel_index]
                    )
                    channel["invite_link"] = invite_link
                    channel_id = channel.get("id")
                    channel_title = channel.get("title")
                    channels[channel_index] = channel
                    save_db()
                    saved = True
            delete_message(chat_id, msg["message_id"])
            if saved:
                if channel_id:
                    with force_join_invite_creation_lock:
                        force_join_invite_creation_cache.pop(
                            str(channel_id),
                            None,
                        )
                invalidate_force_join_cache()
                if prompt_id:
                    result_text = (
                        "✅ Invite link saved for "
                        f"<b>{html.escape(str(channel_title or channel_id))}</b>."
                    )
                    edit_message(
                        chat_id,
                        prompt_id,
                        fj_settings_text(result_text),
                        reply_markup=fj_settings_keyboard(),
                    )
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            elif prompt_id:
                edit_message(
                    chat_id,
                    prompt_id,
                    fj_settings_text("❌ This channel no longer exists."),
                    reply_markup=fj_settings_keyboard(),
                )
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_add_fj" and text:
            success, result, auto_enabled = add_force_join_channel_from_input(text)
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            if msg_id:
                if success:
                    result_text = (
                        f"✅ Channel added: <b>{html.escape(str(result))}</b>"
                    )
                    if auto_enabled:
                        result_text += "\nForce Join has been turned ON automatically."
                    edit_message(
                        chat_id,
                        msg_id,
                        fj_settings_text(result_text),
                        reply_markup=fj_settings_keyboard(),
                    )
                else:
                    prompt_text = (
                        f"{PEM['link']} <b>FORCE JOIN SYSTEM</b>\n"
                        f"{result}\n\n"
                        "Send @username or a public channel link. For a private "
                        "channel, send <code>-100… | https://t.me/+…</code>."
                    )
                    edit_message(
                        chat_id,
                        msg_id,
                        render_body_text(prompt_text),
                        reply_markup={
                            "inline_keyboard": [[{
                                "text": "Cancel",
                                "icon_custom_emoji_id": "5267490665117275176",
                                "callback_data": "manage_fj",
                                "style": "danger",
                            }]]
                        },
                    )
            if success:
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            return
            
        elif state == "wait_for_add_adm" and text:
            if not text.strip().isdigit():
                send_message(chat_id, render_body_text("❌ Invalid Telegram user ID. Send a numeric ID, for example <code>123456789</code>."), reply_markup=get_cancel_kb())
                return
            admin_id = int(text.strip())
            with data_lock:
                if admin_id not in bot_settings["admins"]:
                    bot_settings["admins"].append(admin_id)
                    save_db()
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            if msg_id:
                edit_message(chat_id, msg_id, render_body_text("👥 <b>ADMIN MANAGEMENT</b>\nManage your bot admins below:"), reply_markup=admin_settings_keyboard())
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_add_fw_id" and text:
            target = text.strip()
            if not (re.fullmatch(r"-?\d{5,20}", target) or re.fullmatch(r"@?[A-Za-z][A-Za-z0-9_]{4,31}", target)):
                send_message(chat_id, render_body_text("❌ Invalid group ID/username. Send a numeric chat ID such as <code>-1001234567890</code> or a username such as <code>@my_channel</code>."), reply_markup=get_cancel_kb())
                return
            if target.startswith("@"):
                target = target[1:]
            with data_lock:
                if target not in {str(group.get("chat_id", "")) for group in bot_settings["fw_groups"]}:
                    bot_settings["fw_groups"].append({"chat_id": target, "buttons": []})
                save_db()
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            if msg_id:
                edit_message(chat_id, msg_id, render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return
            
        elif state == "wait_for_add_fw_btn" and text:
            fw_idx = temp.get("fw_idx") if temp else None
            if fw_idx is None:
                send_message(chat_id, render_body_text("❌ Session expired!"))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
            if "-" in text:
                parts = text.split("-", 1)
                btn_text = parts[0].strip()
                btn_url = parts[1].strip()
                if not btn_text or not is_http_url(btn_url):
                    send_message(chat_id, render_body_text("❌ Invalid button. Use <code>Button Text - https://example.com</code>."), reply_markup=get_cancel_kb())
                    return
                
                emoji_id = None
                emoji_char = ""
                for ent in msg.get("entities", []):
                    if ent.get("type") == "custom_emoji":
                        emoji_id = ent.get("custom_emoji_id")
                        offset = ent.get("offset", 0)
                        length = ent.get("length", 0)
                        b_text = text.encode('utf-16-le')
                        emoji_char = b_text[offset*2:(offset+length)*2].decode('utf-16-le')
                        break
                
                if emoji_char:
                    btn_text = btn_text.replace(emoji_char, "").strip()
                    
                btn_data = {"text": btn_text, "url": btn_url}
                if emoji_id:
                    btn_data["icon_custom_emoji_id"] = emoji_id
                    
                with data_lock:
                    bot_settings["fw_groups"][fw_idx]["buttons"].append(btn_data)
                    save_db()
            else:
                send_message(chat_id, render_body_text("❌ Invalid format. Use <code>Button Text - https://example.com</code>."), reply_markup=get_cancel_kb())
                return
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            if msg_id:
                edit_message(chat_id, msg_id, render_body_text(f"🛡 <b>Manage Group:</b> {bot_settings['fw_groups'][fw_idx]['chat_id']}"), reply_markup=specific_fw_group_keyboard(fw_idx))
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return
            
        elif state in ("wait_for_oib_add", "wait_for_oib_edit") and text:
            handle_oib_text_state(chat_id, state, text, msg, temp)
            return

        elif state == "wait_for_otp_link" and text:
            if not is_http_url(text.strip()):
                send_message(chat_id, render_body_text("❌ Invalid OTP Group URL. Send a full link such as <code>https://t.me/my_otp_group</code>."), reply_markup=get_cancel_kb())
                return
            with data_lock:
                bot_settings["otp_link"] = text.strip()
                save_db()
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            if msg_id:
                edit_message(chat_id, msg_id, render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_panel_name" and text:
            p_name = text.strip()
            if not p_name or len(p_name) > 80:
                send_message(chat_id, render_body_text("❌ Provider name must be 1–80 characters. Example: <code>My OTP Panel</code>."), reply_markup=get_cancel_kb())
                return
            t_key = temp.get("add_type") if temp else "api"
            msg_id = temp.get("msg_id") if temp else None
            delete_message(chat_id, msg["message_id"])
            
            if t_key == "logc":
                safe_set_user_state(chat_id, "wait_for_cpanel_url")
                temp = temp or {}
                temp["msg_id"] = msg_id
                temp["p_data"] = {
                    "name": p_name, "type": "Auto Captcha Panel", "status": "ON", 
                    "records": 0, "login_status": "⏳ Pending First Login"
                }
                safe_set_temp_data(chat_id, temp)
                update_admin_prompt(chat_id, temp, render_body_text("1️⃣ <b>Login URL</b>\n➡️ Enter the panel login link:"), reply_markup=get_cancel_kb())
                return
            else:
                with data_lock:
                    bot_settings["panels"].append({
                        "name": p_name, "type": "API Panel", "status": "OFF", 
                        "api_url": "", "token": "", "records": 0
                    })
                    save_db()
                if msg_id:
                    handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_api_panels", "id": "internal"})
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return

        elif state == "wait_for_p_api" and text:
            if not is_http_url(text.strip()):
                send_message(chat_id, render_body_text("❌ Invalid API URL. Send a full URL such as <code>https://api.example.com/numbers</code>."), reply_markup=get_cancel_kb())
                return
            idx = temp.get("p_idx") if temp else None
            if idx is None:
                send_message(chat_id, render_body_text("❌ Session expired!"))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
            with data_lock:
                bot_settings["panels"][idx]["api_url"] = text.strip()
                save_db()
            flush_db_writer(timeout=5.0)
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            show_panel_api_configuration(chat_id, msg_id, idx)
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_p_tok" and text:
            if not text.strip():
                send_message(chat_id, render_body_text("❌ Token cannot be empty. Send the provider token or cancel."), reply_markup=get_cancel_kb())
                return
            idx = temp.get("p_idx") if temp else None
            if idx is None:
                send_message(chat_id, render_body_text("❌ Session expired!"))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
            with data_lock:
                bot_settings["panels"][idx]["token"] = text.strip()
                save_db()
            flush_db_writer(timeout=5.0)
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            show_panel_api_configuration(chat_id, msg_id, idx)
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_p_fapi" and text:
            if not is_http_url(text.strip()):
                send_message(chat_id, render_body_text("❌ Invalid full API URL. Send a URL such as <code>https://api.example.com/get?key=VALUE&amp;start=0</code>."), reply_markup=get_cancel_kb())
                return
            idx = temp.get("p_idx") if temp else None
            if idx is None:
                send_message(chat_id, render_body_text("❌ Session expired!"))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
            with data_lock:
                bot_settings["panels"][idx]["full_api_url"] = text.strip()
                save_db()
            flush_db_writer(timeout=5.0)
            delete_message(chat_id, msg["message_id"])
            msg_id = temp.get("msg_id") if temp else None
            show_panel_api_configuration(chat_id, msg_id, idx)
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "wait_for_p_rec" and text:
            if text.strip().isdigit():
                idx = temp.get("p_idx") if temp else None
                if idx is None:
                    send_message(chat_id, render_body_text("❌ Session expired!"))
                    safe_set_user_state(chat_id, None)
                    safe_set_temp_data(chat_id, None)
                    return
                with data_lock:
                    bot_settings["panels"][idx]["records"] = int(text)
                    save_db()
                flush_db_writer(timeout=5.0)
                delete_message(chat_id, msg["message_id"])
                msg_id = temp.get("msg_id") if temp else None
                show_panel_api_configuration(chat_id, msg_id, idx)
            else:
                send_message(chat_id, render_body_text("❌ Please enter a non-negative whole number, for example <code>10</code>; use <code>0</code> for unlimited. Try again."), reply_markup=get_cancel_kb())
                return
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "set_number_setting" and text:
            msg_id = temp.get("msg_id") if temp else None
            key = temp.get("key") if temp else None
            try:
                value = int(text.strip())
                if key == "number_limit":
                    if not 1 <= value <= 100:
                        raise ValueError("Number limit must be between 1 and 100.")
                    bot_settings["number_limit"] = value
                    # Keep the older WAO setting in sync for existing admins
                    # and installations that still display NUM/REQ.
                    bot_settings["num_req"] = value
                elif key == "change_number_cooldown":
                    if not 0 <= value <= 86400:
                        raise ValueError("Change cooldown must be between 0 and 86400 seconds.")
                    bot_settings["change_number_cooldown"] = value
                else:
                    raise ValueError("Unknown number setting.")

                with data_lock:
                    save_db()
                if msg_id:
                    updated = edit_message(
                        chat_id,
                        msg_id,
                        number_settings_text(),
                        reply_markup=number_settings_keyboard(),
                    )
                    if not updated or not updated.get("ok"):
                        # If the original settings message was deleted or
                        # became uneditable, still show the saved values in a
                        # fresh message instead of reporting a false failure.
                        send_message(
                            chat_id,
                            number_settings_text(),
                            reply_markup=number_settings_keyboard(),
                        )
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
            except (TypeError, ValueError) as e:
                prompt = (
                    "📝 Send a whole number between <b>1 and 100</b> for Number Limit:"
                    if key == "number_limit"
                    else "📝 Send seconds between <b>0 and 86400</b> for Change Number:"
                )
                send_message(
                    chat_id,
                    render_body_text(f"❌ {html.escape(str(e))}\n\n{prompt}"),
                    reply_markup={
                        "inline_keyboard": [[
                            {
                                "text": "Cancel",
                                "icon_custom_emoji_id": "5267490665117275176",
                                "callback_data": "cancel_number_setting",
                                "style": "danger",
                            }
                        ]]
                    },
                )
            except Exception as e:
                logger.exception("Error saving number setting %s", key)
                send_message(
                    chat_id,
                    render_body_text(
                        f"❌ Could not save this setting right now: {html.escape(str(e))}\n\n"
                        "Please try again."
                    ),
                    reply_markup={
                        "inline_keyboard": [[
                            {
                                "text": "Cancel",
                                "icon_custom_emoji_id": "5267490665117275176",
                                "callback_data": "cancel_number_setting",
                                "style": "danger",
                            }
                        ]]
                    },
                )
            return

        elif state == "set_WAO":
            msg_id = temp.get("msg_id") if temp else None
            key = temp.get("key") if temp else None
            if not key:
                send_message(chat_id, render_body_text("❌ Session expired!"))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
            try:
                with data_lock:
                    if key in ["min_withdraw", "otp_reward", "refer_reward"]:
                        value = float(text)
                        if not math.isfinite(value) or value < 0:
                            raise ValueError("Value cannot be negative.")
                        bot_settings[key] = value
                    elif key in ["cooldown", "num_req", "num_share", "change_number_cooldown"]:
                        value = int(text)
                        if key == "num_req" and not 1 <= value <= 100:
                            raise ValueError("Number limit must be between 1 and 100.")
                        if key == "num_share" and not 1 <= value <= 100:
                            raise ValueError("Number share must be between 1 and 100.")
                        if key in ["cooldown", "change_number_cooldown"] and not 0 <= value <= 86400:
                            raise ValueError("Cooldown must be between 0 and 86400 seconds.")
                        bot_settings[key] = value
                        if key == "num_req":
                            bot_settings["number_limit"] = value
                    else:
                        bot_settings[key] = text
                    save_db()
                delete_message(chat_id, msg["message_id"])
                if msg_id:
                    edit_message(chat_id, msg_id, render_body_text("🕹 <b>WAO CONTROL PANEL</b>"), reply_markup=WAO_control_keyboard())
            except Exception as e:
                logger.warning("Invalid WAO value for key %s from admin %s: %s", key, chat_id, e)
                send_message(chat_id, render_body_text(f"❌ Invalid value for <code>{html.escape(str(key))}</code>: {html.escape(str(e))}. Please try again or cancel."), reply_markup=get_cancel_kb())
                return
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

        elif state == "set_service_price" and text:
            msg_id_sp = (temp or {}).get("msg_id")
            sp_mode = (temp or {}).get("sp_mode", "add")
            sp_key = (temp or {}).get("sp_key")
            try:
                if sp_mode == "add":
                    # Expect "SERVICE::COUNTRY PRICE" or "SERVICE COUNTRY PRICE"
                    parts = text.strip().split()
                    if len(parts) < 2:
                        raise ValueError("Format: SERVICE::COUNTRY PRICE")
                    price_str = parts[-1]
                    key_part = " ".join(parts[:-1]).upper().replace(" ", "::")
                    if "::" not in key_part:
                        raise ValueError("Use SERVICE::COUNTRY format")
                    price_val = float(price_str)
                    if not math.isfinite(price_val) or price_val < 0:
                        raise ValueError("Price must be a non-negative number.")
                    with data_lock:
                        bot_settings.setdefault("service_prices", {})[key_part] = price_val
                        save_db()
                else:
                    # Edit mode: text is just the new price
                    if not sp_key:
                        raise ValueError("Session error — key missing.")
                    price_val = float(text.strip())
                    if not math.isfinite(price_val) or price_val < 0:
                        raise ValueError("Price must be a non-negative number.")
                    with data_lock:
                        bot_settings.setdefault("service_prices", {})[sp_key] = price_val
                        save_db()
                delete_message(chat_id, msg["message_id"])
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                txt = render_body_text(f"💲 <b>SERVICE × COUNTRY PRICE RULES</b>\n{ANIMATED_BANNER}\n✅ Price rule saved!")
                if msg_id_sp:
                    edit_message(chat_id, msg_id_sp, txt, reply_markup=service_prices_keyboard())
                else:
                    send_message(chat_id, txt, reply_markup=service_prices_keyboard())
            except Exception as sp_err:
                send_message(chat_id,
                    render_body_text(f"❌ {html.escape(str(sp_err))}\n\nTry again or cancel."),
                    reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "service_prices", "style": "danger"}]]}
                )
            return

        elif state == "wait_for_search" and text:
            query = text.strip().replace("+", "")
            if not query.isdigit() or len(query) < 3 or len(query) > 9:
                send_message(chat_id, render_body_text("❌ Please enter a valid 3 to 9 digit number!"))
                return
                
            wait_msg = send_message(chat_id, number_loading_text())
            wait_msg_id = wait_msg.get("result", {}).get("message_id")
            
            with data_lock:
                found_indices = []
                for b_id, b_data in number_batches.items():
                    if is_service_disabled(b_data.get("service", "")):
                        continue
                    for idx, n_obj in enumerate(b_data["numbers"]):
                        if n_obj["num"].replace("+", "").startswith(query) and chat_id not in n_obj.get("used_by", []):
                            found_indices.append((b_id, idx))
            
            fetched_nums = []
            if not found_indices:
                if wait_msg_id:
                    delete_message(chat_id, wait_msg_id)
                send_message(chat_id, render_body_text("❌ Number out of stock!"), reply_markup=main_menu(chat_id))
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
            else:
                random.shuffle(found_indices)
                with data_lock:
                    for b_id, idx in found_indices:
                        if len(fetched_nums) >= get_number_limit():
                            break
                        n_obj = number_batches[b_id]["numbers"][idx]
                        dirty_number_batches.add(b_id)
                        num_str = n_obj["num"]
                        
                        fetched_nums.append(num_str)
                        usage_batch = number_batches[b_id]
                        pending_number_usage.append((
                            str(chat_id),
                            normalize_service_name(usage_batch.get("service", "")),
                            normalize_service_name(usage_batch.get("country", "")),
                            num_str,
                            time.time(),
                        ))
                        
                        n_obj["shares"] += 1
                        n_obj["used_by"].append(chat_id)
                        total_assigned_stats += 1
                        
                        if n_obj["shares"] >= bot_settings.get("num_share", 1):
                            n_obj["to_remove"] = True
                            used_numbers_list.append(num_str)
                            pending_used_numbers.append(num_str)
                    
                    for b_id in number_batches:
                        number_batches[b_id]["numbers"] = [n for n in number_batches[b_id]["numbers"] if not n.get("to_remove")]
                    save_db(include_local=True)

                if fetched_nums:
                    expire_previous_number(chat_id, remove_message=True)
                
            if wait_msg_id:
                edit_message(chat_id, wait_msg_id, render_body_text("✅ Number Found!"))
            kb = []
            flags_db = bot_settings.get("premium_flags", {})
            for num in fetched_nums:
                _, iso = get_flag_and_code(num)
                display_num = f"+{num}" if not num.startswith("+") else num
                
                emoji_id = "5780471598922337683"
                for flag_code, flag_data in flags_db.items():
                    if iso == flag_data.get("iso"):
                        if "id" in flag_data:
                            emoji_id = flag_data["id"]
                        break
                kb.append([{"text": f"{display_num}", "icon_custom_emoji_id": emoji_id, "copy_text": {"text": display_num}, "style": "primary"}])
                
            search_token = _store_callback_payload("search_range", query, None)
            kb.append([{"text": "Change Number", "icon_custom_emoji_id": CHANGE_NUMBER_BUTTON_EMOJI_ID, "callback_data": f"c_n_s_{search_token}", "style": "danger"},
                       {"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": bot_settings["otp_link"], "style": "primary"}])
            
            c_btns = bot_settings["custom_messages"].get("search_number", {}).get("buttons", [])
            for c_b in c_btns:
                b_copy = c_b.copy()
                if "style" not in b_copy:
                    b_copy["style"] = "primary"
                kb.append([b_copy])
            
            kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
            
            if wait_msg_id:
                edit_message(chat_id, wait_msg_id, "ㅤ\n", reply_markup={"inline_keyboard": kb})
                with sessions_lock:
                    created_at = time.time()
                    user_active_sessions[chat_id] = {
                        "msg_id": wait_msg_id,
                        "nums": fetched_nums,
                        "created_at": created_at,
                        "expires_at": created_at + ACTIVE_USER_SESSION_TTL_SECONDS,
                    }
            else:
                msg_res = send_message(chat_id, "ㅤ\n", reply_markup={"inline_keyboard": kb})
                if msg_res and "result" in msg_res:
                    with sessions_lock:
                        created_at = time.time()
                        user_active_sessions[chat_id] = {
                            "msg_id": msg_res["result"]["message_id"],
                            "nums": fetched_nums,
                            "created_at": created_at,
                            "expires_at": created_at + ACTIVE_USER_SESSION_TTL_SECONDS,
                        }
            # Search is a one-shot prompt; clear it so the next message is handled normally.
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return
            
        elif state == "wait_for_withdraw_amount" and text:
            msg_id_to_edit = temp.get("msg_id") if temp else None
            try:
                amount = round(float(text.strip()), 4)
                bal = temp.get("balance") if temp else 0
                min_w = bot_settings['min_withdraw']
                
                if amount <= 0:
                    raise ValueError("Withdrawal amount must be greater than zero.")
                if amount + 1e-6 < min_w:
                    if msg_id_to_edit:
                        edit_message(chat_id, msg_id_to_edit, render_body_text(f"❌ Minimum withdrawal is {fmt_money(min_w)} ৳!\n💰 Balance: {fmt_money(bal)} ৳\n\n📝 Enter again:"), reply_markup=get_cancel_kb())
                    return
                if amount > bal + 1e-6:
                    if msg_id_to_edit:
                        edit_message(chat_id, msg_id_to_edit, render_body_text(f"❌ You don't have enough balance!\n💰 Balance: {fmt_money(bal)} ৳\n\n📝 Enter again:"), reply_markup=get_cancel_kb())
                    return
                    
                temp = temp or {}
                temp["amount"] = amount
                safe_set_temp_data(chat_id, temp)
                safe_set_user_state(chat_id, "wait_for_withdraw_number")
                if msg_id_to_edit:
                    edit_message(chat_id, msg_id_to_edit, render_body_text(f"✅ Amount: {fmt_money(amount)} ৳\n\n📱 Now send your <b>{temp.get('method')}</b> account number:"), reply_markup=get_cancel_kb())
            except ValueError:
                if msg_id_to_edit:
                    edit_message(chat_id, msg_id_to_edit, render_body_text("❌ Invalid amount!\n\n📝 Please send a valid number:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_2fa_key" and text:
            msg_id_to_edit = temp.get("msg_id") if temp else None
            delete_message(chat_id, msg["message_id"])

            if not msg_id_to_edit:
                send_message(chat_id, render_body_text("❌ Error: Message not found. Try again."))
                safe_set_user_state(chat_id, None)
                return

            try:
                secret = text.strip().replace(" ", "")
                totp = pyotp.TOTP(secret)
                code = totp.now()
                remaining_time = 30 - (int(time.time()) % 30)
                
                success_txt = (
                    f"{ANIMATED_BANNER}\n"
                    f"🔐 <b>2FA CODE</b>\n"
                    f"{ANIMATED_BANNER}\n"
                    f"🔐 <b>CODE:</b> <code>{code}</code>\n"
                    f"{ANIMATED_BANNER}\n"
                    f"🕓 <b>EXPIRES IN:</b> {remaining_time}s\n"
                    f"{ANIMATED_BANNER}"
                )
                safe_set_temp_data(chat_id, {"msg_id": msg_id_to_edit, "secret": secret})
                kb = [[{"text": f"Click to copy {code}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": code}, "style": "success"}],
                      [{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": "ref_2fa_current", "style": "primary"},
                       {"text": "New Code", "icon_custom_emoji_id": "5352552689983067014", "callback_data": "gen_2fa", "style": "danger"}],
                      [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
                
                edit_message(chat_id, msg_id_to_edit, render_body_text(success_txt), reply_markup={"inline_keyboard": kb})
                safe_set_user_state(chat_id, None)
            except Exception as e:
                logger.error(f"2FA error: {e}")
                error_txt = f"🔑 <b>ENTER 2FA KEY</b>\n{ANIMATED_BANNER}\n📝 <b>SEND YOUR 2FA SECRET KEY</b>\n{ANIMATED_BANNER}\n❌ <b>Invalid Secret Key. Try again.</b>\n{ANIMATED_BANNER}"
                cancel_kb = {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_2fa", "style": "danger"}]]}
                edit_message(chat_id, msg_id_to_edit, render_body_text(error_txt), reply_markup=cancel_kb)
            return

        elif state == "wait_for_withdraw_number":
            msg_id_to_edit = temp.get("msg_id") if temp else None
            
            method = temp.get("method") if temp else "Unknown"
            amount = temp.get("amount") if temp else 0
            number = text.strip()
            if not number:
                if msg_id_to_edit:
                    edit_message(
                        chat_id,
                        msg_id_to_edit,
                        render_body_text("❌ Please send a valid account number."),
                        reply_markup=get_cancel_kb(),
                    )
                return
            req_id = f"W_{str(uuid.uuid4())[:6].upper()}"
            
            first_name = msg.get("from", {}).get("first_name", "User")
            last_name = msg.get("from", {}).get("last_name", "")
            full_name = f"{first_name} {last_name}".strip()
            
            if not deduct_balance(chat_id, amount):
                if msg_id_to_edit:
                    edit_message(
                        chat_id,
                        msg_id_to_edit,
                        render_body_text("❌ Your balance changed. Please start the withdrawal again."),
                        reply_markup=get_cancel_kb(),
                    )
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return

            with data_lock:
                pending_withdrawals[req_id] = {
                    "user_id": chat_id, "amount": amount, "method": method, 
                    "number": number, "full_name": full_name
                }
            
            try:
                conn = get_db()
                try:
                    cursor = conn.cursor()
                    cursor.execute('''INSERT INTO withdrawals (req_id, user_id, amount, method, number, full_name, status) 
                                   VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                                   (req_id, str(chat_id), amount, method, number, full_name, 'pending'))
                    conn.commit()
                finally:
                    conn.close()
                # ── analytics_addon ──────────────────────────────────
                try:
                    analytics_increment("withdrawals")
                except Exception:
                    logger.exception("analytics withdrawal increment failed")
                # ─────────────────────────────────────────────────────
                if db:
                    try:
                        db.collection("withdrawals").document(req_id).set({
                            "req_id": req_id,
                            "user_id": int(chat_id),
                            "amount": float(amount),
                            "method": method,
                            "number": number,
                            "full_name": full_name,
                            "status": "pending",
                            "timestamp": firestore.SERVER_TIMESTAMP,
                        }, timeout=3)
                    except Exception as e:
                        logger.warning("Could not save withdrawal %s to Firestore: %s", req_id, e)
            except Exception as e:
                logger.error(f"Error saving withdrawal: {e}")
                with data_lock:
                    pending_withdrawals.pop(req_id, None)
                update_balance(chat_id, amount)
                send_message(
                    chat_id,
                    render_body_text("❌ Could not save the withdrawal request. Your balance was restored."),
                    reply_markup=main_menu(chat_id),
                )
                safe_set_user_state(chat_id, None)
                safe_set_temp_data(chat_id, None)
                return
                
            if bot_settings["w_group"]:
                admin_msg = f"🎙 <b>NEW WITHDRAWAL REQUEST</b>\n\n👤 <b>USER:</b> <a href='tg://user?id={chat_id}'>{html.escape(str(full_name))}</a>\n💳 <b>WITHDRAWAL:</b> {fmt_money(amount)} TK\n🍏 <b>NUMBER:</b> <code>{html.escape(str(number))}</code>\n🏦 <b>METHOD:</b> {html.escape(str(method))}\n\n🧾 <b>REQ ID:</b> {req_id}\n👨‍⚖️ <b>PROCESSED BY ADMIN</b>"
                kb = {"inline_keyboard": [[{"text": "APPROVE", "icon_custom_emoji_id": "5352694861990501856", "callback_data": f"wapp_{req_id}", "style": "success"}, {"text": "REJECT", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"wrej_{req_id}", "style": "danger"}]]}
                send_message(bot_settings["w_group"], render_body_text(admin_msg), reply_markup=kb)
            
            kb = {"inline_keyboard": [[{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]}
            success_text = f"{PEM['ok']} Your withdrawal request has been submitted!\n\n🧾 <b>Req ID:</b> {req_id}\n💰 <b>Amount:</b> {fmt_money(amount)} ৳\n🏦 <b>Method:</b> {html.escape(str(method))}\n📱 <b>Number:</b> <code>{html.escape(str(number))}</code>"
            
            if msg_id_to_edit:
                edit_message(chat_id, msg_id_to_edit, render_body_text(success_text), reply_markup=kb)
            else:
                send_message(chat_id, render_body_text(success_text), reply_markup=kb)
                
            safe_set_user_state(chat_id, None)
            safe_set_temp_data(chat_id, None)
            return

    # --- Regular Commands ---
    if text and text.startswith("/start"):
        send_start_welcome(chat_id)
        # Firebase reads and referral bookkeeping remain off the update thread.
        finish_startup_tasks(chat_id)

    elif text in {"/ref", f"/ref@{BOT_USERNAME}"}:
        # ── advanced_features: styled referral card ───────────────────────
        card = format_referral_card(chat_id)
        send_message(chat_id, render_body_text(card))

    elif text in {"/setlang", "/language",
                  f"/setlang@{BOT_USERNAME}", f"/language@{BOT_USERNAME}"}:
        # ── advanced_features: language picker ───────────────────────────
        prompt = get_text(chat_id, "select_language")
        send_message(
            chat_id,
            render_body_text(f"🌐 <b>{prompt}</b>"),
            reply_markup=build_language_keyboard(),
        )

    elif text in {"/health", "/status"} and is_admin(chat_id):
        send_message(chat_id, render_body_text(system_status_text()))

    elif text == get_text(chat_id, "menu_traffic"):
        txt, markup = build_traffic_ui()
        send_message(chat_id, txt, reply_markup=markup)
        
    elif text == get_text(chat_id, "menu_refer"):
        u_data = get_user(chat_id)
        ref_link = f"https://t.me/{BOT_USERNAME}?start={chat_id}"
        c_msg = bot_settings["custom_messages"].get("refer", {})
        
        raw_txt = c_msg.get("text", f"{PEM['gift']} Refer").replace("{ref_link}", ref_link).replace("{total_ref}", str(u_data.get('total_refers', 0))).replace("{ref_reward}", fmt_money(bot_settings['refer_reward']))
        txt = render_body_text(raw_txt)
        
        kb = [[{"text": "COPY LINK", "icon_custom_emoji_id": "5192739271886282680", "copy_text": {"text": ref_link}, "style": "success"}]]
        for b in c_msg.get("buttons", []):
            b_copy = b.copy()
            if "style" not in b_copy:
                b_copy["style"] = "primary"
            kb.append([b_copy])
        kb.append([{"text": "CLOSE", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text == get_text(chat_id, "menu_withdrawal"):
        if not bot_settings["withdraw_on"]:
            send_message(chat_id, render_body_text(f"{PEM['no']} Withdrawals are currently disabled."))
            return
        
        u_data = get_user(chat_id)
        bal = u_data.get('balance', 0.0)
        
        c_msg = bot_settings["custom_messages"].get("withdrawal", {})
        raw_txt = c_msg.get("text", "Withdrawal").replace("{bal}", fmt_money(bal)).replace("{total_otp}", str(u_data.get('total_otps', 0))).replace("{total_ref}", str(u_data.get('total_refers', 0))).replace("{min_w}", fmt_money(bot_settings['min_withdraw']))
        txt = render_body_text(raw_txt)
        
        kb = []
        for m in bot_settings["w_methods"]:
            kb.append([{"text": m.strip(), "icon_custom_emoji_id": "5190899075968441286", "callback_data": f"sel_wm_{m.strip()}", "style": "primary"}])
        
        for b in c_msg.get("buttons", []):
            b_copy = b.copy()
            if "style" not in b_copy:
                b_copy["style"] = "primary"
            kb.append([b_copy])
        kb.append([{"text": "Cancel", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text == get_text(chat_id, "menu_admin") and is_admin(chat_id):
        send_message(chat_id, get_admin_text(), reply_markup=admin_panel_keyboard())

    elif text == get_text(chat_id, "menu_get_number"):
        with get_number_debounce_lock:
            now = time.monotonic()
            previous_click = last_get_number_click.get(str(chat_id), 0.0)
            if now - previous_click < GET_NUMBER_DEBOUNCE_SECONDS:
                logger.info(
                    "GET NUMBER debounced user=%s instance_id=%s",
                    chat_id,
                    INSTANCE_ID,
                )
                return
            last_get_number_click[str(chat_id)] = now
            if len(last_get_number_click) > 5000:
                stale_before = now - 300
                for stale_key in [
                    k for k, v in last_get_number_click.items() if v < stale_before
                ]:
                    last_get_number_click.pop(stale_key, None)
        logger.info(
            "GET NUMBER user=%s instance_id=%s",
            chat_id,
            INSTANCE_ID,
        )
        txt, markup = fresh_get_number_screen()
        if markup is None:
            snapshot = inventory_diagnostic_snapshot()
            logger.warning(
                "GET NUMBER empty inventory user=%s instance_id=%s "
                "batches=%s numbers=%s disabled_services=%s used=%s",
                chat_id,
                INSTANCE_ID,
                snapshot["batches"],
                snapshot["numbers"],
                snapshot["disabled_services"],
                snapshot["used_numbers"],
            )
            send_message(chat_id, render_body_text(f"{PEM['no']} No numbers or services available!"))
        else:
            send_message(chat_id, txt, reply_markup=markup)

    elif text == get_text(chat_id, "menu_search"):
        safe_set_user_state(chat_id, "wait_for_search")
        c_msg = bot_settings["custom_messages"].get("search_number", {})
        txt = render_body_text(c_msg.get("text", f"{PEM['num']} Search Number"))
        kb = [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_state", "style": "danger"}]]
        for b in c_msg.get("buttons", []):
            b_copy = b.copy()
            if "style" not in b_copy:
                b_copy["style"] = "primary"
            kb.append([b_copy])
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text in {get_text(chat_id, "menu_2fa"), "🔐 2FA ONLINE"}:
        txt = f"🔐 <b>2FA ONLINE</b>\n{ANIMATED_BANNER}\n<i>Generate your 2FA security code instantly using your secret key.</i>\n{ANIMATED_BANNER}"
        kb = [[{"text": "Generate 2fa code", "icon_custom_emoji_id": "5353022963132174959", "callback_data": "gen_2fa", "style": "success"}],
              [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
        send_message(chat_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif text == get_text(chat_id, "menu_support"):
        c_msg = bot_settings["custom_messages"].get("support", {})
        txt = render_body_text(c_msg.get("text", f"{PEM['msg']} Support"))
        if not txt.strip():
            txt = render_body_text(f"{PEM['msg']} Support")
        kb = []
        for b in c_msg.get("buttons", []):
            b_copy = b.copy()
            if "style" not in b_copy:
                b_copy["style"] = "primary"
            kb.append([b_copy])
            
        sup_link = bot_settings.get("support_link", "")
        if sup_link:
            kb.insert(0, [{"text": "Contact Support", "icon_custom_emoji_id": "5337302974806922068", "url": sup_link, "style": "success"}])
            
        kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb} if kb else None)

def handle_message(msg):
    """Process an admin reply, then remove the input once the handler has consumed it."""
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = msg.get("text", "")
    main_menu_commands = {
        "GET NUMBER", "Search Number", "TRAFFIC", "Refer", "WITHDRAWAL",
        "SUPPORT", "Admin Panel", "2FA ONLINE",
    }
    is_main_command = text in main_menu_commands or (
        isinstance(text, str) and text.startswith("/start")
    )
    initial_state = safe_get_user_state(chat_id) if chat_id is not None else None
    original_prompt_id = (
        admin_prompt_messages.get(chat_id)
        if chat_id is not None and is_admin(chat_id)
        else None
    )
    cleanup_admin_flow = bool(
        chat.get("type", "private") == "private"
        and chat_id is not None
        and is_admin(chat_id)
        and initial_state
        and not is_main_command
        and msg.get("message_id")
    )
    remove_after_processing = bool(
        cleanup_admin_flow
        and initial_state != "wait_for_broadcast"
    )

    if remove_after_processing:
        admin_interaction_context.input_key = (
            chat_id,
            msg.get("message_id"),
        )
        admin_interaction_context.input_deleted = False
    try:
        return _handle_message_impl(msg)
    finally:
        if cleanup_admin_flow:
            cleanup_admin_interaction(
                chat_id,
                msg["message_id"],
                initial_state,
                original_prompt_id=original_prompt_id,
            )
        if (
            remove_after_processing
            and not getattr(admin_interaction_context, "input_deleted", False)
        ):
            delete_message(chat_id, msg["message_id"])
        if remove_after_processing:
            admin_interaction_context.input_key = None
            admin_interaction_context.input_deleted = False

def expire_previous_number(chat_id, remove_message=True):
    with sessions_lock:
        prev_data = user_active_sessions.pop(chat_id, None)
        if not prev_data:
            return None
        prev_msg_id = prev_data.get("msg_id")
        prev_numbers = list(prev_data.get("nums", []))

    mark_number_usage_released(chat_id, prev_numbers)

    # Keep the same Telegram message when Change Number is pressed so it can
    # transition directly from the old result to loading, then the new result.
    # (msg_id is None for sessions restored after a restart.)
    if remove_message and prev_msg_id:
        delete_message(chat_id, prev_msg_id)
    return prev_msg_id


def mark_number_usage_released(user_id, numbers):
    numbers = list(dict.fromkeys(str(number) for number in numbers if number))
    if not numbers:
        return
    conn = get_db()
    try:
        conn.executemany(
            "UPDATE number_usage SET released = 1 "
            "WHERE user_id = ? AND number = ?",
            [(str(user_id), number) for number in numbers],
        )
        conn.commit()
    except sqlite3.DatabaseError:
        logger.exception("Could not mark released number sessions for user %s", user_id)
    finally:
        conn.close()


def release_number_session_for_message(chat_id, message_id):
    with sessions_lock:
        session = user_active_sessions.get(chat_id)
        if (
            not session
            or session.get("msg_id") is None
            or str(session.get("msg_id")) != str(message_id)
        ):
            return False
        released_session = user_active_sessions.pop(chat_id)
    mark_number_usage_released(chat_id, released_session.get("nums", []))
    return True


ACTIVE_SESSION_RESTORE_SECONDS = ACTIVE_USER_SESSION_TTL_SECONDS


def restore_active_sessions():
    """Rebuild user_active_sessions from SQLite after a restart.

    user_active_sessions lives in RAM only, so after every restart/redeploy a
    user's already-assigned number was no longer linked to them and its OTP
    never reached their inbox until they requested a new number.  The
    number_usage table already records every assignment, so we restore the most
    recent numbers (up to the per-user limit) of each user.
    """
    try:
        limit = max(1, int(get_number_limit()))
    except Exception:
        limit = 1
    restored = 0
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT user_id, number, used_at FROM number_usage WHERE used_at >= ? "
            "AND released = 0 "
            "ORDER BY used_at DESC",
            (time.time() - ACTIVE_SESSION_RESTORE_SECONDS,),
        ).fetchall()
    except Exception:
        logger.exception("Could not restore active number sessions")
        return 0
    finally:
        conn.close()
    per_user = {}
    for row in rows:
        try:
            uid = int(row["user_id"])
        except (TypeError, ValueError):
            continue
        entry = per_user.setdefault(uid, {"nums": [], "created_at": float(row["used_at"])})
        entry["created_at"] = min(entry["created_at"], float(row["used_at"]))
        if len(entry["nums"]) < limit:
            entry["nums"].append(row["number"])
    with sessions_lock:
        for uid, entry in per_user.items():
            if uid not in user_active_sessions:
                user_active_sessions[uid] = {
                    "msg_id": None,
                    "nums": entry["nums"],
                    "created_at": entry["created_at"],
                    "expires_at": entry["created_at"] + ACTIVE_USER_SESSION_TTL_SECONDS,
                }
                restored += 1
    logger.info("Restored %s active number session(s) from the database", restored)
    return restored

# ==========================================
# Callback Query Handler
# ==========================================
ADMIN_CALLBACK_EXACT = {
    "admin_panel", "back_to_admin", "system_status", "system_settings",
    "download_backup", "user_management", "menu_design_list",
    "md_reset_defaults", "test_message_flow", "manage_emojis",
    "up_flags_txt", "up_apps_txt", "add_single_emoji", "dl_flags_txt",
    "dl_apps_txt", "del_all_flags", "broadcast_msg", "upload_num",
    "delete_files", "show_used", "show_unused", "dl_used", "dl_unused",
    "lb_main", "manage_fj", "toggle_fj", "add_fj", "test_fj",
    "manage_admins", "add_adm", "manage_otp_groups", "add_fw",
    "edit_otp_link", "manage_panels", "manage_api_panels",
    "manage_cpt_panels", "add_api_panel", "add_cpt_panel",
    "list_del_api", "list_del_cpt", "WAO_control", "WAO_toggle_w",
    "manage_w_methods", "add_wm", "number_settings",
    "set_number_limit", "set_change_number_cooldown",
    "cancel_number_setting", "cancel_WAO_edit", "dummy_alert",
}
ADMIN_CALLBACK_PREFIXES = (
    "um_", "md_", "del_", "manage_", "add_", "tog_pnl_", "conf_pnl_",
    "set_", "lb_", "dl_", "show_", "test_", "oib_", "up_", "WAO_",
    "do_del_pnl_", "list_del_", "back_to_admin", "close_cpt_",
)


def _is_admin_only_callback(data):
    if data in ADMIN_CALLBACK_EXACT:
        return True
    return str(data or "").startswith(ADMIN_CALLBACK_PREFIXES)


def _callback_missing(call):
    answer_callback(call["id"], "এটি আর নেই", show_alert=True)


def _safe_callback_index(call, data, part_index, list_key):
    try:
        idx = int(str(data).split("_")[part_index])
    except (IndexError, TypeError, ValueError):
        _callback_missing(call)
        return None
    with data_lock:
        values = bot_settings.get(list_key, [])
        valid = isinstance(values, list) and 0 <= idx < len(values)
    if not valid:
        _callback_missing(call)
        return None
    return idx


def _safe_nested_callback_index(
    call, data, parent_part_index, list_key, child_part_index, child_key
):
    parent_idx = _safe_callback_index(
        call, data, parent_part_index, list_key
    )
    if parent_idx is None:
        return None
    try:
        child_idx = int(str(data).split("_")[child_part_index])
    except (IndexError, TypeError, ValueError):
        _callback_missing(call)
        return None
    with data_lock:
        parents = bot_settings.get(list_key, [])
        children = (
            parents[parent_idx].get(child_key, [])
            if 0 <= parent_idx < len(parents)
            and isinstance(parents[parent_idx], dict)
            else []
        )
        valid = isinstance(children, list) and 0 <= child_idx < len(children)
    if not valid:
        _callback_missing(call)
        return None
    return parent_idx, child_idx


def _safe_custom_button_index(call, menu_key, button_index):
    try:
        button_index = int(button_index)
    except (TypeError, ValueError):
        _callback_missing(call)
        return None
    with data_lock:
        menu = bot_settings.get("custom_messages", {}).get(menu_key)
        buttons = menu.get("buttons", []) if isinstance(menu, dict) else []
        valid = isinstance(buttons, list) and 0 <= button_index < len(buttons)
    if not valid:
        _callback_missing(call)
        return None
    return button_index


def handle_callback(call):
    global total_assigned_stats
    chat_id = call["message"]["chat"]["id"]
    # ── Rate limiter (advanced_features) ─────────────────────────────────
    if not check_rate_limit(chat_id):
        return
    # ─────────────────────────────────────────────────────────────────────
    chat_type = call["message"]["chat"].get("type", "private")
    data = call.get("data", "")
    callback_user_id = (call.get("from") or {}).get("id", chat_id)
    if _is_admin_only_callback(data) and not is_admin(callback_user_id):
        answer_callback(call["id"], "Admin access required.", show_alert=True)
        return

    is_force_join_admin_action = (
        data in {"manage_fj", "toggle_fj", "add_fj", "test_fj"}
        or data.startswith(("del_fj_", "set_fj_invite_"))
    )
    if is_force_join_admin_action and not is_admin(chat_id):
        answer_callback(call["id"], "Admin access required.", show_alert=True)
        return

    if chat_type != "private" and not (data.startswith("wapp_") or data.startswith("wrej_")):
        return

    msg_id = call["message"]["message_id"]

    if chat_type == "private":
        if is_user_banned(chat_id):
            answer_callback(call["id"], "🚫 You are banned from using this bot!", show_alert=True)
            return

        if (
            not check_force_join(chat_id, log_context=f"callback:{data}")
            and data not in {"check_fj", "refresh_fj"}
        ):
            send_force_join_msg(chat_id)
            return

    if data == "refresh_fj":
        with force_join_cache_lock:
            force_join_cache.pop(str(chat_id), None)
        send_force_join_msg(chat_id, message_id=msg_id)
        answer_callback(call["id"])
        return

    if data == "check_fj":
        with force_join_cache_lock:
            force_join_cache.pop(str(chat_id), None)
        missing = get_missing_force_join_channels(
            chat_id,
            force_refresh=True,
        )
        if not missing:
            delete_message(chat_id, msg_id)
            send_start_welcome(chat_id)
            finish_startup_tasks(chat_id)
        else:
            names = [
                _force_join_channel_details(channel)[0]
                for channel in missing
            ]
            answer_callback(
                call["id"],
                "Still need to join: " + ", ".join(names),
                show_alert=True,
            )
            send_force_join_msg(chat_id, message_id=msg_id)
        return

    # ── analytics_addon: UI callbacks ────────────────────────────────────
    if handle_analytics_callback(
        chat_id, msg_id, call["id"], data, answer_callback, is_admin
    ):
        return
    # ─────────────────────────────────────────────────────────────────────

    # ── advanced_features: language picker + targeted broadcast ──────────
    if handle_advanced_callback(
        chat_id, msg_id, call["id"], data,
        answer_callback, is_admin, safe_set_user_state,
    ):
        return
    # ─────────────────────────────────────────────────────────────────────

    if data == "close_msg":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        release_number_session_for_message(chat_id, msg_id)
        delete_message(chat_id, msg_id)

    elif data.startswith("cp_"):
        answer_callback(call["id"], data[3:], show_alert=True)

    elif data.startswith("close_cpt_"):
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        try:
            document_message_id = int(data.rsplit("_", 1)[1])
            delete_message(chat_id, document_message_id)
        except (IndexError, TypeError, ValueError) as parse_error:
            logger.warning("Invalid close-panel callback data %r: %s", data, parse_error)
        delete_message(chat_id, msg_id)

    elif data == "back_get_services":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        txt, markup = fresh_get_number_screen()
        if markup is None:
            snapshot = inventory_diagnostic_snapshot()
            logger.warning(
                "GET NUMBER back screen empty user=%s batches=%s numbers=%s "
                "disabled_services=%s used=%s instance_id=%s",
                chat_id,
                snapshot["batches"],
                snapshot["numbers"],
                snapshot["disabled_services"],
                snapshot["used_numbers"],
                INSTANCE_ID,
            )
            edit_message(
                chat_id,
                msg_id,
                render_body_text(f"{PEM['no']} No numbers or services available!"),
                reply_markup=None,
            )
        else:
            edit_message(chat_id, msg_id, txt, reply_markup=markup)
        if call.get("id") != "internal":
            answer_callback(call["id"])

    elif data == "cancel_state":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        delete_message(chat_id, msg_id)

    elif data == "cancel_2fa":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        txt = f"🔐 <b>2FA ONLINE</b>\n{ANIMATED_BANNER}\n<i>Generate your 2FA security code instantly using your secret key.</i>\n{ANIMATED_BANNER}"
        kb = [[{"text": "Generate 2fa code", "icon_custom_emoji_id": "5353022963132174959", "callback_data": "gen_2fa", "style": "success"}],
              [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})
        answer_callback(call["id"])

    elif data == "gen_2fa":
        safe_set_user_state(chat_id, "wait_for_2fa_key")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        txt = f"🔑 <b>ENTER 2FA KEY</b>\n{ANIMATED_BANNER}\n📝 <b>SEND YOUR 2FA SECRET KEY</b>\n{ANIMATED_BANNER}"
        kb = {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_2fa", "style": "danger"}]]}
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup=kb)
        answer_callback(call["id"])

    elif data.startswith("ref_2fa_"):
        token = data.replace("ref_2fa_", "")
        stored = safe_get_temp_data(chat_id) or {}
        secret = stored.get("secret") if token == "current" else None
        # Accept legacy callbacks only for already-rendered messages.  New
        # buttons never put the TOTP secret into Telegram callback data.
        if not secret and token != "current":
            secret = token
        try:
            totp = pyotp.TOTP(secret)
            code = totp.now()
            remaining_time = 30 - (int(time.time()) % 30)
            
            success_txt = (
                f"{ANIMATED_BANNER}\n"
                f"🔐 <b>2FA CODE</b>\n"
                f"{ANIMATED_BANNER}\n"
                f"🔐 <b>CODE:</b> <code>{code}</code>\n"
                f"{ANIMATED_BANNER}\n"
                f"🕓 <b>EXPIRES IN:</b> {remaining_time}s\n"
                f"{ANIMATED_BANNER}"
            )
            safe_set_temp_data(chat_id, {"msg_id": msg_id, "secret": secret})
            kb = [[{"text": f"Click to copy {code}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": code}, "style": "success"}],
                  [{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": "ref_2fa_current", "style": "primary"},
                   {"text": "New Code", "icon_custom_emoji_id": "5352552689983067014", "callback_data": "gen_2fa", "style": "danger"}],
                  [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
            
            edit_message(chat_id, msg_id, render_body_text(success_txt), reply_markup={"inline_keyboard": kb})
        except Exception as e:
            logger.error(f"2FA refresh error: {e}")
            answer_callback(call["id"], "❌ Error refreshing code!", show_alert=True)

    elif data == "cancel_WAO_edit":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>WAO CONTROL PANEL</b>"), reply_markup=WAO_control_keyboard())
        
    elif data == "dummy_alert":
        answer_callback(call["id"], "This feature will be added later!", show_alert=True)
        
    elif data == "cancel_number_setting":
        pending = safe_get_temp_data(chat_id) or {}
        original_msg_id = pending.get("msg_id")
        if original_msg_id and original_msg_id != msg_id:
            delete_message(chat_id, original_msg_id)
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        edit_message(
            chat_id,
            msg_id,
            number_settings_text(),
            reply_markup=number_settings_keyboard(),
        )

    elif data == "number_settings":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        edit_message(
            chat_id,
            msg_id,
            number_settings_text(),
            reply_markup=number_settings_keyboard(),
        )

    elif data in ("set_number_limit", "set_change_number_cooldown"):
        key = "number_limit" if data == "set_number_limit" else "change_number_cooldown"
        label = "Number Limit" if key == "number_limit" else "Change Number cooldown"
        current = get_number_limit() if key == "number_limit" else get_change_number_cooldown()
        safe_set_user_state(chat_id, "set_number_setting")
        safe_set_temp_data(chat_id, {"msg_id": msg_id, "key": key})
        edit_message(
            chat_id,
            msg_id,
            render_body_text(
                f"📝 <b>{label}</b>\n\n"
                f"Current value: <b>{current}{'' if key == 'number_limit' else ' seconds'}</b>\n"
                f"Send the new value now."
            ),
            reply_markup={
                "inline_keyboard": [[
                    {
                        "text": "Cancel",
                        "icon_custom_emoji_id": "5267490665117275176",
                        "callback_data": "cancel_number_setting",
                        "style": "danger",
                    }
                ]]
            },
        )

    elif data == "refresh_traffic":
        txt, markup = build_traffic_ui()
        edit_message(chat_id, msg_id, txt, reply_markup=markup)
        answer_callback(call["id"], "✅ Traffic Refreshed!", show_alert=False)

    elif data.startswith("exp_rng_"):
        service_token = data[len("exp_rng_"):]
        resolved_service = _get_callback_payload(
            service_token, "explore_service"
        )
        if resolved_service:
            srv_query = resolved_service[0]
        elif _is_short_callback_token(service_token):
            answer_callback(call["id"], "এটি আর নেই", show_alert=True)
            return
        else:
            # Compatibility with previously-rendered buttons.
            srv_query = service_token
        
        country_stats = {}
        current_time = time.time()
        with data_lock:
            for t in recent_traffic:
                if current_time - t.get("time", 0) <= 3600:
                    if t.get("service", "").startswith(srv_query):
                        iso = t.get("iso", "XX")
                        flag = t.get("flag", "🌍")
                        if iso not in country_stats:
                            country_stats[iso] = {"count": 0, "flag": flag}
                        country_stats[iso]["count"] += 1
        
        if not country_stats:
            answer_callback(call["id"], "❌ No recent traffic found for this service!", show_alert=True)
            return
            
        kb = []
        for iso, c_data in sorted(country_stats.items(), key=lambda x: x[1]["count"], reverse=True):
            count = c_data["count"]
            c_name = iso
            emoji_id = "5780471598922337683"
            for code, fdata in bot_settings.get("premium_flags", {}).items():
                if fdata.get("iso") == iso:
                    c_name = fdata.get("name", iso)
                    if "id" in fdata:
                        emoji_id = fdata["id"]
                    break
            
            btn_text = f"{c_name} ({iso}) - {count} OTP"
            country_token = _store_callback_payload(
                "explore_country", srv_query, iso
            )
            kb.append([{"text": btn_text, "icon_custom_emoji_id": emoji_id, "callback_data": f"exp_c_{country_token}", "style": "primary"}])
            
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "refresh_traffic", "style": "danger"}])
        
        app_full_name, prem_app_html = get_service_info_html(srv_query)
        edit_message(chat_id, msg_id, render_body_text(f"📊 <b>Explore Service: {prem_app_html} {html.escape(str(app_full_name))}</b>\n\nSelect a country to view available ranges:"), reply_markup={"inline_keyboard": kb})
        answer_callback(call["id"])

    elif data.startswith("exp_c_"):
        country_token = data[len("exp_c_"):]
        resolved_country = _get_callback_payload(
            country_token, "explore_country"
        )
        if resolved_country:
            srv_query, iso_query = resolved_country
        elif _is_short_callback_token(country_token):
            answer_callback(call["id"], "এটি আর নেই", show_alert=True)
            return
        else:
            # Legacy form stores the ISO last; service names may contain '_'.
            srv_query, separator, iso_query = country_token.rpartition("_")
            if not separator:
                answer_callback(call["id"], "এটি আর নেই", show_alert=True)
                return
        
        nums = []
        current_time = time.time()
        with data_lock:
            for t in recent_traffic:
                if current_time - t.get("time", 0) <= 3600:
                    if t.get("service", "").startswith(srv_query) and t.get("iso") == iso_query:
                        num = t.get("real_range", t.get("number", "").replace("X", "").replace("x", "")).replace("+", "").strip()
                        if num:
                            nums.append(num)
        
        if not nums:
            answer_callback(call["id"], "❌ No recent numbers found for this country!", show_alert=True)
            return
            
        r_counts = Counter()
        for num in nums:
            if len(num) >= 7:
                r_counts[num[:7]] += 1
            else:
                r_counts[num] += 1
                    
        r_list = r_counts.most_common(12)
        
        kb = []
        temp_row = []
        for r, count in r_list:
            clean_r = str(r).replace("X", "").replace("x", "")
            range_token = _store_callback_payload(
                "search_range", clean_r, srv_query
            )
            temp_row.append({"text": f"{clean_r} ({count})", "icon_custom_emoji_id": "5352862640592949843", "callback_data": f"c_n_s_{range_token}", "style": "primary"})
            if len(temp_row) == 2:
                kb.append(temp_row)
                temp_row = []
        if temp_row:
            kb.append(temp_row)
            
        back_service_token = _store_callback_payload(
            "explore_service", srv_query
        )
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"exp_rng_{back_service_token}", "style": "danger"}])
        
        app_full_name, prem_app_html = get_service_info_html(srv_query)
        prem_flag_html = get_flag_info_html(iso_query)
        
        edit_message(chat_id, msg_id, render_body_text(f"📊 <b>Ranges for {prem_app_html} {html.escape(str(app_full_name))} - {prem_flag_html} {html.escape(str(iso_query))}</b>\n\nClick on any range to copy it."), reply_markup={"inline_keyboard": kb})
        answer_callback(call["id"])

    # User Management Flows Integration
    elif data == "user_management":
        edit_message(chat_id, msg_id, get_user_management_text(), reply_markup=user_management_keyboard())

    elif data == "um_manage_balance":
        safe_set_user_state(chat_id, "wait_for_um_bal_uid")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID to Manage Balance:"), reply_markup=get_cancel_kb())
        
    elif data == "um_ban_unban":
        safe_set_user_state(chat_id, "wait_for_um_ban_uid")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID to Ban or Unban:"), reply_markup=get_cancel_kb())

    elif data == "um_user_profile":
        safe_set_user_state(chat_id, "wait_for_um_prof_uid")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID to View Profile:"), reply_markup=get_cancel_kb())

    # Menu Design Integration
    elif data == "menu_design_list":
        edit_message(chat_id, msg_id, render_body_text("🎨 <b>Menu Design Editor</b>\n\nSelect a menu block to edit its Body Text and Inline Buttons. You can use Premium Emojis too!"), reply_markup=menu_design_list_keyboard())

    elif data == "md_reset_defaults":
        with data_lock:
            bot_settings["custom_messages"] = copy.deepcopy(DEFAULT_CUSTOM_MESSAGES)
            save_db()
        answer_callback(call["id"], "✅ Resetted to Premium Defaults!", show_alert=True)

    elif data.startswith("md_edit_"):
        answer_callback(call["id"])
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        key = data.replace("md_edit_", "")
        cm_text = render_body_text(bot_settings["custom_messages"].get(key, {}).get("text", "..."))
        try:
            edit_message(chat_id, msg_id, render_body_text(f"🎨 <b>Editing: {key.upper()}</b>\n\nPreview of current Text:\n{cm_text}"), reply_markup=menu_edit_options_keyboard(key))
        except Exception as e:
            logger.error(f"Error editing menu: {e}")

    elif data.startswith("md_text_"):
        key = data.replace("md_text_", "")
        safe_set_user_state(chat_id, "wait_for_menu_text")
        temp = {"msg_id": msg_id, "menu_key": key}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text(f"📝 <b>Edit Body: {key.upper()}</b>\n\nSend the new text. You can use Premium Emojis directly here.\n(Use standard HTML like <b>bold</b>, <i>italic</i> for formatting)"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"md_edit_{key}", "style": "danger"}]]})

    elif data.startswith("md_btns_"):
        answer_callback(call["id"])
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        key = data.replace("md_btns_", "")
        try:
            edit_message(chat_id, msg_id, render_body_text(f"⚙️ <b>Edit Inline Buttons: {key.upper()}</b>"), reply_markup=menu_buttons_list_keyboard(key))
        except Exception as e:
            logger.error(f"Error editing buttons: {e}")

    elif data.startswith("md_addbtn_"):
        key = data.replace("md_addbtn_", "")
        safe_set_user_state(chat_id, "wait_for_menu_btn")
        temp = {"msg_id": msg_id, "menu_key": key}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text(f"➕ <b>Add Button: {key.upper()}</b>\n\nSend custom button in this format:\n<code>Button Text - https://link.com</code>\n\n<i>(Only normal Emojis supported here!)</i>"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"md_btns_{key}", "style": "danger"}]]})

    elif data.startswith("md_delbtn_"):
        payload = data[len("md_delbtn_"):]
        try:
            key, raw_button_index = payload.rsplit("_", 1)
        except ValueError:
            _callback_missing(call)
            return
        b_idx = _safe_custom_button_index(call, key, raw_button_index)
        if b_idx is None:
            return
        with data_lock:
            del bot_settings["custom_messages"][key]["buttons"][b_idx]
            save_db()
        answer_callback(call["id"], "✅ Button Deleted!", show_alert=True)
        edit_message(chat_id, msg_id, render_body_text(f"⚙️ <b>Edit Inline Buttons: {key.upper()}</b>"), reply_markup=menu_buttons_list_keyboard(key))

    elif data.startswith("sel_wm_"):
        method = data.replace("sel_wm_", "")
        bal = get_user(chat_id).get('balance', 0.0)
        min_w = bot_settings['min_withdraw']
        
        if bal + 1e-6 < min_w:
            answer_callback(call["id"], f"❌ Insufficient balance. Minimum required: {fmt_money(min_w)} TK.", show_alert=True)
            return
            
        temp = {"method": method, "balance": bal, "msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        safe_set_user_state(chat_id, "wait_for_withdraw_amount")
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['ok']} Method: {html.escape(str(method))}\n💰 Available Balance: {fmt_money(bal)} ৳\n\n📝 Enter the amount you want to withdraw (Min: {fmt_money(min_w)} ৳):"), reply_markup=get_cancel_kb())
        answer_callback(call["id"])

    elif data == "test_message_flow":
        safe_set_user_state(chat_id, "wait_for_test_service")
        temp = {}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("🧪 <b>Test Mode</b>\n\n📝 Send the Service Name (e.g., IG):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]]})

    elif data == "manage_emojis":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['star']} <b>Premium Emoji Management</b>\n\nUpload your TXT files or manually add them below:"), reply_markup=emoji_settings_keyboard())

    elif data == "up_flags_txt":
        safe_set_user_state(chat_id, "wait_for_flag_txt")
        safe_set_temp_data(chat_id, {"msg_id": msg_id})
        edit_message(chat_id, msg_id, render_body_text("📂 Please upload the <b>Flag Emojis</b> <code>.txt</code> file."), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_emojis", "style": "danger"}]]})

    elif data == "up_apps_txt":
        safe_set_user_state(chat_id, "wait_for_app_txt")
        safe_set_temp_data(chat_id, {"msg_id": msg_id})
        edit_message(chat_id, msg_id, render_body_text("📂 Please upload the <b>Service Apps</b> <code>.txt</code> file."), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_emojis", "style": "danger"}]]})

    elif data == "add_single_emoji":
        safe_set_user_state(chat_id, "wait_for_emoji_extract")
        edit_message(chat_id, msg_id, render_body_text("📝 Send a premium emoji (for example: 🇧🇩 or 🚫):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_emojis", "style": "danger"}]]})

    elif data == "dl_flags_txt":
        content = generate_emoji_txt("flags")
        if content:
            send_document(chat_id, "Flag_Emojis.txt", content)
            answer_callback(call["id"], "✅ Downloaded!")
        else:
            answer_callback(call["id"], "❌ No Flag Emojis found!", show_alert=True)

    elif data == "dl_apps_txt":
        content = generate_emoji_txt("apps")
        if content:
            send_document(chat_id, "Service_Apps.txt", content)
            answer_callback(call["id"], "✅ Downloaded!")
        else:
            answer_callback(call["id"], "❌ No App Emojis found!", show_alert=True)

    elif data == "del_all_flags":
        with data_lock:
            bot_settings["premium_flags"] = {}
            save_db()
        answer_callback(call["id"], "✅ All Premium Flags Deleted Successfully!", show_alert=True)

    elif data == "broadcast_msg":
        safe_set_user_state(chat_id, "wait_for_broadcast")
        edit_message(chat_id, msg_id, render_body_text("📢 <b>Broadcast Mode</b>\n\nSend the message you want to broadcast (Text, Photo, Video, File etc)."), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]})

    elif data == "upload_num":
        safe_set_temp_data(chat_id, None)
        safe_set_user_state(chat_id, "wait_for_txt")
        safe_set_temp_data(
            chat_id,
            {"msg_id": msg_id, "upload_id": uuid.uuid4().hex},
        )
        edit_message(chat_id, msg_id, render_body_text("📂 Please upload the numbers in a <b>.txt</b> file."), reply_markup={"inline_keyboard": [[{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]})

    elif data == "delete_files":
        kb = []
        with data_lock:
            for b_id, b_data in number_batches.items():
                kb.append([{"text": f"{b_data['filename']} ({len(b_data['numbers'])})", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"del_b_{b_id}", "style": "danger"}])
            service_names = set()
            service_names.update(
                b_data.get("service", "")
                for b_data in number_batches.values()
                if b_data.get("service")
            )
            for service_name in sorted(service_names, key=lambda item: normalize_service_name(item)):
                kb.append([{
                    "text": f"Remove service: {service_name}",
                    "icon_custom_emoji_id": "5422557736330106570",
                    "callback_data": f"del_service_{service_name}",
                    "style": "danger"
                }])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "primary"}])
        txt = "🗑 Select a file to delete or remove a complete service:" if len(kb) > 1 else f"{PEM['no']} No files or services found."
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif data.startswith("del_b_"):
        b_id = data.split("del_b_")[1]
        with data_lock:
            if b_id in number_batches:
                del number_batches[b_id]
                dirty_number_batches.discard(b_id)
                deleted_number_batches.add(b_id)
                _rebuild_known_numbers()  # FIX: remove deleted numbers from known set
                save_db(include_local=True)
        answer_callback(call["id"], "✅ File deleted!", show_alert=True)
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "delete_files", "id": call["id"]})

    elif data.startswith("del_service_"):
        service_name = data[len("del_service_"):]
        remove_service_everywhere(service_name)
        answer_callback(
            call["id"],
            f"✅ {service_name} removed from all service sources!",
            show_alert=True
        )
        handle_callback({
            "message": {"chat": {"id": chat_id}, "message_id": msg_id},
            "data": "delete_files",
            "id": call["id"]
        })

    elif data == "show_used":
        kb = {"inline_keyboard": [[{"text": "Download TXT", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_used", "style": "primary"}], [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]}
        with data_lock:
            used_count = len(used_numbers_list)
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['ok']} <b>Total Used Numbers:</b> {used_count}"), reply_markup=kb)

    elif data == "show_unused":
        with data_lock:
            unused_count = sum(len(b["numbers"]) for b in number_batches.values())
        kb = {"inline_keyboard": [[{"text": "Download TXT", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_unused", "style": "primary"}], [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]}
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['rocket']} <b>Total Unused Numbers:</b> {unused_count}"), reply_markup=kb)

    elif data == "dl_used":
        with data_lock:
            content = "\n".join(used_numbers_list).encode('utf-8') if used_numbers_list else None
        if content is None:
            answer_callback(call["id"], "❌ No used numbers found!", show_alert=True)
            return
        send_document(chat_id, "used_numbers.txt", content)
        answer_callback(call["id"])

    elif data == "dl_unused":
        with data_lock:
            unused_list = [n["num"] for b in number_batches.values() for n in b["numbers"]]
            content = "\n".join(unused_list).encode('utf-8') if unused_list else None
        if content is None:
            answer_callback(call["id"], "❌ No unused numbers found!", show_alert=True)
            return
        send_document(chat_id, "unused_numbers.txt", content)
        answer_callback(call["id"])

    elif data == "lb_main":
        txt = f"{PEM['admin']} <b>LEADER BOARD MENU</b>\n{ANIMATED_BANNER}\n<i>Select a category to view the top performers or history.</i>\n{ANIMATED_BANNER}"
        kb = [
            [{"text": "Top Referrers", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "lb_top_refs", "style": "primary"}],
            [{"text": "Top OTP Receivers", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "lb_top_otps", "style": "primary"}],
            [{"text": "Withdrawal History", "icon_custom_emoji_id": "5348469219761626211", "callback_data": "lb_w_history", "style": "success"}],
            [{"text": "Back to Admin", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]
        ]
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif data.startswith("lb_"):
        sub = data.replace("lb_", "")
        edit_message(chat_id, msg_id, render_body_text("⌛ <i>Fetching Data...</i>"))
        
        num_map = {"1": "1️⃣", "2": "2️⃣", "3": "3️⃣", "4": "4️⃣", "5": "5️⃣", "6": "6️⃣", "7": "7️⃣", "8": "8️⃣", "9": "9️⃣", "0": "0️⃣"}
        def get_p_num(n):
            return "".join([num_map.get(c, c) for c in str(n)])
        
        try:
            conn = None if db else get_db()
            try:
                cursor = conn.cursor() if conn else None
            
                if sub == "top_refs":
                    title, field, limit, icon = "TOP 10 REFERRERS", "total_refers", 10, PEM.get('user', '👥')
                    if db:
                        users = [
                            (doc.id, (doc.to_dict() or {}).get(field, 0), (doc.to_dict() or {}).get("balance", 0))
                            for doc in db.collection("users")
                            .order_by(field, direction="DESCENDING")
                            .limit(limit)
                            .stream(timeout=5)
                        ]
                    else:
                        cursor.execute('SELECT user_id, total_refers, balance FROM users ORDER BY total_refers DESC LIMIT ?', (int(limit),))
                        users = [(row['user_id'], row['total_refers'], row['balance']) for row in cursor.fetchall()]
                    res_txt = ""
                    count = 1
                    for uid, total, bal in users:
                        if total > 0:
                            p = "└" if count == limit else "├"
                            res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={uid}'>{uid}</a> ➔ Refs: <b>{total}</b> | Bal: <b>{fmt_money(bal)}৳</b>\n"
                            count += 1
                    if not res_txt:
                        res_txt = "└ <i>No data found.</i>\n"

                elif sub == "top_otps":
                    title, field, limit, icon = "TOP 10 OTP RECEIVERS", "total_otps", 10, PEM.get('msg', '📩')
                    if db:
                        users = [
                            (doc.id, (doc.to_dict() or {}).get(field, 0), (doc.to_dict() or {}).get("balance", 0))
                            for doc in db.collection("users")
                            .order_by(field, direction="DESCENDING")
                            .limit(limit)
                            .stream(timeout=5)
                        ]
                    else:
                        cursor.execute('SELECT user_id, total_otps, balance FROM users ORDER BY total_otps DESC LIMIT ?', (int(limit),))
                        users = [(row['user_id'], row['total_otps'], row['balance']) for row in cursor.fetchall()]
                    res_txt = ""
                    count = 1
                    for uid, total, bal in users:
                        if total > 0:
                            p = "└" if count == limit else "├"
                            res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={uid}'>{uid}</a> ➔ OTPs: <b>{total}</b> | Bal: <b>{fmt_money(bal)}৳</b>\n"
                            count += 1
                    if not res_txt:
                        res_txt = "└ <i>No data found.</i>\n"

                elif sub == "w_history":
                    title, limit, icon = "LAST 10 WITHDRAWALS", 10, PEM.get('money', '💸')
                    if db:
                        withdrawals = [
                            doc.to_dict() or {}
                            for doc in db.collection("withdrawals")
                            .order_by("timestamp", direction="DESCENDING")
                            .limit(limit)
                            .stream(timeout=5)
                        ]
                    else:
                        cursor.execute('SELECT user_id, amount, status FROM withdrawals ORDER BY timestamp DESC LIMIT ?', (limit,))
                        withdrawals = [dict(row) for row in cursor.fetchall()]
                    res_txt = ""
                    count = 1
                    for withdrawal in withdrawals:
                        s = str(withdrawal.get('status', 'pending')).lower()
                        stat_icon = PEM.get('ok','✅') if s in ["approved","success"] else PEM.get('no','❌') if s=="rejected" else "⏳"
                        uid = withdrawal.get('user_id', 'User')
                        p = "└" if count == limit else "├"
                        res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={uid}'>{uid}</a> ➔ <b>{fmt_money(withdrawal.get('amount', 0))}৳</b> {stat_icon}\n"
                        count += 1
                    if not res_txt:
                        res_txt = "└ <i>No history found.</i>\n"
            
            finally:
                if conn:
                    conn.close()

            final_msg = f"{ANIMATED_BANNER}\n{icon} <b>{title}</b>\n{ANIMATED_BANNER}\n{res_txt}{ANIMATED_BANNER}"
            kb = [[{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": data, "style": "success"}, {"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "lb_main", "style": "danger"}]]
            edit_message(chat_id, msg_id, render_body_text(final_msg), reply_markup={"inline_keyboard": kb})

        except Exception as e:
            logger.exception("Leaderboard error: %s", e)
            edit_message(chat_id, msg_id, render_body_text("❌ Could not load data. Please try again."), reply_markup={"inline_keyboard": [[{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "lb_main", "style": "danger"}]]})

    elif data == "system_status":
        edit_message(
            chat_id,
            msg_id,
            system_status_text(),
            reply_markup={"inline_keyboard": [[{
                "text": "Back",
                "icon_custom_emoji_id": "5267490665117275176",
                "callback_data": "back_to_admin",
                "style": "danger",
            }]]},
        )
        answer_callback(call["id"])

    elif data == "download_backup":
        try:
            backup_path = create_database_backup()
            with open(backup_path, "rb") as backup_file:
                result = send_document(
                    chat_id,
                    os.path.basename(backup_path),
                    backup_file.read(),
                )
            if result.get("ok"):
                answer_callback(call["id"], "✅ Backup sent.")
            else:
                logger.error(
                    "Could not send database backup to admin %s: %s",
                    chat_id,
                    result.get("description", "Telegram sendDocument failed"),
                )
                answer_callback(
                    call["id"],
                    "❌ Backup created, but Telegram could not send it.",
                    show_alert=True,
                )
        except Exception:
            logger.exception("Database backup download failed for admin %s", chat_id)
            answer_callback(
                call["id"],
                "❌ Could not create or send the database backup.",
                show_alert=True,
            )

    elif data == "back_to_admin":
        safe_set_user_state(chat_id, None)
        edit_message(chat_id, msg_id, get_admin_text(), reply_markup=admin_panel_keyboard())
        
    elif data == "system_settings":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['gear']} <b>System Settings</b>\nManage advanced bot configurations below:"), reply_markup=system_settings_keyboard())

    elif data == "manage_fj":
        safe_set_user_state(chat_id, None)
        safe_set_temp_data(chat_id, None)
        edit_message(
            chat_id,
            msg_id,
            fj_settings_text(),
            reply_markup=fj_settings_keyboard(),
        )

    elif data == "toggle_fj":
        with data_lock:
            bot_settings["fj_on"] = not bot_settings["fj_on"]
            save_db()
        invalidate_force_join_cache()
        edit_message(
            chat_id,
            msg_id,
            fj_settings_text(),
            reply_markup=fj_settings_keyboard(),
        )

    elif data == "add_fj":
        safe_set_user_state(chat_id, "wait_for_add_fj")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(
            chat_id,
            msg_id,
            render_body_text(
                "📝 Send a public channel @username or t.me link. "
                "For a private channel, send its ID and invite link like "
                "<code>-100… | https://t.me/+…</code>."
            ),
            reply_markup={"inline_keyboard": [[{
                "text": "Cancel",
                "icon_custom_emoji_id": "5267490665117275176",
                "callback_data": "manage_fj",
                "style": "danger",
            }]]},
        )

    elif data.startswith("set_fj_invite_"):
        try:
            channel_index = int(data.rsplit("_", 1)[1])
        except (IndexError, TypeError, ValueError):
            _callback_missing(call)
            return
        with data_lock:
            channels = bot_settings.get("fj_channels", [])
            valid_index = 0 <= channel_index < len(channels)
            channel_title = (
                normalize_force_join_channel(channels[channel_index]).get("title")
                if valid_index
                else None
            )
        if not valid_index:
            _callback_missing(call)
            return
        safe_set_user_state(chat_id, "wait_for_fj_invite")
        safe_set_temp_data(
            chat_id,
            {"msg_id": msg_id, "channel_index": channel_index},
        )
        edit_message(
            chat_id,
            msg_id,
            render_body_text(
                "📝 Send the private channel's HTTPS invite link for "
                f"<b>{html.escape(str(channel_title or 'this channel'))}</b>."
            ),
            reply_markup={"inline_keyboard": [[{
                "text": "Cancel",
                "icon_custom_emoji_id": "5267490665117275176",
                "callback_data": "manage_fj",
                "style": "danger",
            }]]},
        )

    elif data == "test_fj":
        if not is_admin(chat_id):
            answer_callback(call["id"], "Admin access required.", show_alert=True)
            return
        answer_callback(call["id"])
        diagnostic_text = force_join_diagnostic_text(chat_id)
        diagnostic_lines = diagnostic_text.splitlines()
        chunks = []
        current = ""
        for line in diagnostic_lines:
            if current and len(current) + len(line) + 1 > 3600:
                chunks.append(current)
                current = ""
            current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)
        back_keyboard = {"inline_keyboard": [[{
            "text": "Back to Force Join",
            "icon_custom_emoji_id": "5267490665117275176",
            "callback_data": "manage_fj",
            "style": "primary",
        }]]}
        if chunks:
            edit_message(
                chat_id,
                msg_id,
                chunks[0],
                reply_markup=back_keyboard,
            )
            for chunk in chunks[1:]:
                send_message(chat_id, chunk)

    elif data.startswith("del_fj_"):
        idx = _safe_callback_index(call, data, 2, "fj_channels")
        if idx is None:
            return
        with data_lock:
            del bot_settings["fj_channels"][idx]
            save_db()
        invalidate_force_join_cache()
        answer_callback(call["id"], "✅ Channel deleted!", show_alert=True)
        edit_message(
            chat_id,
            msg_id,
            fj_settings_text(),
            reply_markup=fj_settings_keyboard(),
        )

    elif data == "manage_admins":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['user']} <b>ADMIN MANAGEMENT</b>\nManage your bot admins below:"), reply_markup=admin_settings_keyboard())

    elif data == "add_adm":
        safe_set_user_state(chat_id, "wait_for_add_adm")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID of the new Admin:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_admins", "style": "danger"}]]})

    elif data.startswith("del_adm_"):
        idx = _safe_callback_index(call, data, 2, "admins")
        if idx is None:
            return
        with data_lock:
            del bot_settings["admins"][idx]
            save_db()
        answer_callback(call["id"], "✅ Admin deleted!", show_alert=True)
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['user']} <b>ADMIN MANAGEMENT</b>\nManage your bot admins below:"), reply_markup=admin_settings_keyboard())

    elif data.startswith("oib_"):
        if not is_admin(chat_id):
            answer_callback(call["id"], "Admin access required.", show_alert=True)
            return
        handle_oib_callback(call, data, chat_id, msg_id)

    elif data == "manage_otp_groups":
        edit_message(chat_id, msg_id, render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())

    elif data == "add_fw":
        safe_set_user_state(chat_id, "wait_for_add_fw_id")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Group ID/Username to forward messages to:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})

    elif data.startswith("manage_fw_"):
        idx = _safe_callback_index(call, data, 2, "fw_groups")
        if idx is None:
            return
        with data_lock:
            grp_id = bot_settings["fw_groups"][idx].get("chat_id", "")
        edit_message(chat_id, msg_id, render_body_text(f"🛡 <b>Manage Group:</b> {grp_id}"), reply_markup=specific_fw_group_keyboard(idx))

    elif data.startswith("add_fwbtn_"):
        idx = _safe_callback_index(call, data, 2, "fw_groups")
        if idx is None:
            return
        safe_set_user_state(chat_id, "wait_for_add_fw_btn")
        temp = {"msg_id": msg_id, "fw_idx": idx}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send Custom Inline Button format:\n<code>Button Text - https://link.com</code>"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"manage_fw_{idx}", "style": "danger"}]]})

    elif data.startswith("del_fwbtn_"):
        indices = _safe_nested_callback_index(
            call, data, 2, "fw_groups", 3, "buttons"
        )
        if indices is None:
            return
        idx, b_idx = indices
        with data_lock:
            del bot_settings["fw_groups"][idx]["buttons"][b_idx]
            save_db()
            group_chat_id = bot_settings["fw_groups"][idx].get("chat_id", "")
        answer_callback(call["id"], "✅ Button deleted!", show_alert=True)
        edit_message(chat_id, msg_id, render_body_text(f"🛡 <b>Manage Group:</b> {group_chat_id}"), reply_markup=specific_fw_group_keyboard(idx))

    elif data.startswith("del_fw_"):
        idx = _safe_callback_index(call, data, 2, "fw_groups")
        if idx is None:
            return
        with data_lock:
            del bot_settings["fw_groups"][idx]
            save_db()
        answer_callback(call["id"], "✅ Group deleted!", show_alert=True)
        edit_message(chat_id, msg_id, render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())

    elif data == "edit_otp_link":
        safe_set_user_state(chat_id, "wait_for_otp_link")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the new OTP Group Link:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})

    elif data == "manage_panels":
        api_count = len([p for p in bot_settings["panels"] if p.get("type") == "API Panel"])
        cpt_count = len([p for p in bot_settings["panels"] if p.get("type", "API Panel") == "Auto Captcha Panel"])
        text = f"{PEM['gear']} <b>Panel Management</b>\n\nSelect which type of panel system you want to manage:"
        kb = {"inline_keyboard": [
            [{"text": f"Manage API Panels ({api_count})", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "manage_api_panels", "style": "primary"}],
            [{"text": f"Manage Auto Captcha Panels ({cpt_count})", "icon_custom_emoji_id": "5353022963132174959", "callback_data": "manage_cpt_panels", "style": "success"}],
            [{"text": "Back to System", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]
        ]}
        edit_message(chat_id, msg_id, render_body_text(text), reply_markup=kb)

    elif data in ["manage_api_panels", "manage_cpt_panels"]:
        p_type = "API Panel" if data == "manage_api_panels" else "Auto Captcha Panel"
        p_list = [p for p in bot_settings["panels"] if p.get("type", "API Panel") == p_type]
        icon = f"{PEM['world']} API" if p_type == 'API Panel' else f"{PEM['lock']} Auto Captcha"
        
        text = f"{icon} <b>{p_type}s Management</b>\n\n👀 <b>Active Monitors:</b> {len(p_list)}\n\n🟢 <b>Available Providers:</b>\n"
        for p in p_list:
            status = "Monitoring" if p['status'] == 'ON' else "Stopped"
            login_state = p.get('login_status', '')
            if p['type'] == 'Auto Captcha Panel':
                conf = f" {login_state}" if login_state else f"{PEM['ok']} Configured"
            else:
                conf = f"{PEM['ok']} Configured" if p.get('api_url') else f"{PEM['no']} Not Configured"
            text += f"• {p['name']}: {PEM['ok'] if p['status']=='ON' else PEM['no']} {status} | {conf}\n"
        edit_message(chat_id, msg_id, render_body_text(text), reply_markup=typed_panels_list_keyboard(p_type))

    elif data in ["add_api_panel", "add_cpt_panel"]:
        safe_set_user_state(chat_id, "wait_for_panel_name")
        p_type = "api" if data == "add_api_panel" else "logc"
        temp = {"msg_id": msg_id, "add_type": p_type}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Please send the name of the New Provider:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"manage_{'api' if p_type=='api' else 'cpt'}_panels", "style": "danger"}]]})

    elif data in ["list_del_api", "list_del_cpt"]:
        p_type = "API Panel" if data == "list_del_api" else "Auto Captcha Panel"
        kb = []
        for idx, p in enumerate(bot_settings["panels"]):
            if p.get("type", "API Panel") == p_type:
                kb.append([{"text": f"Delete {p['name']}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"do_del_pnl_{idx}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"manage_{'api' if p_type=='API Panel' else 'cpt'}_panels", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['trash']} <b>Select a Provider to Delete:</b>"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("do_del_pnl_"):
        idx = _safe_callback_index(call, data, 3, "panels")
        if idx is None:
            return
        p_type = "API Panel"
        deleted_monitor_id = None
        with data_lock:
            panel_to_delete = bot_settings["panels"][idx]
            p_type = panel_to_delete.get("type", "API Panel")
            deleted_monitor_id = str(
                panel_to_delete.get("_monitor_id") or uuid.uuid4().hex
            )
            panel_to_delete["_monitor_id"] = deleted_monitor_id
            _deleted_panel_monitor_ids.add(deleted_monitor_id)
            del bot_settings["panels"][idx]
            # panel_sessions / baseline set are keyed by list index: re-key them
            panel_sessions.pop(idx, None)
            for old_idx in sorted(k for k in list(panel_sessions) if k > idx):
                panel_sessions[old_idx - 1] = panel_sessions.pop(old_idx)
            shifted = {(i - 1 if i > idx else i) for i in otp_baselined_panels if i != idx}
            otp_baselined_panels.clear()
            otp_baselined_panels.update(shifted)
            save_db()
        with panel_inflight_lock:
            stale_future = panel_inflight.pop(deleted_monitor_id, None)
            if stale_future:
                stale_future.cancel()
        with data_lock:
            panel_fetch_state.pop(deleted_monitor_id, None)
        answer_callback(call["id"], "✅ Provider Deleted!", show_alert=True)
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"manage_{'api' if p_type=='API Panel' else 'cpt'}_panels", "id": "internal"})

    elif data.startswith("tog_pnl_"):
        idx = _safe_callback_index(call, data, 2, "panels")
        if idx is None:
            return
        with data_lock:
            p = bot_settings["panels"][idx]
            p["status"] = "ON" if p.get("status", "ON") == "OFF" else "OFF"
            p = dict(p)  # snapshot; rendering/network happen outside the lock
            save_db()

        if p["type"] == "Auto Captcha Panel":
            text = f"⚙️ <b>Configure {html.escape(str(p['name']))}</b>\n\n<b>Type:</b> {html.escape(str(p['type']))}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>Login Status:</b> {html.escape(str(p.get('login_status', 'Unknown')))}\n<b>Login URL:</b> <code>{html.escape(str(p.get('login_url', 'None')))}</code>\n<b>User:</b> <code>{html.escape(str(p.get('username', 'None')))}</code>"
        else:
            text = panel_api_configuration_text(p)
        edit_message(chat_id, msg_id, render_body_text(text), reply_markup=panel_config_keyboard(idx))

    elif data.startswith("conf_pnl_"):
        idx = _safe_callback_index(call, data, 2, "panels")
        if idx is None:
            return
        with data_lock:
            p = copy.deepcopy(bot_settings["panels"][idx])
        if p is not None:
            if p.get("type") == "Auto Captcha Panel":
                text = f"⚙️ <b>Configure {html.escape(str(p['name']))}</b>\n\n<b>Type:</b> {html.escape(str(p['type']))}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>Login Status:</b> {html.escape(str(p.get('login_status', 'Unknown')))}\n<b>Login URL:</b> <code>{html.escape(str(p.get('login_url', 'None')))}</code>\n<b>User:</b> <code>{html.escape(str(p.get('username', 'None')))}</code>\n<b>Num Col:</b> {html.escape(str(p.get('num_col_name')))} (Idx: {p.get('num_col_idx')})\n<b>Msg Col:</b> {html.escape(str(p.get('msg_col_name')))} (Idx: {p.get('msg_col_idx')})"
            else:
                text = panel_api_configuration_text(p)
            edit_message(chat_id, msg_id, render_body_text(text), reply_markup=panel_config_keyboard(idx))

    elif data.startswith("set_p_api_"):
        idx = _safe_callback_index(call, data, 3, "panels")
        if idx is None:
            return
        safe_set_user_state(chat_id, "wait_for_p_api")
        temp = {"msg_id": msg_id, "p_idx": idx}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the API URL for this provider:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("set_p_tok_"):
        idx = _safe_callback_index(call, data, 3, "panels")
        if idx is None:
            return
        safe_set_user_state(chat_id, "wait_for_p_tok")
        temp = {"msg_id": msg_id, "p_idx": idx}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Token for this provider:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("set_p_fapi_"):
        idx = _safe_callback_index(call, data, 3, "panels")
        if idx is None:
            return
        safe_set_user_state(chat_id, "wait_for_p_fapi")
        temp = {"msg_id": msg_id, "p_idx": idx}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the FULL API URL (Example: http://api.com/get?key=YOUR_TOKEN&start=0):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("set_p_rec_"):
        idx = _safe_callback_index(call, data, 3, "panels")
        if idx is None:
            return
        safe_set_user_state(chat_id, "wait_for_p_rec")
        temp = {"msg_id": msg_id, "p_idx": idx}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the number of records to fetch (e.g. 10).\nType <code>0</code> for Unlimited:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("test_p_conn_"):
        idx = _safe_callback_index(call, data, 3, "panels")
        if idx is None:
            return
        with data_lock:
            p = copy.deepcopy(bot_settings["panels"][idx])
        wait_msg = send_message(chat_id, render_body_text("⏳ Testing connection. Please wait..."))
        wait_msg_id = wait_msg.get("result", {}).get("message_id") if wait_msg else None
        answer_callback(call["id"])
        
        try:
            parsed = []
            raw_text = ""
            fetch_result = None
            
            if p["type"] == "Auto Captcha Panel":
                sess = panel_sessions.get(idx)
                if not sess:
                    success = attempt_auto_login(p, idx)
                    if not success:
                        if wait_msg_id:
                            delete_message(chat_id, wait_msg_id)
                        send_message(chat_id, render_body_text(f"❌ <b>Auto Login Failed!</b>\nReason: {html.escape(str(p.get('login_status', 'Unknown')))}"))
                        return
                    sess = panel_sessions.get(idx)
                    
                login_url = p.get("login_url", "").strip()
                if not login_url.startswith("http"):
                    login_url = "http://" + login_url
                msg_link = p.get("msg_link", "").strip()
                if not msg_link.startswith("http") and msg_link != "":
                    msg_link = "http://" + msg_link
                check_url = msg_link if msg_link else f"{login_url.split('/login')[0]}/client/SMSCDRStats"
                
                parsed, raw_text = fetch_cpt_panel_cdrs(p, sess, check_url)
                
            else:
                if not (p.get("full_api_url", "").strip() or p.get("api_url", "").strip()):
                    if wait_msg_id:
                        delete_message(chat_id, wait_msg_id)
                    send_message(chat_id, render_body_text("❌ Please Set API URL or Full API URL first!"))
                    return

                # same shared helper the monitor thread uses (URL fallbacks, pagination, diagnostics)
                fetch_result = fetch_api_panel(p, idx=idx, verbose=True)
                parsed = fetch_result["items"]
                raw_text = fetch_result.get("raw_text", "")
                if fetch_result.get("working_template"):
                    with data_lock:
                        p["api_url"] = fetch_result["working_template"]
                        save_db()

            if wait_msg_id:
                delete_message(chat_id, wait_msg_id)
                 
            if parsed:
                if fetch_result is not None:
                    shown_items, limit_info = apply_records_limit(parsed, p.get("records", 0))
                    summary = build_panel_test_summary(p, fetch_result, limit_info)
                else:
                    shown_items = parsed
                    summary = build_panel_test_summary(
                        p, {"ok": True, "format": "html", "pages": 1, "parsed": len(parsed),
                            "rows": None, "attempts": []}, {})
                txt = summary + "\n\n🎯 <b>Parsed Data Sample (Max 3):</b>\n\n"
                
                for i, sample in enumerate(shown_items[:3]):
                    num = sample['number']
                    msg = sample['message']
                    otp = sample['otp']
                    
                    app_full_name, prem_app_html = get_service_info_html(
                        p.get("name", "Panel"), msg,
                        service_hint=sample.get("service_hint"), panel_mode=True,
                    )
                    
                    txt += f"<b>{i+1}.</b> {prem_app_html} <b>{html.escape(str(app_full_name))}</b>\n"
                    txt += f"📱 Number: <code>{html.escape(str(num))}</code>\n"
                    txt += f"📝 Full Msg: <code>{html.escape(msg)}</code>\n"
                    txt += f"🔐 OTP: <code>{html.escape(str(otp))}</code>\n"
                    txt += ANIMATED_BANNER + "\n"
                    
                send_message(chat_id, render_body_text(txt))
            else:
                if p["type"] == "Auto Captcha Panel":
                    try:
                        soup = BeautifulSoup(raw_text, 'html.parser')
                        tables = soup.find_all('table')
                        if tables:
                            full_table_data = "🔍 FULL TABLE DATA (A-Z)\n" + "="*50 + "\n\n"
                            for t_idx, table in enumerate(tables):
                                full_table_data += f"--- Table {t_idx+1} ---\n"
                                rows = table.find_all('tr')
                                for r_idx, row in enumerate(rows):
                                    cols = row.find_all(['th', 'td'])
                                    col_texts = [f"[{c_idx+1}] {c.get_text(separator=' ', strip=True)}" for c_idx, c in enumerate(cols)]
                                    full_table_data += f"Row {r_idx+1}: {' | '.join(col_texts)}\n"
                                full_table_data += "\n" + "="*50 + "\n"
                            
                            document_result = send_document(chat_id, f"Full_Panel_Data_{idx}.txt", full_table_data.encode('utf-8'))
                            document_message_id = (
                                document_result.get("result", {}).get("message_id")
                                if document_result
                                else None
                            )
                            close_callback = (
                                f"close_cpt_{document_message_id}"
                                if document_message_id
                                else "close_msg"
                            )
                            fail_txt = "⚠️ <b>Connected, but couldn't parse OTP data!</b>\n\n<i>Full A-Z data from the link was sent as a text file. Open it, check the correct column indexes (for example: [1], [3]), and update the panel.</i>"
                            close_kb = {"inline_keyboard": [[{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": close_callback, "style": "danger"}]]}
                            send_message(chat_id, render_body_text(fail_txt), reply_markup=close_kb)
                        else:
                            close_kb = {"inline_keyboard": [[{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]}
                            send_message(chat_id, render_body_text("⚠️ <b>Connected, but no HTML Table found!</b>\nMake sure the message link is correct."), reply_markup=close_kb)
                    except Exception as e:
                        logger.error(f"HTML parsing error: {e}")
                        close_kb = {"inline_keyboard": [[{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]}
                        send_message(chat_id, render_body_text(f"❌ <b>Error parsing HTML:</b> {html.escape(str(e))}"), reply_markup=close_kb)
                else:
                    safe_html = html.escape(mask_text(str(raw_text)[:300], p.get("token", "")))
                    close_kb = {"inline_keyboard": [[{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]}
                    report = build_panel_test_summary(p, fetch_result, {}) if fetch_result is not None else "⚠️ <b>Connected, but couldn't find/parse OTP data.</b>"
                    if safe_html:
                        report += f"\n\nRaw response (excerpt, token masked):\n<code>{safe_html}...</code>"
                    send_message(chat_id, render_body_text(report), reply_markup=close_kb)
        except Exception as e:
            logger.error(f"Connection test error: {e}")
            if wait_msg_id:
                delete_message(chat_id, wait_msg_id)
            send_message(chat_id, render_body_text(f"❌ <b>Connection Failed!</b>\nError: {html.escape(str(e))}"))

    elif data == "WAO_control":
        safe_set_user_state(chat_id, None)
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>WAO CONTROL PANEL</b>"), reply_markup=WAO_control_keyboard())

    elif data == "WAO_toggle_w":
        with data_lock:
            bot_settings["withdraw_on"] = not bot_settings["withdraw_on"]
            save_db()
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>WAO CONTROL PANEL</b>"), reply_markup=WAO_control_keyboard())

    elif data == "service_prices":
        txt = render_body_text(
            f"💲 <b>SERVICE × COUNTRY PRICE RULES</b>\n{ANIMATED_BANNER}\n"
            "Set a custom OTP reward for a specific service + country pair.\n"
            f"Format: <code>SERVICE::COUNTRY</code> (e.g. <code>WHATSAPP::BD</code>)\n"
            f"Falls back to global OTP reward when no rule matches.\n{ANIMATED_BANNER}"
        )
        edit_message(chat_id, msg_id, txt, reply_markup=service_prices_keyboard())

    elif data == "sp_add":
        safe_set_user_state(chat_id, "set_service_price")
        safe_set_temp_data(chat_id, {"msg_id": msg_id, "sp_mode": "add", "sp_key": None})
        edit_message(chat_id, msg_id,
            render_body_text(
                "📝 Send the price rule in this format:\n"
                "<code>SERVICE::COUNTRY PRICE</code>\n\n"
                "Examples:\n"
                "➥ <code>WHATSAPP::BD 0.5</code>\n"
                "➥ <code>TELEGRAM::US 1.2</code>\n\n"
                "Country = ISO code or the country label you used when uploading."
            ),
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "service_prices", "style": "danger"}]]}
        )

    elif data.startswith("sp_edit_"):
        sp_key = data[len("sp_edit_"):]
        with data_lock:
            current = fmt_money(bot_settings.get("service_prices", {}).get(sp_key, 0))
        safe_set_user_state(chat_id, "set_service_price")
        safe_set_temp_data(chat_id, {"msg_id": msg_id, "sp_mode": "edit", "sp_key": sp_key})
        edit_message(chat_id, msg_id,
            render_body_text(
                f"✏️ Editing price for <code>{html.escape(sp_key)}</code>\n"
                f"Current value: <b>{current}৳</b>\n\n"
                "Send the new price (e.g. <code>0.75</code>):"
            ),
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "service_prices", "style": "danger"}]]}
        )

    elif data.startswith("sp_del_"):
        sp_key = data[len("sp_del_"):]
        with data_lock:
            bot_settings.setdefault("service_prices", {}).pop(sp_key, None)
            save_db()
        answer_callback(call["id"], f"✅ Deleted rule: {sp_key}", show_alert=True)
        txt = render_body_text(f"💲 <b>SERVICE × COUNTRY PRICE RULES</b>\n{ANIMATED_BANNER}")
        edit_message(chat_id, msg_id, txt, reply_markup=service_prices_keyboard())

    elif data == "manage_w_methods":
        edit_message(chat_id, msg_id, render_body_text("💳 <b>WITHDRAWAL METHODS</b>\n\nManage your withdrawal methods below:"), reply_markup=w_methods_keyboard())

    elif data == "add_wm":
        safe_set_user_state(chat_id, "wait_for_add_wm")
        temp = {"msg_id": msg_id}
        safe_set_temp_data(chat_id, temp)
        edit_message(chat_id, msg_id, render_body_text("📝 Send the name of the new Withdrawal Method:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_w_methods", "style": "danger"}]]})

    elif data.startswith("del_wm_"):
        idx = _safe_callback_index(call, data, 2, "w_methods")
        if idx is None:
            return
        with data_lock:
            del bot_settings["w_methods"][idx]
            save_db()
        answer_callback(call["id"], "✅ Method deleted!", show_alert=True)
        edit_message(chat_id, msg_id, render_body_text("💳 <b>WITHDRAWAL METHODS</b>\n\nManage your withdrawal methods below:"), reply_markup=w_methods_keyboard())

    elif data.startswith("WAO_"):
        key = data.replace("WAO_", "")
        key_map = {"min_w": "min_withdraw", "otp_r": "otp_reward", "ref_r": "refer_reward", "cool": "change_number_cooldown", "num_req": "num_req", "num_share": "num_share", "sup_link": "support_link", "w_group": "w_group"}
        if key in key_map:
            temp = {"msg_id": msg_id, "key": key_map[key]}
            safe_set_temp_data(chat_id, temp)
            safe_set_user_state(chat_id, "set_WAO")
            cancel_kb = {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_WAO_edit", "style": "danger"}]]}
            edit_message(chat_id, msg_id, render_body_text(f"📝 Please send the new value for <code>{key_map[key]}</code>:"), reply_markup=cancel_kb)
            answer_callback(call["id"])

    elif data.startswith("g_s_"):
        service_token = data[len("g_s_"):]
        service_payload = _get_callback_payload(service_token, "service")
        if service_payload:
            service = service_payload[0]
        elif _is_short_callback_token(service_token):
            answer_callback(call["id"], "এটি আর নেই", show_alert=True)
            return
        else:
            service = service_token
        txt, markup = build_country_selection_screen(service)
        edit_message(chat_id, msg_id, txt, reply_markup=markup)

    elif data.startswith("back_get_country_"):
        service_token = data[len("back_get_country_"):]
        service_payload = _get_callback_payload(service_token, "service")
        if service_payload:
            service = service_payload[0]
        elif _is_short_callback_token(service_token):
            answer_callback(call["id"], "এটি আর নেই", show_alert=True)
            return
        else:
            service = service_token
        txt, markup = build_country_selection_screen(service)
        expire_previous_number(chat_id, remove_message=False)
        edit_message(chat_id, msg_id, txt, reply_markup=markup)

    elif data.startswith("g_c_") or data.startswith("c_n_"):
        is_change_request = data.startswith("c_n_")
        resolved_pair = None
        resolved_range = None
        if data.startswith("c_n_s_"):
            range_token = data[len("c_n_s_"):]
            resolved_range = _get_callback_payload(range_token, "search_range")
            if not resolved_range and _is_short_callback_token(range_token):
                answer_callback(call["id"], "এটি আর নেই", show_alert=True)
                return
        elif data.startswith("c_n_") or data.startswith("g_c_"):
            pair_token = data[4:]
            resolved_pair = _get_callback_payload(pair_token, "service_country")
            if not resolved_pair and _is_short_callback_token(pair_token):
                answer_callback(call["id"], "এটি আর নেই", show_alert=True)
                return
            if not resolved_pair:
                resolved_pair = _decode_legacy_service_country(pair_token)
                if not resolved_pair:
                    answer_callback(call["id"], "এটি আর নেই", show_alert=True)
                    return
        if is_change_request:
            cooldown_remaining = 0
            with cooldowns_lock:
                now = time.time()
                cooldown = get_change_number_cooldown()
                elapsed = now - user_cooldowns.get(chat_id, 0)
                if elapsed < cooldown:
                    cooldown_remaining = max(1, int(cooldown - elapsed + 0.999))
                else:
                    user_cooldowns[chat_id] = now
            if cooldown_remaining:
                # Network call kept outside cooldowns_lock (global lock).
                # show_alert=False keeps this as a small, self-dismissing toast.
                answer_callback(
                    call["id"],
                    f"⌛ Change again in {cooldown_remaining}s",
                    show_alert=False,
                )
                return

        # Acknowledge before provider/local-stock work starts.  Telegram then
        # stops spinning on the clicked button while the loading message is
        # being displayed and the API request is running.
        if call.get("id") != "internal":
            answer_callback(call["id"])

        # For Change Number, remove the old number menu before showing the
        # replacement loading state.  A regular GET NUMBER selection keeps
        # the normal in-place edit behavior.
        with sessions_lock:
            previous_session = user_active_sessions.get(chat_id)
            previous_msg_id = previous_session.get("msg_id") if previous_session else None

        if data.startswith("c_n_s_"):
            if resolved_range:
                query, service_from_cb = resolved_range
            else:
                parts_s = data.split("_", 4)
                query = parts_s[3] if len(parts_s) > 3 else ""
                service_from_cb = parts_s[4] if len(parts_s) > 4 else None

            if previous_session:
                expire_previous_number(chat_id, remove_message=False)

            # Edit the existing result in place so there is no delete/vanish
            # transition before the animated loading state appears.
            wait_msg_id = show_number_loading(
                chat_id,
                previous_msg_id or msg_id,
                change_number=True,
            )
            
            with data_lock:
                found_indices = []
                for b_id, b_data in number_batches.items():
                    if is_service_disabled(b_data.get("service", "")):
                        continue
                    for idx, n_obj in enumerate(b_data["numbers"]):
                        if n_obj["num"].replace("+", "").startswith(query) and chat_id not in n_obj.get("used_by", []):
                            found_indices.append((b_id, idx))
            
            fetched_nums = []
            if not found_indices:
                delete_message(chat_id, wait_msg_id)
                send_message(
                    chat_id,
                    render_body_text("❌ Number out of stock!"),
                    reply_markup=main_menu(chat_id),
                )
                return
            else:
                random.shuffle(found_indices)
                affected_batches = set()
                with data_lock:
                    for b_id, idx in found_indices:
                        if len(fetched_nums) >= get_number_limit():
                            break
                        n_obj = number_batches[b_id]["numbers"][idx]
                        dirty_number_batches.add(b_id)
                        affected_batches.add(b_id)
                        num_str = n_obj["num"]
                        fetched_nums.append(num_str)
                        usage_batch = number_batches[b_id]
                        pending_number_usage.append((
                            str(chat_id),
                            normalize_service_name(usage_batch.get("service", "")),
                            normalize_service_name(usage_batch.get("country", "")),
                            num_str,
                            time.time(),
                        ))
                        n_obj["shares"] += 1
                        n_obj["used_by"].append(chat_id)
                        total_assigned_stats += 1
                        if n_obj["shares"] >= bot_settings.get("num_share", 1):
                            n_obj["to_remove"] = True
                            used_numbers_list.append(num_str)
                            pending_used_numbers.append(num_str)
                    for b_id in affected_batches:
                        batch = number_batches.get(b_id)
                        if batch:
                            batch["numbers"] = [
                                n for n in batch["numbers"]
                                if not n.get("to_remove")
                            ]
                    save_db(include_local=True)
                
            kb = []
            if service_from_cb:
                app_full_name, _ = get_service_info_html(service_from_cb)
                emoji_id_srv = get_premium_app_icon_id(
                    service_from_cb,
                    "5337302974806922068",
                )
                kb.append([{"text": f"{app_full_name}", "icon_custom_emoji_id": emoji_id_srv, "callback_data": "ignore", "style": "success"}])

            flags_db = bot_settings.get("premium_flags", {})
            for num in fetched_nums:
                _, iso = get_flag_and_code(num)
                display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                emoji_id = "5780471598922337683"
                for flag_code, flag_data in flags_db.items():
                    if iso == flag_data.get("iso"):
                        if "id" in flag_data:
                            emoji_id = flag_data["id"]
                            break
                kb.append([{"text": f"{display_num}", "icon_custom_emoji_id": emoji_id, "copy_text": {"text": display_num}, "style": "primary"}])
            
            range_token = _store_callback_payload(
                "search_range", query, service_from_cb
            )
            kb.append([{"text": "Change Number", "icon_custom_emoji_id": CHANGE_NUMBER_BUTTON_EMOJI_ID, "callback_data": f"c_n_s_{range_token}", "style": "danger"},
                       {"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": bot_settings["otp_link"], "style": "primary"}])
            
            c_btns = bot_settings["custom_messages"].get("search_number", {}).get("buttons", [])
            for c_b in c_btns:
                b_copy = c_b.copy()
                if "style" not in b_copy:
                    b_copy["style"] = "primary"
                kb.append([b_copy])
            kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
            
            delivered_msg_id = deliver_number_menu(
                chat_id,
                wait_msg_id,
                "ㅤ\n",
                {"inline_keyboard": kb},
            )
            with sessions_lock:
                if delivered_msg_id:
                    created_at = time.time()
                    user_active_sessions[chat_id] = {
                        "msg_id": delivered_msg_id,
                        "nums": fetched_nums,
                        "created_at": created_at,
                        "expires_at": created_at + ACTIVE_USER_SESSION_TTL_SECONDS,
                    }
            return

        if resolved_pair:
            service, country = resolved_pair
        else:
            payload = data[4:]
            legacy_pair = _decode_legacy_service_country(payload)
            if legacy_pair:
                service, country = legacy_pair
            else:
                service = country = None
        if not service or not country:
            send_message(
                chat_id,
                render_body_text("❌ Invalid service or country selection!"),
                reply_markup=main_menu(chat_id),
            )
            return
        service = normalize_service_name(service)
        country = normalize_service_name(country)

        if data.startswith("c_n_") and previous_session:
            expire_previous_number(chat_id, remove_message=False)

        if data.startswith("g_c_"):
            # Country selection should open the number result directly in the
            # existing picker message.  Do not delete it or show a vanish/
            # loading animation; deliver_number_menu() will edit it in place.
            wait_msg_id = msg_id
        elif previous_msg_id:
            # Change Number reuses the existing result message for loading.
            wait_msg_id = show_number_loading(
                chat_id,
                previous_msg_id,
                change_number=True,
            )
        else:
            # Reuse the callback message even when no session is tracked.
            wait_msg_id = show_number_loading(
                chat_id,
                msg_id,
                change_number=True,
            )

        inventory_index = _get_inventory_index()
        batch_ids = inventory_index["batches_by_pair"].get(
            (
                normalize_service_name(service),
                normalize_service_name(country),
            ),
            (),
        )
        with data_lock:
            batch_snapshot = [
                (
                    batch_id,
                    tuple(number_batches.get(batch_id, {}).get("numbers", [])),
                )
                for batch_id in batch_ids
            ]
        number_limit = get_number_limit()

        def collect_candidates(snapshot):
            candidates = []
            randomized_batches = list(snapshot)
            random.shuffle(randomized_batches)
            for batch_id, numbers in randomized_batches:
                if not numbers:
                    continue
                start_offset = random.randrange(len(numbers))
                for offset in range(len(numbers)):
                    number = numbers[(start_offset + offset) % len(numbers)]
                    if (
                        not number.get("to_remove")
                        and chat_id not in number.get("used_by", [])
                    ):
                        candidates.append((batch_id, number))
                        if len(candidates) >= number_limit:
                            return candidates
            return candidates

        candidates = collect_candidates(batch_snapshot)

        if not candidates:
            # A freshly committed upload or a previously cached empty index
            # must not hide new stock from the immediately following click.
            invalidate_inventory_cache()
            inventory_index = _get_inventory_index()
            batch_ids = inventory_index["batches_by_pair"].get(
                (
                    normalize_service_name(service),
                    normalize_service_name(country),
                ),
                (),
            )
            with data_lock:
                batch_snapshot = [
                    (
                        batch_id,
                        tuple(number_batches.get(batch_id, {}).get("numbers", [])),
                    )
                    for batch_id in batch_ids
                ]
            candidates = collect_candidates(batch_snapshot)

        if not candidates:
            diagnostic = selection_stock_diagnostics(
                chat_id,
                service,
                country,
            )
            if (
                (
                    diagnostic["matching_numbers"] > 0
                    and diagnostic["live_already_used"]
                    >= diagnostic["matching_numbers"]
                )
                or (
                    diagnostic["matching_numbers"] == 0
                    and diagnostic["history_used"] > 0
                )
            ):
                reason = "user_already_used_all"
                user_message = (
                    "❌ You've already used all numbers available for this "
                    "service and country."
                )
            elif diagnostic["same_service_other_range"]:
                reason = "service_country_range_mismatch"
                user_message = (
                    "❌ No number range is configured for this service and country."
                )
            elif diagnostic["service_disabled"]:
                reason = "service_disabled"
                user_message = "❌ This service is currently disabled."
            else:
                reason = "stock_empty"
                user_message = "❌ Number out of stock for this service and country."
            snapshot = inventory_diagnostic_snapshot()
            logger.warning(
                "GET NUMBER selection empty user=%s service=%s country=%s "
                "reason=%s matching_batches=%s matching_numbers=%s "
                "user_used=%s history_used=%s all_batches=%s disabled_services=%s "
                "used_total=%s instance_id=%s",
                chat_id,
                service,
                country,
                reason,
                diagnostic["matching_batches"],
                diagnostic["matching_numbers"],
                diagnostic["live_already_used"],
                diagnostic["history_used"],
                snapshot["batches"],
                snapshot["disabled_services"],
                snapshot["used_numbers"],
                INSTANCE_ID,
            )
            delete_message(chat_id, wait_msg_id)
            send_message(
                chat_id,
                render_body_text(user_message),
                reply_markup=main_menu(chat_id),
            )
            return

        fetched_nums = []
        affected_batches = set()
        with data_lock:
            for batch_id, number in candidates:
                if len(fetched_nums) >= number_limit:
                    break
                batch = number_batches.get(batch_id)
                if not batch or number.get("to_remove"):
                    continue
                if (
                    chat_id in number.get("used_by", [])
                    or number.get("to_remove")
                ):
                    continue
                dirty_number_batches.add(batch_id)
                affected_batches.add(batch_id)

                fetched_nums.append(number["num"])
                pending_number_usage.append((
                    str(chat_id),
                    normalize_service_name(batch.get("service", service)),
                    normalize_service_name(batch.get("country", country)),
                    number["num"],
                    time.time(),
                ))
                number["shares"] += 1
                number["used_by"].append(chat_id)
                total_assigned_stats += 1
                
                if number["shares"] >= bot_settings.get("num_share", 1):
                    number["to_remove"] = True
                    used_numbers_list.append(number["num"])
                    pending_used_numbers.append(number["num"])

            for batch_id in affected_batches:
                batch = number_batches.get(batch_id)
                if batch:
                    batch["numbers"] = [
                        item for item in batch["numbers"]
                        if not item.get("to_remove")
                    ]
            save_db(include_local=True)

        if not fetched_nums:
            delete_message(chat_id, wait_msg_id)
            send_message(
                chat_id,
                render_body_text("❌ You have already taken all numbers or stock is empty!"),
                reply_markup=main_menu(chat_id),
            )
            return

        expire_previous_number(chat_id, remove_message=False)

        app_full_name, app_icon_html = get_service_info_html(service)
        emoji_id = get_premium_app_icon_id(service, "5337302974806922068")
        kb = [[{"text": f"{app_full_name}", "icon_custom_emoji_id": emoji_id, "callback_data": "ignore", "style": "success"}]]
        
        flags_db = bot_settings.get("premium_flags", {})
        country_iso = ""
        country_flag_html = ""
        country_name = ""
        for num in fetched_nums:
            _, iso = get_flag_and_code(num)
            if not country_iso:
                country_iso = iso
                country_flag_html = get_flag_info_html(iso)
                country_name = get_flag_info_html(iso, return_full_name=True)
            display_num = f"+{num}" if not num.startswith("+") else num
            
            emoji_id = "5780471598922337683"
            for flag_code, flag_data in flags_db.items():
                if iso == flag_data.get("iso"):
                    if "id" in flag_data:
                        emoji_id = flag_data["id"]
                    break
            kb.append([{"text": f"{display_num}", "icon_custom_emoji_id": emoji_id, "copy_text": {"text": display_num}, "style": "primary"}])
            
        pair_token = _store_callback_payload(
            "service_country", service, country
        )
        kb.append([{"text": "Change Number", "icon_custom_emoji_id": CHANGE_NUMBER_BUTTON_EMOJI_ID, "callback_data": f"c_n_{pair_token}", "style": "danger"},
                   {"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": bot_settings["otp_link"], "style": "primary"}])
                   
        c_btns = bot_settings["custom_messages"].get("get_number", {}).get("buttons", [])
        for c_b in c_btns:
            b_copy = c_b.copy()
            if "style" not in b_copy:
                b_copy["style"] = "primary"
            kb.append([b_copy])
            
        kb.append([{
            "text": "Back",
            "icon_custom_emoji_id": "5267490665117275176",
            "callback_data": f"back_get_country_{_store_callback_payload('service', service)}",
            "style": "danger",
        }])
        
        if not country_iso or country_iso == "XX":
            country_iso = normalize_service_name(country)[:2] or "XX"
        if not country_name or country_name == country_iso:
            country_name = str(country).title()
        if not country_flag_html or country_flag_html == "🌍":
            country_flag_html = get_flag_info_html(fetched_nums[0]) if fetched_nums else "🌍"

        result_header = (
            f"{country_flag_html} <b>{html.escape(country_name)} "
            f"({html.escape(country_iso)})</b> | "
            f"{app_icon_html} <b>{html.escape(app_full_name)}</b>"
        )
        text_numbers = render_body_text(
            f"{result_header}\n{PEM['waiting']} <b>Waiting for OTP</b>"
        )
        markup = {"inline_keyboard": kb}
        delivered_msg_id = deliver_number_menu(
            chat_id,
            wait_msg_id,
            text_numbers,
            markup,
        )
        if delivered_msg_id:
            with sessions_lock:
                user_active_sessions[chat_id] = {
                    "msg_id": delivered_msg_id,
                    "nums": fetched_nums,
                    "created_at": time.time(),
                    "expires_at": time.time() + ACTIVE_USER_SESSION_TTL_SECONDS,
                }

    elif data.startswith("wapp_") or data.startswith("wrej_"):
        user_id_clicked = call["from"]["id"]
        if not is_admin(user_id_clicked):
            answer_callback(call["id"], "🚫 Only Bot Admins can process withdrawals!", show_alert=True)
            return
            
        action = "APPROVE" if data.startswith("wapp_") else "REJECT"
        req_id = data.replace("wapp_", "").replace("wrej_", "")
        
        # Read the cached request, then claim it exactly once in SQLite below.
        with data_lock:
            req_data = pending_withdrawals.get(req_id)
        if not req_data:
            answer_callback(call["id"], "❌ Request already processed!", show_alert=True)
            return

        u_id, amt = req_data["user_id"], req_data["amount"]
        num = req_data["number"]
        full_name = req_data.get("full_name", u_id)

        if action == "APPROVE" and len(num) >= 7:
            masked_num = f"{num[:4]}❖WAO❖{num[-3:]}"
        else:
            masked_num = num

        status_text = "APPROVED" if action == "APPROVE" else "REJECTED"
        emoji_icon_id = "5352694861990501856" if action == "APPROVE" else "5420130255174145507"
        new_text = f"🎙 <b>WITHDRAWAL {status_text}</b>\n\n👤 <b>USER:</b> <a href='tg://user?id={u_id}'>{html.escape(str(full_name))}</a>\n💳 <b>WITHDRAWAL:</b> {fmt_money(amt)} TK\n🍏 <b>NUMBER:</b> <code>{html.escape(str(masked_num))}</code>\n🏦 <b>METHOD:</b> {html.escape(str(req_data['method']))}\n\n🧾 <b>REQ ID:</b> {req_id}\n👨‍⚖️ <b>PROCESSED BY ADMIN</b>"

        kb = {"inline_keyboard": [[{"text": status_text, "icon_custom_emoji_id": emoji_icon_id, "callback_data": "ignore", "style": "success" if action == "APPROVE" else "danger"}]]}

        try:
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("BEGIN IMMEDIATE")
                new_status = "approved" if action == "APPROVE" else "rejected"
                cursor.execute(
                    "UPDATE withdrawals SET status = ? "
                    "WHERE req_id = ? AND status = 'pending'",
                    (new_status, req_id),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    with data_lock:
                        pending_withdrawals.pop(req_id, None)
                    answer_callback(
                        call["id"],
                        "❌ Request already processed!",
                        show_alert=True,
                    )
                    return
                if action == "REJECT":
                    cursor.execute(
                        "UPDATE users SET balance = ROUND(COALESCE(balance, 0) + ?, 4) "
                        "WHERE user_id = ?",
                        (float(amt), str(u_id)),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            "Could not refund withdrawal: user row is missing"
                        )
                conn.commit()
            finally:
                conn.close()

            with data_lock:
                pending_withdrawals.pop(req_id, None)
            if action == "REJECT":
                with user_cache_lock:
                    cached_user = user_cache.get(str(u_id))
                    if cached_user is not None:
                        cached_user["balance"] = round(
                            cached_user.get("balance", 0.0) + float(amt),
                            4,
                        )
                try:
                    _firebase_increment_user(u_id, balance=float(amt))
                except Exception:
                    logger.exception(
                        "SQLite refund committed but Firestore balance sync failed for withdrawal %s",
                        req_id,
                    )
            if db:
                try:
                    db.collection("withdrawals").document(req_id).update(
                        {"status": new_status},
                        timeout=3,
                    )
                except Exception as firebase_error:
                    logger.warning(
                        "Could not update withdrawal %s in Firestore: %s",
                        req_id,
                        firebase_error,
                    )

            edit_message(chat_id, msg_id, render_body_text(new_text), reply_markup=kb)
            if action == "REJECT":
                send_message(u_id, render_body_text(f"❌ Your {fmt_money(amt)} TK withdrawal request was rejected. Balance refunded."))
            else:
                send_message(u_id, render_body_text(f"{PEM['ok']} Your {fmt_money(amt)} TK withdrawal request has been paid successfully!"))
            answer_callback(call["id"], "✅ " + status_text.title())
        except Exception:
            logger.exception("Withdrawal %s processing failed", req_id)
            answer_callback(call["id"], "❌ Something went wrong while processing.", show_alert=True)

# ==========================================
# Main Polling Loop
# ==========================================
def persist_telegram_updates(updates):
    """Save received updates before acknowledging them to Telegram."""
    if not updates:
        return True
    now = time.time()
    rows = [
        (int(update["update_id"]), json.dumps(update), now)
        for update in updates
        if isinstance(update, dict) and "update_id" in update
    ]
    if not rows:
        return True
    conn = get_db()
    try:
        stored = conn.execute(
            "SELECT value FROM settings WHERE key = 'telegram_polling_offset'"
        ).fetchone()
        try:
            previous_offset = int(json.loads(stored["value"])) if stored else 0
        except (TypeError, ValueError, json.JSONDecodeError):
            previous_offset = 0
        rows = [row for row in rows if row[0] >= previous_offset]
        conn.executemany(
            "INSERT OR IGNORE INTO pending_telegram_updates "
            "(update_id, update_json, received_at) VALUES (?, ?, ?)",
            rows,
        )
        if rows:
            next_offset = max(previous_offset, max(row[0] for row in rows) + 1)
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                ("telegram_polling_offset", json.dumps(next_offset)),
            )
        conn.execute(
            "DELETE FROM pending_telegram_updates WHERE received_at < ?",
            (now - 24 * 60 * 60,),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        logger.exception("Could not persist Telegram updates")
        return False
    finally:
        conn.close()

def load_telegram_polling_offset():
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'telegram_polling_offset'"
        ).fetchone()
        if not row:
            return None
        try:
            return max(0, int(json.loads(row["value"])))
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.warning("Ignoring malformed saved Telegram polling offset")
            return None
    finally:
        conn.close()

_update_failure_counts = {}
MAX_UPDATE_ATTEMPTS = 3

def finish_telegram_update(update_id, success):
    if not success:
        with pending_update_ids_lock:
            attempts = _update_failure_counts.get(update_id, 0) + 1
            _update_failure_counts[update_id] = attempts
            if len(_update_failure_counts) > 2000:
                _update_failure_counts.clear()
        if attempts >= MAX_UPDATE_ATTEMPTS:
            logger.error(
                "Dropping Telegram update %s after %s failed attempts",
                update_id,
                attempts,
            )
            success = True
    else:
        with pending_update_ids_lock:
            _update_failure_counts.pop(update_id, None)
    try:
        if success:
            conn = get_db()
            try:
                conn.execute(
                    "DELETE FROM pending_telegram_updates WHERE update_id = ?",
                    (int(update_id),),
                )
                conn.commit()
            finally:
                conn.close()
    except Exception:
        logger.exception("Could not remove completed Telegram update %s", update_id)
    finally:
        with pending_update_ids_lock:
            pending_update_ids.discard(update_id)

def dispatch_pending_telegram_updates(executor, limit=500):
    conn = get_db()
    try:
        conn.execute(
            "DELETE FROM pending_telegram_updates WHERE received_at < ?",
            (time.time() - 24 * 60 * 60,),
        )
        conn.commit()
        rows = conn.execute(
            "SELECT update_id, update_json FROM pending_telegram_updates "
            "ORDER BY update_id LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    newly_queued = {}
    for row in rows:
        update_id = int(row["update_id"])
        try:
            update = json.loads(row["update_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.exception("Discarding malformed persisted Telegram update %s", update_id)
            finish_telegram_update(update_id, True)
            continue
        chat_id = get_telegram_update_chat_id(update)
        if chat_id is None:
            logger.warning(
                "Discarding unsupported Telegram update %s without chat_id",
                update_id,
            )
            finish_telegram_update(update_id, True)
            continue
        with pending_update_ids_lock:
            if update_id in pending_update_ids:
                continue
            pending_update_ids.add(update_id)
        chat_key = str(chat_id)
        newly_queued.setdefault(chat_key, []).append((update_id, update))

    for chat_key, updates_for_chat in newly_queued.items():
        should_start_worker = False
        with chat_update_queue_lock:
            queue_for_chat = chat_update_queues.setdefault(chat_key, [])
            queue_for_chat.extend(updates_for_chat)
            queue_for_chat.sort(key=lambda item: item[0])
            if chat_key not in active_chat_workers:
                active_chat_workers.add(chat_key)
                should_start_worker = True
        if not should_start_worker:
            continue
        try:
            executor.submit(drain_ordered_chat_updates, chat_key)
        except Exception:
            logger.exception("Could not start ordered update worker for chat %s", chat_key)
            with chat_update_queue_lock:
                active_chat_workers.discard(chat_key)
                failed_queue = chat_update_queues.pop(chat_key, [])
            with pending_update_ids_lock:
                for failed_id, _ in failed_queue:
                    pending_update_ids.discard(failed_id)


def get_telegram_update_chat_id(update):
    """Return the chat ID for a persisted Telegram update, if present."""
    if not isinstance(update, dict):
        return None
    message = update.get("message")
    if isinstance(message, dict):
        return (message.get("chat") or {}).get("id")
    callback = update.get("callback_query")
    if isinstance(callback, dict):
        return ((callback.get("message") or {}).get("chat") or {}).get("id")
    member_update = update.get("chat_member")
    if isinstance(member_update, dict):
        return (member_update.get("chat") or {}).get("id")
    return None


def process_telegram_update(update_id, update):
    """Dispatch one update synchronously inside its chat's ordered worker."""
    if "message" in update:
        return run_message_handler(update["message"], update_id)
    if "callback_query" in update:
        return run_callback_handler(update["callback_query"], update_id)
    if "chat_member" in update:
        return run_chat_member_handler(update["chat_member"], update_id)
    finish_telegram_update(update_id, True)
    return True


_update_executor = None


def drain_ordered_chat_updates(chat_key):
    """Process each chat's updates in update_id order, without serializing others."""
    while True:
        with chat_update_queue_lock:
            queue_for_chat = chat_update_queues.get(chat_key, [])
            if not queue_for_chat:
                chat_update_queues.pop(chat_key, None)
                active_chat_workers.discard(chat_key)
                return
            update_id, update = queue_for_chat.pop(0)
        if not process_telegram_update(update_id, update):
            # Preserve strict ordering on failure: later updates remain in SQLite
            # and are reloaded on the next poll, after this failed ID.
            with chat_update_queue_lock:
                waiting = chat_update_queues.pop(chat_key, [])
                active_chat_workers.discard(chat_key)
            with pending_update_ids_lock:
                for waiting_id, _ in waiting:
                    pending_update_ids.discard(waiting_id)
            # Do not wait for the next long-poll (up to 50 s) to resume this
            # chat; re-dispatch the stored updates almost immediately.
            if _update_executor is not None:
                schedule_once(
                    1.0,
                    dispatch_pending_telegram_updates,
                    _update_executor,
                )
            return

def run_message_handler(message, update_id=None):
    chat_id = message.get("chat", {}).get("id")
    success = False
    lock = get_handler_lock(chat_id) if chat_id is not None else threading.RLock()
    lock_acquired = False
    started_at = time.monotonic()
    try:
        lock_acquired = lock.acquire(timeout=15)
        if not lock_acquired:
            logger.error(
                "Message handler lock timed out chat=%s update_id=%s",
                chat_id,
                update_id,
            )
            if chat_id is not None:
                send_message(
                    chat_id,
                    render_body_text(
                        "⏳ The bot is busy with your previous request. "
                        "This message was not processed; please send it again shortly."
                    ),
                )
            success = True
            return success
        handler_started_at = time.monotonic()
        try:
            handle_message(message)
        finally:
            elapsed = time.monotonic() - handler_started_at
            if elapsed > 15:
                logger.error(
                    "Slow message handler chat=%s elapsed=%.1fs update_id=%s",
                    chat_id,
                    elapsed,
                    update_id,
                )
        success = True
    except Exception:
        logger.exception(
            "Unhandled message error chat=%s instance_id=%s",
            chat_id,
            INSTANCE_ID,
        )
        notify_owner_error("Unhandled message handler exception")
        try:
            if chat_id is not None and message.get("chat", {}).get("type", "private") == "private":
                send_message(
                    chat_id,
                    render_body_text("❌ Something went wrong. Please try again."),
                )
        except Exception as feedback_error:
            logger.warning("Could not send message-error feedback: %s", feedback_error)
        # The handler may have performed a side effect before it failed. Never
        # replay the same Telegram update automatically.
        success = True
    finally:
        if lock_acquired:
            lock.release()
        total_elapsed = time.monotonic() - started_at
        if total_elapsed > 30:
            logger.error(
                "Message update exceeded watchdog threshold chat=%s elapsed=%.1fs",
                chat_id,
                total_elapsed,
            )
        if update_id is not None:
            finish_telegram_update(update_id, success)
    return success

# Callbacks that wait on Firestore, SQLite backups, uploads or other slow I/O.
# They must never block the per-chat ordered queue (which would delay every
# later button press of that chat), so they run in a dedicated pool.
SLOW_CALLBACK_EXACT = {
    "system_status", "download_backup", "test_fj",
    "dl_used", "dl_unused", "dl_flags_txt", "dl_apps_txt",
    "refresh_traffic",
}
SLOW_CALLBACK_PREFIXES = ("lb_top_", "lb_w_", "test_p_conn_")
# Pure navigation callbacks: acknowledged immediately, before any lock/DB work.
EARLY_ACK_PREFIXES = (
    "g_s_", "g_c_", "refresh_fj", "back_get_", "back_to_admin",
    "close_msg", "close_cpt_", "cancel_state", "cancel_2fa",
    "number_settings", "lb_main",
)
slow_callback_executor = ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="bot-slow-callback"
)

def _is_slow_callback(callback_data):
    return (
        callback_data in SLOW_CALLBACK_EXACT
        or callback_data.startswith(SLOW_CALLBACK_PREFIXES)
    )

def _callback_failure_feedback(callback):
    """Always give the user visible feedback so Loading never hangs."""
    try:
        answer_callback(
            callback.get("id"),
            "❌ Something went wrong. Please try again.",
            show_alert=True,
        )
    except Exception as ack_error:
        logger.warning("Could not send callback error acknowledgement: %s", ack_error)

def _run_slow_callback(callback, update_id):
    success = False
    started_at = time.monotonic()
    try:
        handle_callback(callback)
        success = True
    except Exception:
        logger.exception(
            "Unhandled slow callback error for data=%s instance_id=%s",
            callback.get("data", ""),
            INSTANCE_ID,
        )
        notify_owner_error("Unhandled callback handler exception")
        _callback_failure_feedback(callback)
        success = True   # see run_callback_handler: never replay a failed button
    finally:
        elapsed = time.monotonic() - started_at
        if elapsed > 30:
            logger.error(
                "Slow callback exceeded watchdog threshold elapsed=%.1fs data=%s",
                elapsed,
                str(callback.get("data", ""))[:100],
            )
        try:
            answer_callback(callback.get("id"))  # no-op if already answered
        except Exception:
            logger.warning("Final callback acknowledgement failed", exc_info=True)
        if update_id is not None:
            finish_telegram_update(update_id, success)

def run_callback_handler(callback, update_id=None):
    # Start the acknowledgement timer before waiting on the per-chat lock, so
    # a queued button press cannot keep Telegram's spinner active indefinitely.
    begin_callback_ack(callback)
    chat_id = callback.get("message", {}).get("chat", {}).get("id")
    success = False
    deferred = False
    lock = None
    lock_acquired = False
    started_at = time.monotonic()
    try:
        callback_data = callback.get("data", "") or ""
        if callback_data.startswith(EARLY_ACK_PREFIXES):
            # Acknowledge these UI callbacks before any database or inventory
            # work, not after waiting for the per-chat handler lock.
            answer_callback(callback.get("id"))
        if callback_data == "refresh_traffic":
            # The toast text must be sent now; after the 0.5s auto-ack it
            # would otherwise arrive as a separate chat message.
            answer_callback(callback.get("id"), "✅ Traffic Refreshed!", show_alert=False)
        if _is_slow_callback(callback_data):
            # Run outside both the per-chat lock and the ordered chat queue.
            try:
                slow_callback_executor.submit(_run_slow_callback, callback, update_id)
                deferred = True
                return True
            except Exception:
                logger.exception("Could not start slow callback worker; running inline")
                handle_callback(callback)
        else:
            lock = get_handler_lock(chat_id) if chat_id is not None else threading.RLock()
            lock_acquired = lock.acquire(timeout=15)
            if not lock_acquired:
                logger.error(
                    "Callback handler lock timed out chat=%s update_id=%s data=%s",
                    chat_id,
                    update_id,
                    callback_data[:100],
                )
                if chat_id is not None:
                    send_message(
                        chat_id,
                        render_body_text(
                            "⏳ The bot is busy with your previous request. "
                            "This button was not processed; please tap again shortly."
                        ),
                    )
                success = True
                return success
            handler_started_at = time.monotonic()
            try:
                handle_callback(callback)
            finally:
                elapsed = time.monotonic() - handler_started_at
                if elapsed > 15:
                    logger.error(
                        "Slow callback handler chat=%s elapsed=%.1fs data=%s",
                        chat_id,
                        elapsed,
                        callback_data[:100],
                    )
        success = True
    except Exception:
        logger.exception(
            "Unhandled callback error for data=%s instance_id=%s",
            callback.get("data", ""),
            INSTANCE_ID,
        )
        notify_owner_error("Unhandled callback handler exception")
        _callback_failure_feedback(callback)
        # The user already got an error toast.  Replaying a stale button later
        # (and blocking every later update of this chat meanwhile) only made
        # the bot feel frozen, so treat it as handled.
        success = True
    finally:
        if lock_acquired and lock is not None:
            lock.release()
        if time.monotonic() - started_at > 30:
            logger.error(
                "Callback update exceeded watchdog threshold chat=%s elapsed=%.1fs",
                chat_id,
                time.monotonic() - started_at,
            )
        if not deferred:
            try:
                # Guarantee no branch leaves the spinner running.
                answer_callback(callback.get("id"))
            except Exception:
                logger.warning("Final callback acknowledgement failed", exc_info=True)
            if update_id is not None:
                finish_telegram_update(update_id, success)
    return success


def run_chat_member_handler(chat_member_update, update_id=None):
    success = False
    try:
        member = chat_member_update.get("new_chat_member", {}) or {}
        user_id = (member.get("user", {}) or {}).get("id")
        if user_id is not None:
            with force_join_cache_lock:
                force_join_cache.pop(str(user_id), None)
            logger.info(
                "Force Join membership cache invalidated user=%s chat=%s status=%s",
                user_id,
                (chat_member_update.get("chat", {}) or {}).get("id"),
                member.get("status", "unknown"),
            )
        success = True
    except Exception:
        logger.exception(
            "Could not process Telegram chat_member update instance_id=%s",
            INSTANCE_ID,
        )
        notify_owner_error("Unhandled chat-member handler exception")
    finally:
        if update_id is not None:
            finish_telegram_update(update_id, success)
    return success


def create_database_backup():
    """Create a consistent SQLite backup and retain only the latest five."""
    global LAST_BACKUP_TIME
    with backup_lock:
        backup_directory = DB_FILE + ".backups"
        os.makedirs(backup_directory, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        backup_path = os.path.join(
            backup_directory,
            f"bot_backup_{stamp}_{INSTANCE_ID}.db",
        )
        destination = None
        source = get_db()
        try:
            destination = sqlite3.connect(backup_path, timeout=30)
            source.backup(destination)
            destination.commit()
        finally:
            if destination is not None:
                destination.close()
            source.close()
        scrub_sqlite_secrets(backup_path)  # defence in depth: never ship plaintext credentials
        backup_files = sorted(
            glob.glob(os.path.join(backup_directory, "bot_backup_*.db")),
            key=os.path.getmtime,
            reverse=True,
        )
        for old_backup in backup_files[5:]:
            try:
                os.remove(old_backup)
            except OSError:
                logger.exception("Could not remove old database backup %s", old_backup)
        LAST_BACKUP_TIME = time.time()
        logger.info("SQLite backup created: %s", backup_path)
        return backup_path


def _send_backup_to_owner(backup_path):
    """Send the SQLite backup file to the owner's Telegram inbox."""
    try:
        with open(backup_path, "rb") as backup_file:
            content = backup_file.read()
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        filename = f"bot_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        url = f"{BASE_URL}/sendDocument"
        caption = (
            f"🗄 <b>Auto DB Backup</b>\n"
            f"📅 {stamp}\n"
            f"📦 Size: {len(content) // 1024} KB\n"
            f"🤖 Instance: <code>{INSTANCE_ID}</code>"
        )
        files = {"document": (filename, content, "application/octet-stream")}
        data_payload = {"chat_id": OWNER_ID, "caption": caption, "parse_mode": "HTML"}
        result = tg_session.post(url, data=data_payload, files=files, timeout=60)
        if result.ok:
            logger.info("Auto backup sent to owner (%s KB)", len(content) // 1024)
        else:
            logger.warning("Owner backup send failed: %s", result.status_code)
    except Exception:
        logger.exception("Could not send backup to owner")


# Send backup to owner every 24 hours (local backups still run every 6h).
_BACKUP_SEND_TO_OWNER_INTERVAL = 24 * 60 * 60
_backup_send_cycle = 0


def database_backup_worker():
    """Create a startup backup (and send to owner), then repeat every six hours.
    Every 4th cycle (≈24 h) the backup is also forwarded to the owner's inbox.
    """
    global _backup_send_cycle
    while True:
        try:
            backup_path = create_database_backup()
            _backup_send_cycle += 1
            # Send to owner on startup (cycle 1) and every 24 h thereafter.
            if _backup_send_cycle == 1 or _backup_send_cycle % 4 == 0:
                _send_backup_to_owner(backup_path)
                # ── advanced_features: mirror to backup channel ───────────
                send_backup_to_channel(backup_path)
        except Exception:
            logger.exception("Scheduled SQLite backup failed")
        time.sleep(6 * 60 * 60)


def check_source_encoding():
    """Warn if the running UTF-8 source still contains common mojibake bytes."""
    try:
        with open(__file__, "r", encoding="utf-8") as source_file:
            source_text = source_file.read()
        # UTF-8 emoji/punctuation decoded as CP1252 typically starts with one
        # of these Latin lead-byte glyphs. Avoid flagging ordinary accented words.
        prefixes = {chr(value) for value in (0x00C2, 0x00C3, 0x00E0, 0x00E2, 0x00EF, 0x00F0)}
        found = []
        for index, char in enumerate(source_text[:-1]):
            if char in prefixes and ord(source_text[index + 1]) >= 128:
                found.append((index, source_text[index:index + 8]))
                if len(found) >= 5:
                    break
        if found:
            logger.warning("Source may contain UTF-8 mojibake sequences: %r", found)
        else:
            logger.info("Source encoding self-check passed (UTF-8; no common mojibake signatures).")
    except (OSError, UnicodeError):
        logger.exception("Could not run source encoding self-check")


def main():
    global BOT_USERNAME, last_poll_at
    # Polling apps must run as one Reserved VM or Background Worker instance.
    # Do not use Autoscale, and stop Workspace Run while the Deployment is active.
    # Never point two instances at the same SQLite file.
    check_source_encoding()
    acquire_single_instance_lock()
    migrate_rounded_balances()
    logger.info(
        "Bot startup pid=%s instance_id=%s db=%s",
        os.getpid(),
        INSTANCE_ID,
        DB_FILE,
    )
    try:
        res = api_call("getMe")
        if res.get("ok"):
            BOT_USERNAME = res["result"]["username"]
    except Exception as e:
        logger.error(f"Error getting bot info: {e}")
        BOT_USERNAME = "your_bot_username"
    
    logger.info(f"🤖 Bot is starting... @{BOT_USERNAME}")
    if _FERNET is None:
        alert_owner_if_data_encryption_key_missing()
    alert_owner_if_previous_encrypted_data_key_was_lost()
    notify_expired_admin_interactions()
    restore_active_sessions()
    schedule_once(60, _cleanup_runtime_state)

    # Keep the Telegram HTTPS connections warm so the first button press after
    # a quiet period does not hit a dead pooled connection.
    def _telegram_keepalive_loop():
        while True:
            time.sleep(25)
            try:
                api_call("getMe")
            except Exception:
                pass
    threading.Thread(target=_telegram_keepalive_loop, name="tg-keepalive", daemon=True).start()
    
    # Start background threads
    threading.Thread(target=panel_monitor_thread, daemon=True).start()
    logger.info("📡 Auto Captcha panel monitor started!")
    threading.Thread(
        target=database_backup_worker,
        name="sqlite-backup",
        daemon=True,
    ).start()

    # ── analytics_addon: emergency crash backup hooks ─────────────────
    setup_emergency_backup_hooks()

    # ── analytics_addon: daily report + low-stock alert thread ───────
    threading.Thread(
        target=analytics_report_worker,
        name="analytics-reporter",
        daemon=True,
    ).start()
    logger.info("📊 Analytics reporter + emergency backup hooks active.")
    # ─────────────────────────────────────────────────────────────────

    # ── advanced_features: rate limiter · i18n · cloud backup · targeted blast
    start_advanced_threads()
    logger.info("🚀 Advanced features active: rate limiter · i18n · cloud backup · targeted broadcast")
    # ─────────────────────────────────────────────────────────────────

    # Delete webhook to ensure polling works
    try:
        api_call("deleteWebhook")
    except Exception:
        logger.exception("Could not delete Telegram webhook before polling")
    
    executor = ThreadPoolExecutor(
        max_workers=32,
        thread_name_prefix="bot-update",
    )
    
    global _update_executor
    _update_executor = executor
    offset = load_telegram_polling_offset()
    dispatch_pending_telegram_updates(executor)
    poll_failure_streak = 0
    while True:
        try:
            updates = api_call(
                "getUpdates",
                {
                    "timeout": 50,
                    "offset": offset,
                    "allowed_updates": ["message", "callback_query", "chat_member"],
                },
            )
            last_poll_at = time.time()
            if not updates or not updates.get("ok", False):
                poll_failure_streak = min(poll_failure_streak + 1, 6)
                delay = min(2 ** poll_failure_streak, 60)
                logger.warning(
                    "Telegram polling returned no updates/error; retrying in %ss",
                    delay,
                )
                time.sleep(delay)
                continue
            poll_failure_streak = 0
            received_updates = updates.get("result", [])
            if persist_telegram_updates(received_updates):
                if received_updates:
                    offset = load_telegram_polling_offset()
                dispatch_pending_telegram_updates(executor)
            else:
                # Do not move the offset when SQLite could not durably save the
                # payload; Telegram will return it again on the next poll.
                poll_failure_streak = min(poll_failure_streak + 1, 6)
                time.sleep(min(2 ** poll_failure_streak, 60))
        except Exception as e:
            last_poll_at = time.time()
            poll_failure_streak = min(poll_failure_streak + 1, 6)
            delay = min(2 ** poll_failure_streak, 60)
            logger.exception(
                "Telegram polling failed; retrying in %ss (%s)",
                delay,
                type(e).__name__,
            )
            time.sleep(delay)

if __name__ == "__main__":
    main()
