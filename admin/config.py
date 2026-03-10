import os
import secrets

from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    print(
        "[WARN] ADMIN_SECRET_KEY not set in .env — using a random key. "
        "Sessions will not survive restarts. Add ADMIN_SECRET_KEY to your .env file."
    )

ADMIN_PORT = int(os.getenv("ADMIN_PORT", "8111"))
ADMIN_DB_PATH = os.getenv("ADMIN_DB_PATH", os.path.join(os.getcwd(), "admin.db"))
SESSION_MAX_AGE = 60 * 60 * 24 * 7  # 7 days

# Config keys that must never be exposed in the config editor
HIDDEN_CONFIG_KEYS = {"ADMIN_SECRET_KEY"}

# Known .env keys for the gearshop crawler (used for allowlist validation)
KNOWN_ENV_KEYS = {
    "BREVO_API_KEY", "EMAIL_FROM", "EMAIL_TO",
    "SITE_ROOT", "COLLECTION_URL", "DB_PATH",
    "PRODUCT_DELAY", "MAX_SCROLL_SECONDS", "AVAIL_HTML_MAX",
    "HEARTBEAT_UTC_HOUR", "CRAWLER_DEBUG", "JSON_OUT_PATH",
    "ADMIN_SECRET_KEY", "ADMIN_PORT", "ADMIN_DB_PATH",
}

# Keys whose values should be masked in the UI
SENSITIVE_KEYS = {"BREVO_API_KEY", "ADMIN_SECRET_KEY"}
