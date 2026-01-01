import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        config_path = Path(__file__).parent.parent / "config.yaml"
        with open(config_path) as f:
            self._yaml = yaml.safe_load(f)

        # Telegram Userbot
        self.telegram_api_id = int(os.environ["TELEGRAM_API_ID"])
        self.telegram_api_hash = os.environ["TELEGRAM_API_HASH"]
        self.telegram_phone = os.environ["TELEGRAM_PHONE"]

        # Telegram Bot
        self.bot_token = os.environ["BOT_TOKEN"]
        self.admin_chat_id = int(os.environ["ADMIN_CHAT_ID"])

        # LLM Provider (gemini or openrouter)
        self.llm_provider = os.environ.get("LLM_PROVIDER", "gemini").lower()

        # Gemini
        self.gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
        self.gemini_model = self._yaml.get("gemini", {}).get("model", "gemini-2.0-flash")

        # OpenRouter
        self.openrouter_api_key = os.environ.get("OPENROUTER_API_KEY", "")
        self.openrouter_model = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")

        # PostgreSQL
        self.db_url = (
            f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
            f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
        )

        # Settings from YAML
        self.folder_name = self._yaml["telegram"]["folder_name"]
        self.exclude_words = [w.lower() for w in self._yaml["filter"]["exclude_words"]]
        self.interval_minutes = self._yaml["scheduler"]["interval_minutes"]
        
        # Scan mode: timer (auto-scan) or manual (bot commands only)
        self.scan_mode = os.environ.get("SCAN_MODE", "timer").lower()
        
        # Facebook settings
        fb_config = self._yaml.get("facebook", {})
        self.facebook_enabled = fb_config.get("enabled", False)
        self.facebook_posts_per_group = fb_config.get("posts_per_group", 20)
        self.facebook_scan_interval = fb_config.get("scan_interval_minutes", 15)
        self.chrome_cdp_url = os.environ.get("CHROME_CDP_URL", "http://localhost:9222")


config = Config()

