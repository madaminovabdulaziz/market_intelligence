"""Application configuration via pydantic-settings."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """All settings, overridable via environment variables or .env file."""

    # ── Database ──────────────────────────────────────────────
    db_host: str = "localhost"
    db_port: int = 5433
    db_name: str = "market_intelligence"
    db_user: str = "postgres"
    db_password: str = "postgres"

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # ── ETender API ───────────────────────────────────────────
    etender_api_url: str = "https://apietender.uzex.uz/api/common/DealsList"
    etender_concurrency: int = 5
    etender_batch_delay: float = 0.5

    # ── Reyting ───────────────────────────────────────────────
    reyting_api_base: str = "https://japi-reyting.mc.uz"
    reyting_browser_url: str = "https://reyting.mc.uz/ratings/0/{stir}"
    reyting_concurrency: int = 3
    reyting_request_delay: float = 0.3

    # ── HTTP defaults ─────────────────────────────────────────
    http_timeout: int = 30
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (compatible; MarketIntel/1.0)"

    # ── Construction keyword filter ───────────────────────────
    construction_keywords: list[str] = [
        "qurilish", "строительств", "ta'mir", "tamir", "ремонт",
        "школ", "дорог", "больниц", "мост", "ирригаци",
        "бино", "здани", "канализаци", "водоснабж",
        "электромонтаж", "кровл", "фасад", "бетон",
        "асфальт", "газоснабж", "теплоснабж",
    ]

    # ── Uzbekistan regions (for extraction from text) ─────────
    regions: list[str] = [
        "Тошкент шахар", "Тошкент вилояти",
        "Самарқанд", "Бухоро", "Фарғона",
        "Андижон", "Наманган", "Қашқадарё",
        "Сурхондарё", "Жиззах", "Сирдарё",
        "Навоий", "Хоразм", "Қорақалпоғистон",
        # Latin variants
        "Toshkent", "Samarqand", "Buxoro", "Farg'ona",
        "Andijon", "Namangan", "Qashqadaryo",
        "Surxondaryo", "Jizzax", "Sirdaryo",
        "Navoiy", "Xorazm", "Qoraqalpog'iston",
    ]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


config = Config()
