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
        # Core
        "qurilish", "строительств", "ta'mir", "tamir", "ta'mirlash", "ремонт",
        # Infrastructure objects
        "школ", "дорог", "больниц", "мост", "ирригаци",
        "бино", "здани", "сооружени", "подстанци",
        # Utility systems
        "канализаци", "водоснабж", "газоснабж", "теплоснабж",
        "тепловых сет", "котельн", "насос",
        # Work types
        "электромонтаж", "монтаж", "демонтаж",
        "кровл", "фасад", "отделк", "бетон", "асфальт",
        "перекладк", "благоустройств", "трансформатор",
        # Uzbek extras
        "o'rnatish",
    ]

    # ── Non-construction deal filter (Tier 2 negative) ──────
    # Categories that are obviously NOT construction — used to reject
    # false positives when a deal matches only via customer/provider name.
    non_construction_keywords: list[str] = [
        # Food/catering
        "питан", "овқатлантириш", "catering", "ошхона",
        # IT/office equipment
        "сервер", "компьютер", "принтер", "оргтехник", "программ",
        # Transport (not road construction)
        "перевозк", "ташиш", "транспортир",
        # Medical/pharma
        "медицин", "фармацевтик", "дори",
        # Furniture/office supplies
        "мебел", "канцеляр",
        # Fuel
        "топлив", "ёнилғи", "бензин",
    ]

    # ── Non-contractor company filter ────────────────────────
    # Companies matching these in their name are NOT construction contractors.
    # They participate in construction tenders as assessors/labs/consultants.
    non_contractor_keywords: list[str] = [
        # Uzbek
        "baholash", "baho", "sinov", "laboratoriya", "ekspertiza", "ekspert",
        "konsalting", "tekshirish", "nazorat", "sertifikat", "standart", "metrologiya",
        # Russian
        "оценк", "оценоч", "испытат", "лаборатор", "экспертиз", "консалтинг",
        "консультац", "инспекц", "сертифик", "стандарт", "метролог",
        "надзор", "аудит", "мониторинг",
        # English (some companies use English names)
        "consulting", "assessment", "laboratory", "evaluation", "inspection",
        "certification", "expertise", "audit", "monitoring",
    ]

    # Company types to exclude from competitor rankings.
    # Deals from these companies still count toward market volume totals.
    excluded_company_types: list[str] = ["consultant", "laboratory", "assessor", "other"]

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

    # Normalization: map all variant spellings → single canonical Cyrillic name.
    # Used by enrichment to merge duplicates (e.g., "Toshkent" + "Тошкент шахар").
    region_normalization: dict[str, str] = {
        # Latin short → Cyrillic canonical
        "Toshkent": "Тошкент шахар",
        "Toshkent shahri": "Тошкент шахар",
        "Toshkent viloyati": "Тошкент вилояти",
        "Samarqand": "Самарқанд",
        "Buxoro": "Бухоро",
        "Farg'ona": "Фарғона",
        "Andijon": "Андижон",
        "Namangan": "Наманган",
        "Qashqadaryo": "Қашқадарё",
        "Surxondaryo": "Сурхондарё",
        "Jizzax": "Жиззах",
        "Sirdaryo": "Сирдарё",
        "Navoiy": "Навоий",
        "Xorazm": "Хоразм",
        "Qoraqalpog'iston": "Қорақалпоғистон",
        # Reyting.mc.uz format (Latin + "viloyati" suffix)
        "Samarqand viloyati": "Самарқанд",
        "Buxoro viloyati": "Бухоро",
        "Farg'ona viloyati": "Фарғона",
        "Andijon viloyati": "Андижон",
        "Namangan viloyati": "Наманган",
        "Qashqadaryo viloyati": "Қашқадарё",
        "Surxondaryo viloyati": "Сурхондарё",
        "Jizzax viloyati": "Жиззах",
        "Sirdaryo viloyati": "Сирдарё",
        "Navoiy viloyati": "Навоий",
        "Xorazm viloyati": "Хоразм",
        "Qoraqalpog'iston Respublikasi": "Қорақалпоғистон",
        # Russian variants
        "Ташкент": "Тошкент шахар",
        "Ташкентская": "Тошкент вилояти",
        "Самарканд": "Самарқанд",
        "Самаркандская": "Самарқанд",
        "Бухарская": "Бухоро",
        "Бухарский": "Бухоро",
        "Ферганская": "Фарғона",
        "Андижанская": "Андижон",
        "Наманганская": "Наманган",
        "Кашкадарьинская": "Қашқадарё",
        "Сурхандарьинская": "Сурхондарё",
        "Сурхандарынская": "Сурхондарё",
        "Джизакская": "Жиззах",
        "Сырдарьинская": "Сирдарё",
        "Навоийская": "Навоий",
        "Хорезмская": "Хоразм",
        "Каракалпакстан": "Қорақалпоғистон",
        "Нукус": "Қорақалпоғистон",
    }

    # District/city names → parent region (for text extraction fallback)
    district_to_region: dict[str, str] = {
        # Tashkent city districts
        "Сергели": "Тошкент шахар", "Юнусабад": "Тошкент шахар",
        "Чиланзар": "Тошкент шахар", "Мирзо Улугбек": "Тошкент шахар",
        "Яккасарай": "Тошкент шахар", "Шайхантахур": "Тошкент шахар",
        "Алмазар": "Тошкент шахар", "Учтепа": "Тошкент шахар",
        "Бектемир": "Тошкент шахар", "Мирабад": "Тошкент шахар",
        "Яшнабад": "Тошкент шахар", "Chilonzor": "Тошкент шахар",
        "Sergeli": "Тошкент шахар", "Yunusobod": "Тошкент шахар",
        "Олмазор": "Тошкент шахар",
        # Major cities → region
        "Карши": "Қашқадарё", "Термез": "Сурхондарё",
        "Нукус": "Қорақалпоғистон", "Nukus": "Қорақалпоғистон",
        "Олмалиқ": "Тошкент вилояти", "Ангрен": "Тошкент вилояти",
        "Чирчик": "Тошкент вилояти", "Алмалык": "Тошкент вилояти",
        "Olmaliq": "Тошкент вилояти", "Chirchiq": "Тошкент вилояти",
        "Angren": "Тошкент вилояти",
    }

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


config = Config()
