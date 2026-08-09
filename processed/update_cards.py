"""Обновление Cards_target.csv инкрементами из raw/cards на Google Drive.

Инкременты по картам — чистый append: каждый день на FTP выкладывается файл с
новыми картами, они уже лежат в raw/cards/<дата>/. Скрипт обрабатывает даты
из диапазона (processed-watermark + 1 день … min(raw-watermark, ds_run − 1 день)),
приводит инкременты к схеме целевого файла и дописывает их к базовому файлу.

Дубликаты снимаются по паре (`код`, `телефон`) с приоритетом более свежей
строки: инкременты присоединяются в хронологическом порядке после базы, поэтому
`keep="last"` оставляет последнюю известную версию карты. Это же даёт
идемпотентность — повторная обработка той же даты не удвоит строки.

Возраст и признак `vip` пересчитываются для всей таблицы, а не только для новых
карт: возраст меняется со временем, а справочник ВИПов («Свод ВИПов» на Google
Drive) правится вручную и задним числом.

Файл перезаписывается целиком одной операцией в конце: если что-то упало на
середине диапазона, ни целевой файл, ни watermark не меняются.
"""

import argparse
import io
import logging
import sys
from configparser import ConfigParser
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

import numpy as np
import pandas as pd

from lib.drive_client import (
    download_bytes,
    get_drive_service,
    list_children,
    update_file_bytes,
)
from lib.gsheet_client import read_sheet_as_dataframe
from lib.pipeline_state import read_last_updated, update_pipeline_state

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "cards"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

SW_PROGRAM = "Бонусная программа Smart-Wallet"

# соответствие колонок инкремента колонкам целевого файла;
# всё, чего нет в этом словаре (вид карты, возрастная группа), отбрасывается.
# год_рождения — временная колонка: из неё собирается дата_рождения и удаляется
INCREMENT_RENAME = {
    "Информационная карта": "фио",
    "Код": "код",
    "Код карты": "телефон",
    "Бонусная программа лояльности": "бонусная_программа_лояльности",
    "Дата рождения": "дата_рождения",
    "Год рождения": "год_рождения",
    "Дата открытия": "дата_открытия_карты",
    "Пол": "пол",
    "Источник": "источник",
    "Группа": "магазин_открытия_карты",
    "Удовлетвренность продуктом": "удовлетворенность_продуктом",
    "Уровень сервиса": "уровень_сервиса",
}

# читаем как строки, иначе excel отдаёт '04.10' как float 4.1 и месяц теряется
INCREMENT_DTYPES = {
    "Код": str,
    "Код карты": str,
    "Дата рождения": str,
    "Год рождения": str,
}

MAP_STORE = {
    '01 г. Алматы "Алмаз"': "Алматы Алмаз",
    '02 г. Алматы "Гаухар"': "Алматы Гаухар",
    "03 г.Актобе «Изумруд»": "Актобе Изумруд",
    "04 г.Астана «Гаухар»": "Астана Гаухар",
    "05 г.Жезгазган «Янтарь»": "Жезказган Янтарь",
    "06 г. Караганда «Аметист»": "Караганда Аметист",
    "07 г.Костанай «Рубин»": "Костанай Рубин",
    "08 г.Оскемен «Янтарь»": "Оскемен Янтарь",
    "09 г.Орал «Агат»": "Орал Агат",
    "10 г.Павлодар «Кристалл»": "Павлодар Кристалл",
    "11 г.Петропавловск «Алмаз»": "Петропавловск Алмаз",
    "12 г.Семей «Изумруд»": "Семей Изумруд",
    "13 г.Тараз «Алмаз»": "Тараз Алмаз",
    "14 г.Шымкент «Алмаз»": "Шымкент Алмаз",
    "15 г.Экибастуз": "Экибастуз Жемчуг",
    "16 г.Актау": "Актау",
    "19 г.Астана Левый берег": "Астана Левый берег",
    "20 Алматы Mega": "Алматы Mega",
    "21 Алматы Esentai Mall": "Алматы Esentai Mall"
}
DEFAULT_STORE = "Алматы Mega"

KEY_COLUMNS = ["код", "телефон"]
TARGET_COLUMNS = [
    "фио",
    "код",
    "телефон",
    "бонусная_программа_лояльности",
    "пол",
    "источник",
    "удовлетворенность_продуктом",
    "уровень_сервиса",
    "дата_рождения",
    "возраст",
    "категория_возраста",
    "vip",
    "магазин_первой_покупки",
    "фамилия",
    "имя",
    "магазин_открытия_карты",
    "дата_открытия_карты",
]

VIP_SHEET_NAME = "Свод ВИПов"
VIP_TAB_NAME = "Лист1"
VIP_PHONE_COLUMN = "телефон"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

config = ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

CREDS_PATH = PROJECT_DIR / config["DRIVE_API"]["creds"]
TOKEN_PATH = PROJECT_DIR / config["DRIVE_API"]["token"]
GSHEET_CREDENTIALS = PROJECT_DIR / config["GSHEET"]["credentials_json"]

RAW_CARDS_ID = config["PIPELINE_RAW"]["pipeline_raw_cards_id"]
CARDS_TARGET_ID = config["PIPELINE_PROCESSED"]["cards_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def phone_processor(phone):
    """Приводит номер телефона к 10 цифрам; NaN, если номер короче."""
    if pd.isna(phone):
        return np.nan

    # гугл-таблицы отдают номер как float; без int() в строке остаётся '.0'
    # и последние 10 цифр съезжают
    raw = str(int(phone)) if isinstance(phone, float) else str(phone)

    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 10:
        return digits[-10:]

    return np.nan


def calculate_age(birthdate, today):
    """Возраст на дату today; None вне диапазона 15–89 лет (защита от мусорных дат)."""
    if pd.isna(birthdate):
        return np.nan

    age = (
        today.year
        - birthdate.year
        - ((today.month, today.day) < (birthdate.month, birthdate.day))
    )
    return age if 14 < age < 90 else np.nan


def age_group(age):
    """Категория возраста покупателя."""
    if pd.isna(age):
        return np.nan
    if age < 25:
        return "до 25"
    if age < 35:
        return "25-34"
    if age < 45:
        return "35-44"
    if age < 55:
        return "45-54"
    return "55+"


def find_increment_files(service, date_strs):
    """Ищет в raw/cards эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_CARDS_ID)
        if item["mimeType"] == FOLDER_MIMETYPE
    }

    file_ids = {}
    missing = []
    for ds in date_strs:
        folder_id = date_folders.get(ds)
        if folder_id is None:
            missing.append(ds)
            continue

        excels = [
            item
            for item in list_children(service, folder_id)
            if Path(item["name"]).suffix.lower() in EXCEL_SUFFIXES
        ]
        if not excels:
            missing.append(ds)
            continue

        file_ids[ds] = excels[0]["id"]

    if missing:
        logger.error(
            "В raw/cards нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дней — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return file_ids


def transform_increment(raw):
    """Приводит инкремент карт к схеме целевого файла."""
    incr = raw.rename(columns=INCREMENT_RENAME)
    incr = incr[[column for column in INCREMENT_RENAME.values() if column in incr.columns]].copy()

    incr["телефон"] = incr["телефон"].apply(phone_processor)
    incr = incr.dropna(subset=["телефон"]).copy()

    # фамилию и имя выделяем только у Smart-Wallet: в остальных программах
    # поле "Информационная карта" заполняется произвольно
    sw_mask = incr["бонусная_программа_лояльности"].eq(SW_PROGRAM)
    fio_parts = incr["фио"].str.strip().str.split(r"\s+", regex=True)
    incr["фамилия"] = fio_parts.str[0].where(sw_mask)
    incr["имя"] = fio_parts.str[-1].where(sw_mask)

    # день и месяц лежат в одной колонке ('04.10'), год — в соседней
    incr["дата_рождения"] = pd.to_datetime(
        incr["дата_рождения"].str.strip() + "." + incr["год_рождения"].str.strip(),
        format="%d.%m.%Y",
        errors="coerce",
    )
    incr = incr.drop(columns=["год_рождения"])

    # формат задан явно: чужой формат выгрузки даст NaT, а не тихо разберётся
    incr["дата_открытия_карты"] = pd.to_datetime(
        incr["дата_открытия_карты"], format="%d.%m.%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    incr["магазин_открытия_карты"] = (
        incr["магазин_открытия_карты"].replace(MAP_STORE).fillna(DEFAULT_STORE)
    )

    # магазин первой покупки известен только из продаж; vip проставляется
    # по справочнику уже после объединения с базой
    incr["магазин_первой_покупки"] = pd.NA

    return incr


def update_cards(ds_run=None):
    """Дописывает инкременты карт из raw/cards в Cards_target.csv.

    ds_run — дата запуска (строка YYYY-MM-DD); по умолчанию сегодня.
    Обрабатываются даты до ds_run − 1 включительно и не дальше raw-watermark.
    """
    run_date = (
        pd.to_datetime(ds_run).normalize() if ds_run else pd.Timestamp.today().normalize()
    )

    service = get_drive_service(CREDS_PATH, TOKEN_PATH)

    last_updated = read_last_updated(service, PIPELINE_STATE_ID, SOURCE, LAYER)
    if last_updated is None:
        logger.error(
            "В pipeline_state.csv нет записи о последнем обновлении source=%s, layer=%s. "
            "Задайте базовую дату вручную и повторите запуск.",
            SOURCE,
            LAYER,
        )
        sys.exit(1)

    raw_updated = read_last_updated(service, PIPELINE_STATE_ID, SOURCE, RAW_LAYER)
    if raw_updated is None:
        logger.error(
            "В pipeline_state.csv нет записи о загрузке сырых инкрементов "
            "source=%s, layer=%s. Сначала запустите raw/load_cards_increment.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки cards: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон обработки cards: с %s по %s (%s дн.)",
        date_strs[0],
        date_strs[-1],
        len(date_strs),
    )

    # 1. Проверяем, что инкременты есть за все даты диапазона, до их чтения
    file_ids = find_increment_files(service, date_strs)

    # 2. Читаем и приводим к целевой схеме каждый инкремент, в хронологическом порядке
    increments = []
    for ds in date_strs:
        raw = pd.read_excel(download_bytes(service, file_ids[ds]), dtype=INCREMENT_DTYPES)
        incr = transform_increment(raw)
        increments.append(incr)
        logger.info("Инкремент cards за %s: строк после обработки %s", ds, len(incr))

    # 3. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, CARDS_TARGET_ID), dtype={"код": str, "телефон": str}
    )
    logger.info("Базовый файл Cards_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Cards_target.csv нет колонок: %s. "
            "Приведите базовый файл к целевой схеме и повторите запуск.",
            ", ".join(missing_columns),
        )
        sys.exit(1)

    target["дата_рождения"] = pd.to_datetime(target["дата_рождения"], errors="coerce")

    # 4. Присоединяем инкременты и снимаем дубликаты — свежая строка вытесняет старую
    combined = pd.concat([target, *increments], ignore_index=True)

    rows_before = len(combined)
    combined = combined.drop_duplicates(subset=KEY_COLUMNS, keep="last").reset_index(drop=True)
    logger.info(
        "Удалено дубликатов по (%s): %s",
        ", ".join(KEY_COLUMNS),
        rows_before - len(combined),
    )

    # 5. Пересчитываем возраст на дату запуска — он меняется и у старых карт
    combined["возраст"] = combined["дата_рождения"].apply(calculate_age, today=run_date)
    combined["категория_возраста"] = combined["возраст"].apply(age_group)
    combined["дата_рождения"] = combined["дата_рождения"].dt.strftime("%Y-%m-%d")

    # 6. Проставляем признак vip по справочнику — он живой, пересчитываем всем
    vip = read_sheet_as_dataframe(GSHEET_CREDENTIALS, VIP_SHEET_NAME, VIP_TAB_NAME)
    vip_phones = set(vip[VIP_PHONE_COLUMN].apply(phone_processor).dropna())
    logger.info("Справочник ВИПов: телефонов %s", len(vip_phones))

    combined["vip"] = np.where(combined["телефон"].isin(vip_phones), "да", "нет")

    combined = combined[TARGET_COLUMNS]

    # 7. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, CARDS_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Cards_target.csv перезаписан: строк всего %s (добавлено %s)",
        len(combined),
        len(combined) - len(target),
    )

    # 8. Двигаем watermark в pipeline_state до последней обработанной даты
    watermark = date_strs[-1]
    update_pipeline_state(service, PIPELINE_STATE_ID, SOURCE, LAYER, watermark)
    logger.info(
        "pipeline_state.csv обновлён: source=%s, layer=%s, updated_at=%s",
        SOURCE,
        LAYER,
        watermark,
    )
    logger.info("Готово: обновление cards за %s … %s выполнено", date_strs[0], date_strs[-1])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Cards_target.csv инкрементами из raw/cards на Google Drive"
    )
    parser.add_argument(
        "--date",
        dest="ds_run",
        default=None,
        help=(
            "Дата запуска в формате YYYY-MM-DD (по умолчанию сегодня). "
            "Обрабатываются даты до дня запуска включительно."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    update_cards(args.ds_run)
