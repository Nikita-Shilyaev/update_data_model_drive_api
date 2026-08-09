"""Обновление Sales_cheki_target.csv инкрементами из raw/sales_cheki на Google Drive.

Инкременты по продажам — чистый append: каждый день на FTP выкладывается файл
с новыми чеками, они уже лежат в raw/sales_cheki/<дата>/. Скрипт обрабатывает
даты из диапазона (processed-watermark + 1 день … min(raw-watermark, ds_run − 1
день)), приводит инкременты к схеме целевого файла и дописывает их к базовому
файлу.

Дубликаты снимаются по паре (`штрихкод`, `чек`) с приоритетом более свежей
строки: инкременты присоединяются в хронологическом порядке после базы, поэтому
`keep="last"` оставляет последнюю известную версию строки. Это же даёт
идемпотентность — повторная обработка той же даты не удвоит строки.

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
from lib.pipeline_state import read_last_updated, update_pipeline_state

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "sales_cheki"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

# читаем "Чек.Номер" строкой, иначе пустые значения нельзя однозначно отличить
# от валидных числовых номеров при проверке на пропуски
INCREMENT_DTYPES = {"Чек.Номер": str}

KEY_COLUMNS = ["штрихкод", "чек"]
TARGET_COLUMNS = [
    "штрихкод",
    "чек",
    "дата",
    "магазин",
    "продавец",
    "код",
    "телефон",
    "количество",
    "сумма_скидки_оплаты_бонусом",
    "сумма_со_скидкой",
    "сумма_без_скидки",
    "date",
]

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

RAW_SALES_CHEKI_ID = config["PIPELINE_RAW"]["pipeline_raw_sales_cheki_id"]
SALES_CHEKI_TARGET_ID = config["PIPELINE_PROCESSED"]["sales_cheki_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def phone_processor(phone):
    """Приводит номер телефона к 10 цифрам; NaN, если номер короче."""
    if pd.isna(phone):
        return np.nan

    # гугл-таблицы/эксель отдают номер как float; без int() в строке остаётся
    # '.0' и последние 10 цифр съезжают
    raw = str(int(phone)) if isinstance(phone, float) else str(phone)

    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 10:
        return digits[-10:]

    return np.nan


def find_increment_files(service, date_strs):
    """Ищет в raw/sales_cheki эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_SALES_CHEKI_ID)
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
            "В raw/sales_cheki нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дней — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return file_ids


def transform_increment(raw):
    """Приводит инкремент sales_cheki к схеме целевого файла.

    Если за дату не было продаж, выгрузка содержит только заголовки и одну
    строку со словом "Итого" в первом столбце — такую строку отбрасываем
    явно, до попытки привести числовые колонки к нужным типам.
    """
    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    first_column = raw.columns[0]
    is_totals_row = raw[first_column].astype(str).str.strip().str.lower().eq("итого")
    raw = raw[~is_totals_row].copy()

    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    # "Чек.Номер" — обязательное поле, пустые строки удаляем
    incr = raw.dropna(subset=["Чек.Номер"]).copy()
    incr["Чек.Номер"] = incr["Чек.Номер"].str.strip()
    incr = incr[incr["Чек.Номер"] != ""].copy()

    # Штрихкод тоже обязателен: без него строка не соединяется ни с одной
    # другой таблицей модели и не участвует в дедупе (он входит в ключ) —
    # это брак заполнения в 1С, а не значимые данные
    incr = incr.dropna(subset=["Номенклатура.БИТ Основной штрихкод"]).copy()

    # dayfirst=True: эксель может отдать ячейку и строкой "22.06.2026 16:40:46",
    # и уже готовым datetime — обе формы разбираются одинаково
    incr["Чек.Дата"] = pd.to_datetime(incr["Чек.Дата"], dayfirst=True, errors="coerce")

    # поле "чек" собираем так же, как оно выглядит в целевой таблице
    incr["чек"] = (
        "Чек "
        + incr["Чек.Номер"]
        + " от "
        + incr["Чек.Дата"].dt.strftime("%d.%m.%Y %H:%M:%S")
    )
    incr["дата"] = incr["Чек.Дата"].dt.strftime("%Y-%m-%d %H:%M:%S")
    incr["date"] = incr["Чек.Дата"].dt.strftime("%Y-%m-%d")

    incr["магазин"] = (
        incr["Чек.Магазин.Наименование"]
        .str.strip()
        .str.replace("Ювелирный салон ", "", regex=False)
        .str.replace('"', "", regex=False)
    )

    incr["штрихкод"] = incr["Номенклатура.БИТ Основной штрихкод"].astype("Int64").astype(str)

    incr["телефон"] = incr["Чек.Дисконтная карта.Код карты"].apply(phone_processor)

    incr = incr.rename(
        columns={
            "Продавец.ФИО": "продавец",
            "Чек.Дисконтная карта.Код": "код",
            "Количество": "количество",
            "Сумма скидки оплаты бонусом": "сумма_скидки_оплаты_бонусом",
            "Сумма": "сумма_со_скидкой",
            "Цена": "сумма_без_скидки",
        }
    )

    return incr[TARGET_COLUMNS]


def update_sales_cheki(ds_run=None):
    """Дописывает инкременты sales_cheki из raw/sales_cheki в Sales_cheki_target.csv.

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
            "source=%s, layer=%s. Сначала запустите raw/load_sales_cheki_increment.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки sales_cheki: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон обработки sales_cheki: с %s по %s (%s дн.)",
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
        logger.info("Инкремент sales_cheki за %s: строк после обработки %s", ds, len(incr))

    # 3. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, SALES_CHEKI_TARGET_ID),
        dtype={"штрихкод": str, "чек": str, "код": str, "телефон": str},
    )
    logger.info("Базовый файл Sales_cheki_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Sales_cheki_target.csv нет колонок: %s. "
            "Приведите базовый файл к целевой схеме и повторите запуск.",
            ", ".join(missing_columns),
        )
        sys.exit(1)

    # 4. Присоединяем инкременты и снимаем дубликаты — свежая строка вытесняет старую
    combined = pd.concat([target, *increments], ignore_index=True)

    rows_before = len(combined)
    combined = combined.drop_duplicates(subset=KEY_COLUMNS, keep="last").reset_index(drop=True)
    logger.info(
        "Удалено дубликатов по (%s): %s",
        ", ".join(KEY_COLUMNS),
        rows_before - len(combined),
    )

    combined = combined[TARGET_COLUMNS]

    # 5. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, SALES_CHEKI_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Sales_cheki_target.csv перезаписан: строк всего %s (добавлено %s)",
        len(combined),
        len(combined) - len(target),
    )

    # 6. Двигаем watermark в pipeline_state до последней обработанной даты
    watermark = date_strs[-1]
    update_pipeline_state(service, PIPELINE_STATE_ID, SOURCE, LAYER, watermark)
    logger.info(
        "pipeline_state.csv обновлён: source=%s, layer=%s, updated_at=%s",
        SOURCE,
        LAYER,
        watermark,
    )
    logger.info(
        "Готово: обновление sales_cheki за %s … %s выполнено", date_strs[0], date_strs[-1]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Sales_cheki_target.csv инкрементами из raw/sales_cheki на Google Drive"
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
    update_sales_cheki(args.ds_run)
