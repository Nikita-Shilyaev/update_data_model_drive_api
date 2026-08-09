"""Обновление Oplata_sert_target.csv инкрементами из raw/oplata_sert на Google Drive.

Инкременты по оплатам сертификатов — суточные выгрузки из 1С, уже лежащие в
raw/oplata_sert/<дата>/. Скрипт обрабатывает даты из диапазона
(processed-watermark + 1 день … min(raw-watermark, ds_run − 1 день)), приводит
инкременты к схеме целевого файла и вливает их в базовый файл.

Идемпотентность обеспечивается заменой по дате документа, а не dedup по
ключу: в этом источнике нет надёжного натурального ключа строки. Номер
документа не уникален глобально — нумерация в 1С сбрасывается каждый год
("00РТ-000520" на всей истории встречается и в январе 2025, и в январе 2026 —
это два разных документа в двух разных магазинах). А внутри одного документа
могут быть настоящие повторяющиеся строки: несколько сертификатов одного
номинала, оплаченные одним документом (например, "КАА00000060" — три строки,
из них две с одинаковым номиналом 50 000). Дедуп по любому ключу, включая
полную строку, стёр бы такие легитимные повторы. Поэтому из базового файла
удаляются все строки с датами, встретившимися в инкрементах, и на их место
кладётся содержимое инкрементов — как в update_sales_sku.py.

Каждая суточная выгрузка — полный срез за свой день, так что замена корректна,
а повторный прогон за ту же дату даёт тот же результат.

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

import pandas as pd

from lib.drive_client import (
    download_bytes,
    get_drive_service,
    list_children,
    update_file_bytes,
)
from lib.pipeline_state import read_last_updated, update_pipeline_state

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "oplata_sert"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

COLUMN_RENAMES = {
    "Магазин.Наименование": "магазин",
    "Документ.Дата": "дата",
    "Документ.Номер": "номер",
    "Сумма по номиналу, тг.": "сумма_по_номиналу",
    "Сумма документа, тг.": "сумма_документа",
    "Внереализационная прибыль, тг.": "внереализационная_прибыль",
}

TARGET_COLUMNS = [
    "оплата_сертификата",
    "магазин",
    "дата",
    "номер",
    "сумма_по_номиналу",
    "сумма_документа",
    "внереализационная_прибыль",
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

RAW_OPLATA_SERT_ID = config["PIPELINE_RAW"]["pipeline_raw_oplata_sert_id"]
OPLATA_SERT_TARGET_ID = config["PIPELINE_PROCESSED"]["oplata_sert_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def find_increment_files(service, date_strs):
    """Ищет в raw/oplata_sert эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_OPLATA_SERT_ID)
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
            "В raw/oplata_sert нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дней — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return file_ids


def transform_increment(raw):
    """Приводит инкремент oplata_sert к схеме целевого файла.

    Выгрузка всегда заканчивается служебной строкой "Итого" в первом столбце —
    отбрасываем её до приведения типов. Если за дату не было оплат
    сертификатов, кроме этой строки в файле ничего нет.
    """
    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    first_column = raw.columns[0]
    is_totals_row = raw[first_column].astype(str).str.strip().str.lower().eq("итого")
    raw = raw[~is_totals_row].copy()

    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    incr = raw.rename(columns=COLUMN_RENAMES)

    incr["магазин"] = incr["магазин"].astype(str).str.strip()
    incr["номер"] = incr["номер"].astype(str).str.strip()

    # dayfirst=True: эксель отдаёт ячейку и строкой "22.06.2026 16:50:44",
    # и уже готовым datetime — обе формы разбираются одинаково
    timestamp = pd.to_datetime(incr["дата"], dayfirst=True, errors="coerce")
    incr["дата"] = timestamp.dt.strftime("%Y-%m-%d %H:%M:%S")
    incr["date"] = timestamp.dt.strftime("%Y-%m-%d")

    incr["магазин"] = (
        incr["магазин"]
        .str.replace("Ювелирный салон ", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.strip()
    )

    incr["оплата_сертификата"] = (
        "Оплата сертификата " + incr["номер"] + " от " + incr["дата"]
    )

    return incr[TARGET_COLUMNS]


def update_oplata_sert(ds_run=None):
    """Вливает инкременты oplata_sert из raw/oplata_sert в Oplata_sert_target.csv.

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
            "source=%s, layer=%s. Сначала запустите raw/load_oplata_sert_increment.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки oplata_sert: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон обработки oplata_sert: с %s по %s (%s дн.)",
        date_strs[0],
        date_strs[-1],
        len(date_strs),
    )

    # 1. Проверяем, что инкременты есть за все даты диапазона, до их чтения
    file_ids = find_increment_files(service, date_strs)

    # 2. Читаем и приводим к целевой схеме каждый инкремент, в хронологическом порядке
    increments = []
    for ds in date_strs:
        raw = pd.read_excel(download_bytes(service, file_ids[ds]))
        incr = transform_increment(raw)
        increments.append(incr)
        logger.info(
            "Инкремент oplata_sert за %s: строк после обработки %s, сумма документов %s",
            ds,
            len(incr),
            format(incr["сумма_документа"].sum(), ",") if len(incr) else "0",
        )

    combined_increments = pd.concat(increments, ignore_index=True)
    if combined_increments.empty:
        logger.info("В инкрементах за диапазон нет ни одной строки оплат.")

    # 3. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, OPLATA_SERT_TARGET_ID),
        dtype={"номер": str, "date": str, "дата": str},
    )
    logger.info("Базовый файл Oplata_sert_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Oplata_sert_target.csv нет колонок: %s. "
            "Приведите базовый файл к целевой схеме и повторите запуск.",
            ", ".join(missing_columns),
        )
        sys.exit(1)

    # 4. Заменяем в базе дни, покрытые инкрементами: суточная выгрузка — полный
    #    срез за свой день, натурального ключа строки в этом источнике нет
    doc_dates = sorted(combined_increments["date"].dropna().unique())
    if doc_dates:
        logger.info(
            "Даты документов в инкрементах: %s … %s (%s дн.)",
            doc_dates[0],
            doc_dates[-1],
            len(doc_dates),
        )

    replaced = target["date"].isin(doc_dates)
    logger.info("Из базового файла вытеснено строк за эти даты: %s", int(replaced.sum()))

    combined = pd.concat(
        [target[~replaced], combined_increments], ignore_index=True
    ).sort_values("дата").reset_index(drop=True)

    combined = combined[TARGET_COLUMNS]

    # 5. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, OPLATA_SERT_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Oplata_sert_target.csv перезаписан: строк всего %s (было %s), сумма документов всего %s",
        len(combined),
        len(target),
        format(combined["сумма_документа"].sum(), ","),
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
        "Готово: обновление oplata_sert за %s … %s выполнено", date_strs[0], date_strs[-1]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Oplata_sert_target.csv инкрементами из raw/oplata_sert на Google Drive"
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
    update_oplata_sert(args.ds_run)
