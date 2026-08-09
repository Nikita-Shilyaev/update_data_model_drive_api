"""Обновление Rassrochka_target.csv инкрементами из raw/rassrochka на Google Drive.

Инкременты по рассрочкам — чистый append: каждый день на FTP выкладывается файл
с новыми платежами, они уже лежат в raw/rassrochka/<дата>/. Скрипт обрабатывает
даты из диапазона (processed-watermark + 1 день … min(raw-watermark, ds_run − 1
день)), приводит инкременты к схеме целевого файла и дописывает их к базовому
файлу.

Дубликаты снимаются по паре (`чек`, `терминал`) с приоритетом более свежей
строки. Ключа из одного `чек` недостаточно: покупку можно разделить между двумя
банками, и тогда на один чек приходится две строки с разными терминалами и
разными суммами — в истории таких чеков 26 на 16.5 млн ₸, и дедуп по одному
только чеку срезал бы половину каждого такого платежа. Пара (`чек`, `терминал`)
на всей истории уникальна.

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

SOURCE = "rassrochka"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

COLUMN_RENAMES = {
    "Чек.Оплата.Вид оплаты": "вид_оплаты",
    "Чек.Оплата.Эквайринговый терминал": "терминал",
    "Чек.Оплата.Сумма": "сумма",
    "Дата": "дата",
    "Чек": "чек",
    "Магазин": "магазин",
}

# один чек может быть разделён между банками — терминал входит в ключ
KEY_COLUMNS = ["чек", "терминал"]

TARGET_COLUMNS = [
    "вид_оплаты",
    "терминал",
    "сумма",
    "дата",
    "чек",
    "магазин",
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

RAW_RASSROCHKA_ID = config["PIPELINE_RAW"]["pipeline_raw_rassrochka_id"]
RASSROCHKA_TARGET_ID = config["PIPELINE_PROCESSED"]["rassrochka_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def find_increment_files(service, date_strs):
    """Ищет в raw/rassrochka эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_RASSROCHKA_ID)
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
            "В raw/rassrochka нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дней — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return file_ids


def transform_increment(raw):
    """Приводит инкремент rassrochka к схеме целевого файла.

    Выгрузка всегда заканчивается служебной строкой "Итого" в первом столбце —
    отбрасываем её до приведения типов. Если за дату не было рассрочек, кроме
    этой строки в файле ничего нет.
    """
    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    first_column = raw.columns[0]
    is_totals_row = raw[first_column].astype(str).str.strip().str.lower().eq("итого")
    raw = raw[~is_totals_row].copy()

    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    incr = raw.rename(columns=COLUMN_RENAMES)

    # "Количество записей" — служебное поле выгрузки, в модель не идёт
    incr = incr.drop(columns=["Количество записей"], errors="ignore")

    # без чека строка не идентифицируется и не дедуплицируется
    incr = incr.dropna(subset=["чек"]).copy()

    if incr.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    for column in ["вид_оплаты", "терминал", "чек", "магазин"]:
        incr[column] = incr[column].astype(str).str.strip()

    # dayfirst=True: эксель отдаёт ячейку и строкой "22.06.2026 17:08:41",
    # и уже готовым datetime — обе формы разбираются одинаково
    timestamp = pd.to_datetime(incr["дата"], dayfirst=True, errors="coerce")
    incr["дата"] = timestamp.dt.strftime("%Y-%m-%d %H:%M:%S")
    incr["date"] = timestamp.dt.strftime("%Y-%m-%d")

    # в источнике у большинства названий магазина хвостовой пробел
    incr["магазин"] = (
        incr["магазин"]
        .str.replace("Ювелирный салон ", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.strip()
    )

    # суммы держим дробными: в источнике есть копейки, округление построчно
    # уводит итог от контрольной суммы выгрузки
    incr["сумма"] = pd.to_numeric(incr["сумма"], errors="coerce").round(2)

    return incr[TARGET_COLUMNS]


def update_rassrochka(ds_run=None):
    """Дописывает инкременты rassrochka из raw/rassrochka в Rassrochka_target.csv.

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
            "source=%s, layer=%s. Сначала запустите raw/load_rassrochka_increment.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки rassrochka: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон обработки rassrochka: с %s по %s (%s дн.)",
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
            "Инкремент rassrochka за %s: строк после обработки %s, сумма %s",
            ds,
            len(incr),
            format(incr["сумма"].sum(), ",.2f") if len(incr) else "0.00",
        )

    # 3. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, RASSROCHKA_TARGET_ID),
        dtype={"чек": str, "терминал": str, "дата": str, "date": str},
    )
    logger.info("Базовый файл Rassrochka_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Rassrochka_target.csv нет колонок: %s. "
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

    combined = combined[TARGET_COLUMNS].sort_values("дата").reset_index(drop=True)

    # 5. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, RASSROCHKA_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Rassrochka_target.csv перезаписан: строк всего %s (добавлено %s), сумма %s",
        len(combined),
        len(combined) - len(target),
        format(combined["сумма"].sum(), ",.2f"),
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
        "Готово: обновление rassrochka за %s … %s выполнено", date_strs[0], date_strs[-1]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Rassrochka_target.csv инкрементами из raw/rassrochka на Google Drive"
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
    update_rassrochka(args.ds_run)
