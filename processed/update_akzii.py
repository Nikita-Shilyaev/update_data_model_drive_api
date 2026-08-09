"""Обновление Akzii_target.csv инкрементами из raw/akzii на Google Drive.

Инкремент akzii — не ленточка новых событий, а снимок всех действующих на
момент выгрузки маркетинговых акций по каждому магазину. Акция, которая длится
месяц, попадает в снимок каждый день, пока активна, поэтому пара (номер акции,
магазин) на одном дне уникальна, а между днями — повторяется массово.

Инкременты присоединяются к базе в хронологическом порядке и дедуплицируются
по (`номер_акции`, `магазин`) с `keep="last"`: свежий снимок вытесняет старый,
и обычный append+dedup даёт идемпотентность так же, как в sales_cheki и
vozvraty. Если акция перестаёт попадать в снимок (закончилась), её последняя
известная строка остаётся в целевом файле как исторический след — это не
таблица "что действует прямо сейчас", а журнал последних известных состояний.
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

SOURCE = "akzii"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

COLUMN_RENAMES = {
    "Маркетинговая акция.Номер": "номер_акции",
    "Маркетинговая акция.Наименование акции": "название_акции",
    "Маркетинговая акция.Дата": "дата_создания",
    "Скидка (наценка, ограничение)": "скидка",
    "Для всех магазинов": "для_всех_магазинов",
    "Магазин": "магазин",
    "Дата начала акции": "дата_начала",
    "Дата окончания акции": "дата_окончания",
}

KEY_COLUMNS = ["номер_акции", "магазин"]

TARGET_COLUMNS = [
    "номер_акции",
    "название_акции",
    "дата_создания",
    "скидка",
    "для_всех_магазинов",
    "магазин",
    "дата_начала",
    "дата_окончания",
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

RAW_AKZII_ID = config["PIPELINE_RAW"]["pipeline_raw_akzii_id"]
AKZII_TARGET_ID = config["PIPELINE_PROCESSED"]["akzii_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def find_increment_files(service, date_strs):
    """Ищет в raw/akzii эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_AKZII_ID)
        if item["mimeType"] == FOLDER_MIMETYPE
    }

    files = {}
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

        files[ds] = excels[0]["id"]

    if missing:
        logger.error(
            "В raw/akzii нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дней — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return files


def transform_increment(raw):
    """Приводит инкремент akzii к схеме целевого файла."""
    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    incr = raw.rename(columns=COLUMN_RENAMES).copy()

    for column in ["номер_акции", "название_акции", "скидка", "для_всех_магазинов", "магазин"]:
        incr[column] = incr[column].astype(str).str.strip()

    # dayfirst=True: эксель отдаёт ячейку и строкой "01.02.2026 12:11:28",
    # и уже готовым datetime — обе формы разбираются одинаково
    incr["дата_создания"] = pd.to_datetime(
        incr["дата_создания"], dayfirst=True, errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    for column in ["дата_начала", "дата_окончания"]:
        incr[column] = pd.to_datetime(
            incr[column], dayfirst=True, errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    incr["магазин"] = (
        incr["магазин"]
        .str.replace("Ювелирный салон ", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.strip()
    )

    return incr[TARGET_COLUMNS]


def update_akzii(ds_run=None):
    """Дописывает инкременты akzii из raw/akzii в Akzii_target.csv.

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
            "source=%s, layer=%s. Сначала запустите raw/load_akzii_increment.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки akzii: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон обработки akzii: с %s по %s (%s дн.)",
        date_strs[0],
        date_strs[-1],
        len(date_strs),
    )

    # 1. Проверяем, что инкременты есть за все даты диапазона, до их чтения
    files = find_increment_files(service, date_strs)

    # 2. Читаем и приводим к целевой схеме каждый инкремент, в хронологическом порядке
    increments = []
    for ds in date_strs:
        raw = pd.read_excel(download_bytes(service, files[ds]))
        incr = transform_increment(raw)
        increments.append(incr)
        logger.info("Инкремент akzii за %s: строк после обработки %s", ds, len(incr))

    # 3. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, AKZII_TARGET_ID),
        dtype={"номер_акции": str, "магазин": str},
    )
    logger.info("Базовый файл Akzii_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Akzii_target.csv нет колонок: %s. "
            "Приведите базовый файл к целевой схеме и повторите запуск.",
            ", ".join(missing_columns),
        )
        sys.exit(1)

    # 4. Присоединяем инкременты и снимаем дубликаты — свежий снимок вытесняет старый
    combined = pd.concat([target, *increments], ignore_index=True)

    rows_before = len(combined)
    combined = combined.drop_duplicates(subset=KEY_COLUMNS, keep="last").reset_index(drop=True)
    logger.info(
        "Удалено дубликатов по (%s): %s",
        ", ".join(KEY_COLUMNS),
        rows_before - len(combined),
    )

    combined = combined[TARGET_COLUMNS].sort_values(
        ["номер_акции", "магазин"]
    ).reset_index(drop=True)

    # 5. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, AKZII_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Akzii_target.csv перезаписан: строк всего %s (было %s)",
        len(combined),
        len(target),
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
        "Готово: обновление akzii за %s … %s выполнено", date_strs[0], date_strs[-1]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Akzii_target.csv инкрементами из raw/akzii на Google Drive"
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
    update_akzii(args.ds_run)
