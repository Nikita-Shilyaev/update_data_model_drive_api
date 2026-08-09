"""Обновление visits_target.csv из Google Sheet Visits_2026 (лист посещения-2026).

Источник — живая гугл-таблица: строки могут дописываться задним числом и
править задним числом, поэтому вместо чистого append используется скользящее
окно [updated_at - 21 день, ds_run]. Окно из листа замещает
соответствующие строки в целевом файле (удаление окна + concat), файл
перезаписывается целиком, watermark в pipeline_state двигается до конца окна.
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

from lib.drive_client import download_bytes, get_drive_service, update_file_bytes
from lib.gsheet_client import read_sheet_as_dataframe
from lib.pipeline_state import read_last_updated, update_pipeline_state

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "visits"
LAYER = "processed"
WINDOW_DAYS = 21

SHEET_NAME = "Visits_2026"
TAB_NAME = "посещения-2026"

# соответствие колонок листа колонкам целевого файла
SHEET_RENAME = {"Дата": "date", "Кол-во": "посещений", "Магазин": "магазин"}
TARGET_COLUMNS = ["date", "Месяц", "Час", "посещений", "магазин"]
SORT_COLUMNS = ["date", "магазин", "Час"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

config = ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

GSHEET_CREDENTIALS = PROJECT_DIR / config["GSHEET"]["credentials_json"]
CREDS_PATH = PROJECT_DIR / config["DRIVE_API"]["creds"]
TOKEN_PATH = PROJECT_DIR / config["DRIVE_API"]["token"]

VISITS_TARGET_ID = config["PIPELINE_PROCESSED"]["visits_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def update_visits(ds_run=None):
    """Обновляет visits_target.csv окном из листа Visits_2026.

    ds_run — дата запуска (строка YYYY-MM-DD); по умолчанию сегодня.
    Окно обновления: [updated_at - WINDOW_DAYS, ds_run].
    """
    run_date = (
        pd.to_datetime(ds_run).normalize() if ds_run else pd.Timestamp.today().normalize()
    )
    window_end = run_date

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

    window_start = last_updated - pd.Timedelta(days=WINDOW_DAYS)

    if window_start > window_end:
        logger.info(
            "Пустое окно обновления visits: %s > %s (последнее обновление %s, дата запуска %s). Остановка.",
            window_start.date(),
            window_end.date(),
            last_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    logger.info(
        "Окно обновления visits: %s … %s (последнее обновление %s)",
        window_start.date(),
        window_end.date(),
        last_updated.date(),
    )

    # 1. Читаем лист Visits_2026 и приводим к схеме целевого файла
    sheet = read_sheet_as_dataframe(GSHEET_CREDENTIALS, SHEET_NAME, TAB_NAME)
    sheet = sheet.rename(columns=SHEET_RENAME)
    sheet["date"] = pd.to_datetime(sheet["date"], format="%d.%m.%Y")

    incr = sheet.loc[sheet["date"].between(window_start, window_end), TARGET_COLUMNS].copy()
    logger.info("Из листа отобрано строк в окне: %s", len(incr))

    # 2. Читаем целевой файл и удаляем из него строки, попадающие в окно
    target = pd.read_csv(download_bytes(service, VISITS_TARGET_ID))
    target["date"] = pd.to_datetime(target["date"])

    rows_before = len(target)
    target = target.loc[~target["date"].between(window_start, window_end)]
    logger.info("Из целевого файла удалено строк в окне: %s", rows_before - len(target))

    # 3. Присоединяем окно из листа
    combined = pd.concat([target, incr], ignore_index=True)

    # 4. Сортируем и приводим дату обратно к строковому виду YYYY-MM-DD
    combined = combined.sort_values(SORT_COLUMNS).reset_index(drop=True)
    combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")
    combined = combined[TARGET_COLUMNS]

    # 5. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, VISITS_TARGET_ID, out.getvalue(), "text/csv")
    logger.info("visits_target.csv перезаписан: строк всего %s", len(combined))

    # 6. Двигаем watermark в pipeline_state до конца окна
    watermark = window_end.strftime("%Y-%m-%d")
    update_pipeline_state(service, PIPELINE_STATE_ID, SOURCE, LAYER, watermark)
    logger.info(
        "pipeline_state.csv обновлён: source=%s, layer=%s, updated_at=%s",
        SOURCE,
        LAYER,
        watermark,
    )
    logger.info("Готово: обновление visits за окно %s … %s выполнено", window_start.date(), window_end.date())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление visits_target.csv из Google Sheet Visits_2026 скользящим окном"
    )
    parser.add_argument(
        "--date",
        dest="ds_run",
        default=None,
        help=(
            "Дата запуска в формате YYYY-MM-DD (по умолчанию сегодня). "
            "Окно обновления: [updated_at − 21 день, дата запуска]."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    update_visits(args.ds_run)
