"""Загрузка еженедельных инкрементов ostatki с FTP-сервера на Google Drive.

В отличие от остальных источников слоя raw:
- данные приходят на сервер за каждый день, но загружаются только даты,
  приходящиеся на понедельник, а также последний день каждого месяца
  (даже если он не понедельник) — для точного среза остатков на конец месяца.

На FTP архивы лежат в папке /userftpbi/sku (не /userFTPBI/pipeline/..., как
остальные источники) и названы по общему шаблону "<дата>_sku.zip"
(например "2026-07-20_sku.zip") — как и у остальных выгрузок, внутри всегда
ровно один Excel-файл.

Скрипт запускается не каждую неделю и догружает все пропущенные понедельники
и концы месяцев. Общая логика (диапазон дат, проверка полноты, распаковка,
Drive, pipeline_state) — в lib/weekly_increment.py.
"""

import argparse
import logging
import sys
from configparser import ConfigParser
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from lib.drive_client import get_drive_service
from lib.weekly_increment import load_weekly_increments

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "ostatki"
LAYER = "raw"
# на FTP архивы названы по "sku", а не по внутреннему имени источника "ostatki"
FILENAME_SOURCE = "sku"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

config = ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

FTP_HOST = config["FTP"]["host"]
FTP_USER = config["FTP"]["user"]
FTP_PASSWORD = config["FTP"]["password"]
FTP_DIR = config["FTP"]["ostatki_dir"]

CREDS_PATH = PROJECT_DIR / config["DRIVE_API"]["creds"]
TOKEN_PATH = PROJECT_DIR / config["DRIVE_API"]["token"]

DRIVE_FOLDER_ID = config["PIPELINE_RAW"]["pipeline_raw_ostatki_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Загрузка еженедельных инкрементов ostatki (понедельники и концы месяцев) "
            "с FTP на Google Drive"
        )
    )
    parser.add_argument(
        "--date",
        dest="ds_run",
        default=None,
        help=(
            "Дата запуска в формате YYYY-MM-DD (по умолчанию сегодня). "
            "Обрабатываются понедельники и концы месяцев до дня запуска включительно."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    service = get_drive_service(CREDS_PATH, TOKEN_PATH)
    load_weekly_increments(
        service=service,
        source=SOURCE,
        layer=LAYER,
        ftp_host=FTP_HOST,
        ftp_user=FTP_USER,
        ftp_password=FTP_PASSWORD,
        ftp_dir=FTP_DIR,
        filename_source=FILENAME_SOURCE,
        drive_folder_id=DRIVE_FOLDER_ID,
        pipeline_state_id=PIPELINE_STATE_ID,
        ds_run=args.ds_run,
    )
