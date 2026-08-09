"""Загрузка инкрементов cards с FTP-сервера на Google Drive за диапазон дат.

Скрипт запускается не каждый день и догружает все пропущенные даты.
Общая логика (FTP, Drive, pipeline_state, диапазон дат) — в lib/daily_increment.py.
"""

import argparse
import logging
import sys
from configparser import ConfigParser
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from lib.daily_increment import load_daily_increments
from lib.drive_client import get_drive_service

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "cards"
LAYER = "raw"

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
FTP_DIR = config["FTP"]["cards_dir"]

CREDS_PATH = PROJECT_DIR / config["DRIVE_API"]["creds"]
TOKEN_PATH = PROJECT_DIR / config["DRIVE_API"]["token"]

DRIVE_FOLDER_ID = config["PIPELINE_RAW"]["pipeline_raw_cards_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Загрузка инкрементов cards с FTP на Google Drive за диапазон дат"
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
    service = get_drive_service(CREDS_PATH, TOKEN_PATH)
    load_daily_increments(
        service=service,
        source=SOURCE,
        layer=LAYER,
        ftp_host=FTP_HOST,
        ftp_user=FTP_USER,
        ftp_password=FTP_PASSWORD,
        ftp_dir=FTP_DIR,
        drive_folder_id=DRIVE_FOLDER_ID,
        pipeline_state_id=PIPELINE_STATE_ID,
        ds_run=args.ds_run,
    )
