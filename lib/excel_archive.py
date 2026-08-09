"""Распаковка zip-архивов, содержащих ровно один Excel-файл."""

import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_single_excel(zip_buffer):
    """Распаковывает архив в памяти и возвращает (имя_файла, байты) Excel-файла."""
    with zipfile.ZipFile(zip_buffer) as zf:
        members = [name for name in zf.namelist() if not name.endswith("/")]
        if not members:
            raise ValueError("Архив пуст — Excel-файл не найден")
        inner_name = members[0]
        if len(members) > 1:
            logger.warning(
                "В архиве ожидался один файл, найдено %s. Беру первый: %s",
                len(members),
                inner_name,
            )
        data = zf.read(inner_name)
    return Path(inner_name).name, data
