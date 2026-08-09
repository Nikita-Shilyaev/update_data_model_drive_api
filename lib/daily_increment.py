"""Загрузка ежедневных инкрементов с FTP на Google Drive за диапазон дат.

Общая оркестрация для источников, которые на FTP каждый день выкладывают
один zip-архив `<дата>_<source>.zip`, и для которых пайплайн не допускает
пропущенных дней.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

from lib.drive_client import find_or_create_subfolder, upload_bytes
from lib.excel_archive import extract_single_excel
from lib.ftp_client import download_archive, list_archives, open_ftp
from lib.pipeline_state import read_last_updated, update_pipeline_state

logger = logging.getLogger(__name__)

EXCEL_MIMETYPES = {
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
}


def load_daily_increments(
    *,
    service,
    source,
    layer,
    ftp_host,
    ftp_user,
    ftp_password,
    ftp_dir,
    drive_folder_id,
    pipeline_state_id,
    ds_run=None,
):
    """Загружает инкременты source за диапазон дат в raw-папку drive_folder_id.

    Диапазон: от (последняя дата в pipeline_state для source/layer + 1 день)
    до дня запуска (ds_run) включительно. Дата запуска ds_run —
    строка YYYY-MM-DD; по умолчанию сегодня.

    Если хотя бы за одну дату диапазона архив `<дата>_<source>.zip` отсутствует
    на FTP — не загружает ничего и останавливается с перечнем недостающих дат.

    Даты обрабатываются по одной: скачали архив в память, залили на Drive,
    зафиксировали дату в pipeline_state, перешли к следующей — без накопления
    всех архивов диапазона в памяти одновременно.
    """
    run_date = (
        pd.to_datetime(ds_run).normalize() if ds_run else pd.Timestamp.today().normalize()
    )
    end_date = run_date

    last_updated = read_last_updated(service, pipeline_state_id, source, layer)
    if last_updated is None:
        logger.error(
            "В pipeline_state.csv нет записи о последнем обновлении source=%s, layer=%s. "
            "Задайте базовую дату вручную и повторите запуск.",
            source,
            layer,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для загрузки %s: последнее обновление %s, обрабатываем до %s "
            "(дата запуска %s). Остановка.",
            source,
            last_updated.date(),
            end_date.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон загрузки %s: с %s по %s (%s дн.)",
        source,
        date_strs[0],
        date_strs[-1],
        len(date_strs),
    )
    expected = {ds: f"{ds}_{source}.zip" for ds in date_strs}

    ftp = open_ftp(ftp_host, ftp_user, ftp_password, ftp_dir)
    try:
        # --- Проверяем наличие ВСЕХ архивов диапазона, не скачивая их ---
        available = list_archives(ftp)
        missing = [ds for ds, name in expected.items() if name not in available]
        if missing:
            logger.error(
                "На FTP отсутствуют архивы %s за даты: %s. "
                "Пайплайн не допускает пропущенных дней — остановка без загрузки.",
                source,
                ", ".join(missing),
            )
            sys.exit(1)

        # --- По одной дате: скачали в память -> залили на Drive -> обновили state ---
        for ds in date_strs:
            buffer = download_archive(ftp, expected[ds])
            excel_name, excel_data = extract_single_excel(buffer)
            logger.info("Скачан и распакован архив %s", expected[ds])

            mimetype = EXCEL_MIMETYPES.get(
                Path(excel_name).suffix.lower(), "application/octet-stream"
            )
            date_folder_id = find_or_create_subfolder(service, drive_folder_id, ds)
            upload_bytes(service, date_folder_id, excel_name, excel_data, mimetype)
            logger.info("Инкремент %s за %s загружен на Google Drive", source, ds)

            update_pipeline_state(service, pipeline_state_id, source, layer, ds)
            logger.info(
                "pipeline_state.csv обновлён: source=%s, layer=%s, updated_at=%s",
                source,
                layer,
                ds,
            )
    finally:
        ftp.quit()

    logger.info(
        "Готово: загружено инкрементов %s — %s (%s … %s)",
        source,
        len(date_strs),
        date_strs[0],
        date_strs[-1],
    )
