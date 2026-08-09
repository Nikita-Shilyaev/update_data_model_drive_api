"""Загрузка еженедельных инкрементов с FTP на Google Drive.

Отличие от lib/daily_increment.py: из диапазона дат обрабатываются только
даты, приходящиеся на понедельник, а также последний день месяца (даже если
это не понедельник) — остальные дни диапазона игнорируются.
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


def target_dates(start_date, end_date):
    """Даты диапазона [start_date, end_date], приходящиеся на понедельник или
    являющиеся последним днём месяца (без дублей, по возрастанию)."""
    return [
        d
        for d in pd.date_range(start_date, end_date, freq="D")
        if d.weekday() == 0 or d.is_month_end
    ]


def load_weekly_increments(
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
    filename_source=None,
    ds_run=None,
):
    """Загружает инкременты source за все понедельники и концы месяцев диапазона дат.

    Диапазон: от (последняя дата в pipeline_state для source/layer + 1 день)
    до дня запуска (ds_run) включительно. Внутри диапазона
    обрабатываются только даты, приходящиеся на понедельник или являющиеся
    последним днём месяца; остальные дни диапазона игнорируются.

    На FTP за каждую дату лежит архив `<дата>_<filename_source>.zip` с одним
    Excel-файлом внутри. filename_source нужен, когда имя источника на FTP
    не совпадает с внутренним именем source (по умолчанию берётся source).

    Если хотя бы за одну дату диапазона архив отсутствует на FTP —
    не загружает ничего и останавливается с перечнем недостающих дат.
    Даты обрабатываются по одной: скачали архив в память, распаковали,
    залили на Drive, зафиксировали дату в pipeline_state, перешли к следующей.
    """
    filename_source = filename_source or source
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

    dates = target_dates(start_date, end_date)

    if not dates:
        logger.info(
            "В диапазоне %s … %s нет дат понедельника или конца месяца для %s. Остановка.",
            start_date.date(),
            end_date.date(),
            source,
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    expected = {ds: f"{ds}_{filename_source}.zip" for ds in date_strs}
    logger.info(
        "Диапазон загрузки %s (понедельники и концы месяцев): %s (%s дн.)",
        source,
        ", ".join(date_strs),
        len(date_strs),
    )

    ftp = open_ftp(ftp_host, ftp_user, ftp_password, ftp_dir)
    try:
        # --- Проверяем наличие архивов ЗА ВСЕ даты диапазона, не скачивая их ---
        available = list_archives(ftp)
        missing = [ds for ds, name in expected.items() if name not in available]
        if missing:
            logger.error(
                "На FTP отсутствуют архивы %s за даты: %s. "
                "Пайплайн не допускает пропущенных дат — остановка без загрузки.",
                source,
                ", ".join(missing),
            )
            sys.exit(1)

        # --- По одной дате: скачали в память -> распаковали -> залили на Drive -> обновили state ---
        for ds in date_strs:
            filename = expected[ds]
            buffer = download_archive(ftp, filename)
            excel_name, excel_data = extract_single_excel(buffer)
            logger.info("Скачан и распакован архив %s", filename)

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
        "Готово: загружено инкрементов %s — %s (%s)",
        source,
        len(date_strs),
        ", ".join(date_strs),
    )
