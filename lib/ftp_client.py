"""FTP-клиент для проверки и скачивания архивов инкрементов."""

import io
from ftplib import FTP, error_perm
from pathlib import Path


def open_ftp(host, user, password, remote_dir, encoding="utf-8"):
    """Открывает соединение с FTP и переходит в указанный каталог.

    encoding — кодировка имён файлов на сервере (некоторые каталоги отдают
    листинг с кириллическими именами в cp1251, а не в utf-8).
    """
    ftp = FTP(host, encoding=encoding)
    ftp.login(user, password)
    ftp.cwd(remote_dir)
    return ftp


def list_archives(ftp):
    """Возвращает множество имён файлов в текущем каталоге FTP.

    Если каталог пуст и сервер отвечает ошибкой на листинг — возвращает пустое множество.
    """
    try:
        return {Path(name).name for name in ftp.nlst()}
    except error_perm:
        return set()


def download_archive(ftp, filename):
    """Скачивает файл filename из текущего каталога FTP в память."""
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    return buffer
