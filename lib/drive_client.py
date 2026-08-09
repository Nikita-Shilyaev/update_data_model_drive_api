"""Клиент Google Drive API: авторизация и файловые операции."""

import io
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_MIMETYPE = "application/vnd.google-apps.folder"

logger = logging.getLogger(__name__)


def get_drive_service(creds_path, token_path):
    """Авторизуется в Drive API, при необходимости обновляя/пересоздавая token.json."""
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def find_or_create_subfolder(service, parent_id, name):
    """Возвращает id подпапки name внутри parent_id, создавая её при необходимости."""
    query = (
        f"'{parent_id}' in parents and name = '{name}' "
        f"and mimeType = '{FOLDER_MIMETYPE}' and trashed = false"
    )
    response = (
        service.files()
        .list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    folders = response.get("files", [])
    if folders:
        return folders[0]["id"]

    metadata = {"name": name, "mimeType": FOLDER_MIMETYPE, "parents": [parent_id]}
    folder = (
        service.files()
        .create(body=metadata, fields="id", supportsAllDrives=True)
        .execute()
    )
    logger.info("Создана папка '%s' на Google Drive (id=%s)", name, folder["id"])
    return folder["id"]


def list_children(service, parent_id):
    """Возвращает содержимое папки parent_id списком словарей id/name/mimeType."""
    children = []
    page_token = None
    while True:
        response = (
            service.files()
            .list(
                q=f"'{parent_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=1000,
                pageToken=page_token,
            )
            .execute()
        )
        children.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            return children


def upload_bytes(service, folder_id, filename, data, mimetype):
    """Идемпотентно загружает data в folder_id под именем filename.

    Если файл с таким именем уже есть в папке — обновляет его содержимое,
    иначе создаёт новый. Возвращает id файла на Drive.
    """
    query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    response = (
        service.files()
        .list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)

    existing = response.get("files", [])
    if existing:
        file_id = existing[0]["id"]
        service.files().update(
            fileId=file_id, media_body=media, supportsAllDrives=True
        ).execute()
        logger.info("Файл '%s' обновлён на Google Drive (id=%s)", filename, file_id)
        return file_id

    metadata = {"name": filename, "parents": [folder_id]}
    created = (
        service.files()
        .create(
            body=metadata, media_body=media, fields="id", supportsAllDrives=True
        )
        .execute()
    )
    logger.info("Файл '%s' загружен на Google Drive (id=%s)", filename, created["id"])
    return created["id"]


def download_bytes(service, file_id):
    """Скачивает файл по id в io.BytesIO."""
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer


def update_file_bytes(service, file_id, data, mimetype):
    """Перезаписывает содержимое существующего файла Drive (по id) байтами data."""
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
    service.files().update(
        fileId=file_id, media_body=media, supportsAllDrives=True
    ).execute()
    logger.info("Файл Drive (id=%s) перезаписан", file_id)
    return file_id
